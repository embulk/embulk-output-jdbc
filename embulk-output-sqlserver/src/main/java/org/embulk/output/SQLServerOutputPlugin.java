package org.embulk.output;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.StandardBatchInsert;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.embulk.output.sqlserver.InsertMethod;
import org.embulk.output.sqlserver.NativeBatchInsert;
import org.embulk.output.sqlserver.SQLServerOutputConnector;
import org.embulk.output.sqlserver.setter.SQLServerColumnSetterFactory;
import org.joda.time.DateTimeZone;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class SQLServerOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    public interface SQLServerPluginTask
            extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        @ConfigDefault("null")
        public Optional<String> getHost();

        @Config("port")
        @ConfigDefault("1433")
        public int getPort();

        @Config("instance")
        @ConfigDefault("null")
        public Optional<String> getInstance();

        @Config("database")
        @ConfigDefault("null")
        public Optional<String> getDatabase();

        @Config("integratedSecurity")
        @ConfigDefault("null")
        public Optional<Boolean> getIntegratedSecurity();

        @Config("url")
        @ConfigDefault("null")
        public Optional<String> getUrl();

        @Config("user")
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public Optional<String> getPassword();

        @Config("insert_method")
        @ConfigDefault("\"normal\"")
        public InsertMethod getInsertMethod();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return SQLServerPluginTask.class;
    }

    @Override
    protected Features getFeatures(PluginTask task)
    {
        return new Features()
            .setMaxTableNameLength(128)
            .setSupportedModes(ImmutableSet.of(Mode.INSERT, Mode.INSERT_DIRECT, Mode.TRUNCATE_INSERT, Mode.REPLACE))
            .setIgnoreMergeKeys(false);
    }

    @Override
    protected SQLServerOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        SQLServerPluginTask sqlServerTask = (SQLServerPluginTask) task;

        if (sqlServerTask.getDriverPath().isPresent()) {
            loadDriverJar(sqlServerTask.getDriverPath().get());
        }

        String url;
        if (sqlServerTask.getUrl().isPresent()) {
            if (sqlServerTask.getInsertMethod() == InsertMethod.NATIVE) {
                throw new IllegalArgumentException("Cannot set 'url' when 'insert_method' is 'native'.");
            }

            if (sqlServerTask.getHost().isPresent()
                    || sqlServerTask.getInstance().isPresent()
                    || sqlServerTask.getDatabase().isPresent()
                    || sqlServerTask.getIntegratedSecurity().isPresent()) {
                throw new IllegalArgumentException("'host', 'port', 'instance', 'database' and 'integratedSecurity' parameters are invalid if 'url' parameter is set.");
            }
            url = sqlServerTask.getUrl().get();
        } else {
            if (!sqlServerTask.getHost().isPresent()) {
                throw new IllegalArgumentException("Field 'host' is not set.");
            }
            if (!sqlServerTask.getDatabase().isPresent()) {
                throw new IllegalArgumentException("Field 'database' is not set.");
            }
            StringBuilder urlBuilder = new StringBuilder();
            if (sqlServerTask.getInstance().isPresent()) {
                urlBuilder.append(String.format("jdbc:sqlserver://%s\\%s",
                        sqlServerTask.getHost().get(), sqlServerTask.getInstance().get()));
            } else {
                urlBuilder.append(String.format("jdbc:sqlserver://%s:%d",
                        sqlServerTask.getHost().get(), sqlServerTask.getPort()));
            }
            if (sqlServerTask.getDatabase().isPresent()) {
                urlBuilder.append(";databaseName=" + sqlServerTask.getDatabase().get());
            }
            if (sqlServerTask.getIntegratedSecurity().isPresent() && sqlServerTask.getIntegratedSecurity().get()) {
                urlBuilder.append(";integratedSecurity=" + sqlServerTask.getIntegratedSecurity().get());
            } else {
                if (!sqlServerTask.getUser().isPresent()) {
                    throw new IllegalArgumentException("Field 'user' is not set.");
                }
                if (!sqlServerTask.getPassword().isPresent()) {
                    throw new IllegalArgumentException("Field 'password' is not set.");
                }
            }
            url = urlBuilder.toString();
        }


        Properties props = new Properties();
        props.putAll(sqlServerTask.getOptions());

        if (sqlServerTask.getUser().isPresent()) {
            props.setProperty("user", sqlServerTask.getUser().get());
        }
        logger.info("Connecting to {} options {}", url, props);
        if (sqlServerTask.getPassword().isPresent()) {
            props.setProperty("password", sqlServerTask.getPassword().get());
        }

        return new SQLServerOutputConnector(url, props, null);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<List<String>> mergeKeys) throws IOException, SQLException
    {
        SQLServerPluginTask sqlServerTask = (SQLServerPluginTask) task;
        if (sqlServerTask.getInsertMethod() == InsertMethod.NATIVE) {
            return new NativeBatchInsert(sqlServerTask.getHost().get(), sqlServerTask.getPort(), sqlServerTask.getInstance(),
                    sqlServerTask.getDatabase().get(), sqlServerTask.getUser(), sqlServerTask.getPassword());
        }
        return new StandardBatchInsert(getConnector(task, true), mergeKeys, task.getOnDuplicateKeyUpdate());
    }

    @Override
    protected ColumnSetterFactory newColumnSetterFactory(BatchInsert batch, DateTimeZone defaultTimeZone)
    {
        return new SQLServerColumnSetterFactory(batch, defaultTimeZone);
    }
}
