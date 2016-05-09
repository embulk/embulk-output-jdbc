package org.embulk.output;

import java.util.List;
import java.util.Properties;
import java.io.IOException;
import java.sql.SQLException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.StandardBatchInsert;
import org.embulk.output.oracle.DirectBatchInsert;
import org.embulk.output.oracle.InsertMethod;
import org.embulk.output.oracle.OracleCharset;
import org.embulk.output.oracle.OracleOutputConnection;
import org.embulk.output.oracle.OracleOutputConnector;

public class OracleOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    public interface OraclePluginTask
            extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        @ConfigDefault("null")
        public Optional<String> getHost();

        @Config("port")
        @ConfigDefault("1521")
        public int getPort();

        @Config("database")
        @ConfigDefault("null")
        public Optional<String> getDatabase();

        @Config("schema")
        @ConfigDefault("null")
        public Optional<String> getSchema();

        @Config("url")
        @ConfigDefault("null")
        public Optional<String> getUrl();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

        @Config("insert_method")
        @ConfigDefault("\"normal\"")
        public InsertMethod getInsertMethod();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return OraclePluginTask.class;
    }

    @Override
    protected Features getFeatures(PluginTask task)
    {
        return new Features()
            .setMaxTableNameLength(30)
            .setSupportedModes(ImmutableSet.of(Mode.INSERT, Mode.INSERT_DIRECT, Mode.TRUNCATE_INSERT, Mode.REPLACE))
            .setIgnoreMergeKeys(false);
    }

    @Override
    protected OracleOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        OraclePluginTask oracleTask = (OraclePluginTask) task;

        if (oracleTask.getDriverPath().isPresent()) {
            loadDriverJar(oracleTask.getDriverPath().get());
        }

        String url;
        if (oracleTask.getInsertMethod() == InsertMethod.oci) {
            if (!oracleTask.getHost().isPresent()) {
                throw new IllegalArgumentException("Field 'host' is not set.");
            }
            if (!oracleTask.getDatabase().isPresent()) {
                throw new IllegalArgumentException("Field 'database' is not set.");
            }
        } else {
            if (oracleTask.getUrl().isPresent()) {
                if (oracleTask.getHost().isPresent() || oracleTask.getDatabase().isPresent()) {
                    throw new IllegalArgumentException("'host', 'port' and 'database' parameters are invalid if 'url' parameter is set.");
                }
            } else {
                if (!oracleTask.getHost().isPresent()) {
                    throw new IllegalArgumentException("Field 'host' is not set.");
                }
                if (!oracleTask.getDatabase().isPresent()) {
                    throw new IllegalArgumentException("Field 'database' is not set.");
                }
            }
        }

        if (oracleTask.getUrl().isPresent()) {
            url = oracleTask.getUrl().get();
        } else {
            url = String.format("jdbc:oracle:thin:@%s:%d:%s",
                    oracleTask.getHost().get(), oracleTask.getPort(), oracleTask.getDatabase().get());
        }

        Properties props = new Properties();
        props.putAll(oracleTask.getOptions());

        props.setProperty("user", oracleTask.getUser());
        logger.info("Connecting to {} options {}", url, props);
        props.setProperty("password", oracleTask.getPassword());

        return new OracleOutputConnector(url, props, oracleTask.getSchema().orNull(), oracleTask.getInsertMethod() == InsertMethod.direct);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<List<String>> mergeKeys) throws IOException, SQLException
    {
        if (mergeKeys.isPresent()) {
            throw new UnsupportedOperationException("Oracle output plugin doesn't support 'merge_direct' mode.");
        }

        OraclePluginTask oracleTask = (OraclePluginTask) task;
        JdbcOutputConnector connector = getConnector(task, true);

        if (oracleTask.getInsertMethod() == InsertMethod.oci) {
            OracleCharset charset;
            OracleCharset nationalCharset;
            try (OracleOutputConnection connection = (OracleOutputConnection)connector.connect(true)) {
                charset = connection.getOracleCharset();
                nationalCharset = connection.getOracleNationalCharset();
            }

            return new DirectBatchInsert(
                    String.format("%s:%d/%s", oracleTask.getHost().get(), oracleTask.getPort(), oracleTask.getDatabase().get()),
                    oracleTask.getUser(),
                    oracleTask.getPassword(),
                    oracleTask.getSchema().orNull(),
                    charset,
                    nationalCharset,
                    oracleTask.getBatchSize());
        }

        return new StandardBatchInsert(getConnector(task, true), mergeKeys, task.getOnDuplicateKeyUpdate());
    }
}
