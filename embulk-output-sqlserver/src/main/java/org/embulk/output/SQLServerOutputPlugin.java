package org.embulk.output;

import org.embulk.config.ConfigException;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.StandardBatchInsert;
import org.embulk.output.jdbc.TableIdentifier;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.embulk.output.sqlserver.InsertMethod;
import org.embulk.output.sqlserver.NativeBatchInsert;
import org.embulk.output.sqlserver.SQLServerOutputConnector;
import org.embulk.output.sqlserver.setter.SQLServerColumnSetterFactory;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;

import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Properties;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static java.util.Locale.ENGLISH;

public class SQLServerOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    private static int DEFAULT_PORT = 1433;

    public interface SQLServerPluginTask
            extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("driver_type")
        @ConfigDefault("\"mssql-jdbc\"")
        public String getDriverType();

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

        @Config("schema")
        @ConfigDefault("null")
        public Optional<String> getSchema();

        @Config("temp_schema")
        @ConfigDefault("null")
        public Optional<String> getTempSchema();

        @Config("insert_method")
        @ConfigDefault("\"normal\"")
        public InsertMethod getInsertMethod();

        @Config("native_driver")
        @ConfigDefault("null")
        public Optional<String> getNativeDriverName();

        @Config("database_encoding")
        @ConfigDefault("\"MS932\"")
        public String getDatabaseEncoding();

        @Config("connect_timeout")
        @ConfigDefault("null")
        public Optional<Integer> getConnectTimeout();

        @Config("socket_timeout")
        @ConfigDefault("null")
        public Optional<Integer> getSocketTimeout();
    }

    private static class UrlAndProperties {
        private final String url;
        private final Properties props;

        public UrlAndProperties(String url, Properties props)
        {
            this.url = url;
            this.props = props;
        }

        public String getUrl()
        {
            return this.url;
        }

        public Properties getProps()
        {
            return this.props;
        }
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
            .setSupportedModes(Collections.unmodifiableSet(new HashSet<Mode>(Arrays.asList(
                    Mode.INSERT, Mode.INSERT_DIRECT, Mode.MERGE, Mode.TRUNCATE_INSERT, Mode.REPLACE))))
            .setIgnoreMergeKeys(false);
    }

    @Override
    protected JdbcOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        SQLServerPluginTask sqlServerTask = (SQLServerPluginTask) task;

        if (sqlServerTask.getDriverPath().isPresent()) {
            addDriverJarToClasspath(sqlServerTask.getDriverPath().get());
        }

        boolean useJtdsDriver;
        if (sqlServerTask.getDriverType().equalsIgnoreCase("mssql-jdbc")) {
            useJtdsDriver = false;
            try {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
            }
            catch (Exception e) {
                throw new ConfigException("Can't load Microsoft SQLServerDriver from classpath", e);
            }
        } else if (sqlServerTask.getDriverType().equalsIgnoreCase("jtds")) {
            useJtdsDriver = true;
            try {
                Class.forName("net.sourceforge.jtds.jdbc.Driver").newInstance();
            }
            catch (Exception e) {
                throw new ConfigException("Can't load jTDS Driver from classpath", e);
            }
        } else {
            throw new ConfigException("Unknown driver_type : " + sqlServerTask.getDriverType());
        }

        UrlAndProperties urlProps = getUrlAndProperties(sqlServerTask, useJtdsDriver);
        logConnectionProperties(urlProps.getUrl(), urlProps.getProps());
        return new SQLServerOutputConnector(urlProps.getUrl(), urlProps.getProps(), sqlServerTask.getSchema().orElse(null),
                sqlServerTask.getTransactionIsolation());
    }

    private UrlAndProperties getUrlAndProperties(SQLServerPluginTask sqlServerTask, boolean useJtdsDriver)
    {
        Properties props = new Properties();
        String url;

        props.putAll(sqlServerTask.getOptions());
        if (sqlServerTask.getUser().isPresent()) {
            props.setProperty("user", sqlServerTask.getUser().get());
        }
        if (sqlServerTask.getPassword().isPresent()) {
            props.setProperty("password", sqlServerTask.getPassword().get());
        }

        if (sqlServerTask.getUrl().isPresent()) {
            if (sqlServerTask.getInsertMethod() == InsertMethod.NATIVE) {
                throw new IllegalArgumentException("Cannot set 'url' when 'insert_method' is 'native'.");
            }

            if (sqlServerTask.getHost().isPresent()
                    || sqlServerTask.getInstance().isPresent()
                    || sqlServerTask.getDatabase().isPresent()) {
                throw new IllegalArgumentException("'host', 'instance' and 'database' parameters are invalid if 'url' parameter is set.");
            }
            url = sqlServerTask.getUrl().get();
        } else {
            if (!sqlServerTask.getHost().isPresent()) {
                throw new IllegalArgumentException("Field 'host' is not set.");
            }
            if (!sqlServerTask.getDatabase().isPresent()) {
                throw new IllegalArgumentException("Field 'database' is not set.");
            }
        }

        if(useJtdsDriver) {
            // jTDS URL: host:port[/database] or host[/database][;instance=]
            // host:port;instance= is allowed but port will be ignored? in this case.
            if (sqlServerTask.getInstance().isPresent()) {
                if (sqlServerTask.getPort() != DEFAULT_PORT) {
                    logger.warn("'port: {}' option is ignored because instance option is set", sqlServerTask.getPort());
                }
                url = String.format(ENGLISH, "jdbc:jtds:sqlserver://%s", sqlServerTask.getHost().get());
                props.setProperty("instance", sqlServerTask.getInstance().get());
            }
            else {
                url = String.format(ENGLISH, "jdbc:jtds:sqlserver://%s:%d", sqlServerTask.getHost().get(), sqlServerTask.getPort());
            }

            // /database
            if (sqlServerTask.getDatabase().isPresent()) {
                url += "/" + sqlServerTask.getDatabase().get();
            }

            // integratedSecutiry is not supported, user + password is required
            if (sqlServerTask.getIntegratedSecurity().isPresent()) {
                throw new ConfigException("'integratedSecutiry' option is not supported with jTDS driver. Set 'driver_path: /path/to/sqljdbc.jar' option if you want to use Microsoft SQLServerDriver.");
            }

            if (!sqlServerTask.getUser().isPresent()) {
                throw new ConfigException("'user' option is required but not set.");
            }

            if (sqlServerTask.getSocketTimeout().isPresent()) {
                props.setProperty("socketTimeout", String.valueOf(sqlServerTask.getSocketTimeout().get())); // seconds
            }
        } else {
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

            if (sqlServerTask.getSocketTimeout().isPresent()) {
                props.setProperty("socketTimeout", String.valueOf(sqlServerTask.getSocketTimeout().get() * 1000L)); // milliseconds
            }

            url = urlBuilder.toString();
        }

        if (sqlServerTask.getConnectTimeout().isPresent()) {
            props.setProperty("loginTimeout", String.valueOf(sqlServerTask.getConnectTimeout().get())); // seconds
        }

        return new UrlAndProperties(url, props);
    }

    @Override
    protected TableIdentifier buildIntermediateTableId(JdbcOutputConnection con, PluginTask task, String tableName) {
        SQLServerPluginTask sqlServerTask = (SQLServerPluginTask) task;
        // replace mode doesn't support temp_schema because sp_rename cannot change schema of table
        if (sqlServerTask.getTempSchema().isPresent() && sqlServerTask.getMode() != Mode.REPLACE) {
            return new TableIdentifier(null, sqlServerTask.getTempSchema().get(), tableName);
        }
        return super.buildIntermediateTableId(con, task, tableName);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        SQLServerPluginTask sqlServerTask = (SQLServerPluginTask) task;
        if (sqlServerTask.getInsertMethod() == InsertMethod.NATIVE) {
            return new NativeBatchInsert(
                    sqlServerTask.getHost().get(), sqlServerTask.getPort(), sqlServerTask.getInstance(),
                    sqlServerTask.getDatabase().get(), sqlServerTask.getUser(), sqlServerTask.getPassword(),
                    sqlServerTask.getNativeDriverName(), sqlServerTask.getDatabaseEncoding());
        }
        return new StandardBatchInsert(getConnector(task, true), mergeConfig);
    }

    @Override
    protected ColumnSetterFactory newColumnSetterFactory(final BatchInsert batch, final ZoneId defaultTimeZone)
    {
        return new SQLServerColumnSetterFactory(batch, defaultTimeZone);
    }
}
