package org.embulk.output;

import java.util.Properties;
import java.sql.Driver;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.output.jdbc.*;

public class JdbcOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    public interface GenericPluginTask extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("driver_class")
        public String getDriverClass();

        @Config("url")
        public String getUrl();

        @Config("user")
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();

        @Config("schema")
        @ConfigDefault("null")
        public Optional<String> getSchema();

        @Config("max_table_name_length")
        @ConfigDefault("30")
        public int getMaxTableNameLength();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return GenericPluginTask.class;
    }

    @Override
    protected Features getFeatures(PluginTask task)
    {
        GenericPluginTask t = (GenericPluginTask) task;
        return new Features()
            .setMaxTableNameLength(t.getMaxTableNameLength())
            .setSupportedModes(Collections.unmodifiableSet(
                    new HashSet<>(Arrays.asList(Mode.INSERT, Mode.INSERT_DIRECT, Mode.TRUNCATE_INSERT, Mode.REPLACE))));
    }

    @Override
    protected GenericOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        GenericPluginTask t = (GenericPluginTask) task;

        if (t.getDriverPath().isPresent()) {
            addDriverJarToClasspath(t.getDriverPath().get());
        }

        Properties props = new Properties();

        props.putAll(t.getOptions());

        if (t.getUser().isPresent()) {
            props.setProperty("user", t.getUser().get());
        }
        if (t.getPassword().isPresent()) {
            props.setProperty("password", t.getPassword().get());
        }
        logConnectionProperties(t.getUrl(), props);

        return new GenericOutputConnector(t.getUrl(), props, t.getDriverClass(),
                t.getSchema().orElse(null));
    }

    private static class GenericOutputConnector
            implements JdbcOutputConnector
    {
        private final Driver driver;
        private final String url;
        private final Properties properties;
        private final String schemaName;

        public GenericOutputConnector(String url, Properties properties, String driverClass,
                String schemaName)
        {
            try {
                // TODO check Class.forName(driverClass) is a Driver before newInstance
                //      for security
                this.driver = (Driver) Class.forName(driverClass).newInstance();
            } catch (final RuntimeException ex) {
                throw ex;
            } catch (final Exception ex) {
                throw new RuntimeException(ex);
            }
            this.url = url;
            this.properties = properties;
            this.schemaName = schemaName;
        }

        @Override
        public JdbcOutputConnection connect(boolean autoCommit) throws SQLException
        {
            Connection c = driver.connect(url, properties);
            try {
                c.setAutoCommit(autoCommit);
                JdbcOutputConnection con = new JdbcOutputConnection(c, schemaName);
                c = null;
                return con;
            } finally {
                if (c != null) {
                    c.close();
                }
            }
        }
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        return new StandardBatchInsert(getConnector(task, true), mergeConfig);
    }
}
