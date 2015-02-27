package org.embulk.output;

import java.nio.file.Paths;
import java.util.Set;
import java.util.HashSet;
import java.util.Properties;
import java.sql.Driver;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.embulk.spi.Exec;
import org.embulk.spi.PluginClassLoader;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.StandardBatchInsert;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.JdbcOutputConnection;

public class JdbcOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    private final static Set<String> loadedJarGlobs = new HashSet<String>();

    public interface GenericPluginTask extends PluginTask
    {
        @Config("driver_name")
        public String getDriverName();

        @Config("driver_class")
        public String getDriverClass();

        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return GenericPluginTask.class;
    }

    @Override
    protected GenericOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        GenericPluginTask g = (GenericPluginTask) task;

        if (g.getDriverPath().isPresent()) {
            synchronized (loadedJarGlobs) {
                String glob = g.getDriverPath().get();
                if (!loadedJarGlobs.contains(glob)) {
                    loadDriverJar(glob);
                    loadedJarGlobs.add(glob);
                }
            }
        }

        String url;
        if (g.getPort().isPresent()) {
            url = String.format("jdbc:%s://%s:%d/%s",
                    g.getDriverName(), g.getHost(), g.getPort().get(), g.getDatabase());
        } else {
            url = String.format("jdbc:%s://%s/%s",
                    g.getDriverName(), g.getHost(), g.getDatabase());
        }

        Properties props = new Properties();
        props.setProperty("user", g.getUser());
        props.setProperty("password", g.getPassword());

        props.putAll(g.getOptions());

        return new GenericOutputConnector(url, props, g.getDriverClass(),
                g.getSchema().orNull());
    }

    private void loadDriverJar(String glob)
    {
        // TODO match glob
        PluginClassLoader loader = (PluginClassLoader) getClass().getClassLoader();
        loader.addPath(Paths.get(glob));
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
            } catch (Exception ex) {
                throw Throwables.propagate(ex);
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
    protected BatchInsert newBatchInsert(PluginTask task) throws IOException, SQLException
    {
        return new StandardBatchInsert(getConnector(task, true));
    }
}
