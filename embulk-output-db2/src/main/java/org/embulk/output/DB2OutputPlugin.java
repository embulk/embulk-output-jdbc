package org.embulk.output;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.db2.DB2BatchInsert;
import org.embulk.output.db2.DB2OutputConnector;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.MergeConfig;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import static java.util.Locale.ENGLISH;

public class DB2OutputPlugin
        extends AbstractJdbcOutputPlugin
{
    public interface DB2PluginTask
            extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("50000")
        public int getPort();

        @Config("database")
        public String getDatabase();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return DB2PluginTask.class;
    }

    @Override
    protected Features getFeatures(PluginTask task)
    {
        return new Features()
            .setMaxTableNameLength(128) // http://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.sql.ref.doc/doc/r0001029.html
            .setSupportedModes(ImmutableSet.of(Mode.INSERT, Mode.INSERT_DIRECT, Mode.TRUNCATE_INSERT, Mode.REPLACE))
            .setIgnoreMergeKeys(false);
    }

    @Override
    protected DB2OutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        DB2PluginTask db2Task = (DB2PluginTask) task;

        if (db2Task.getDriverPath().isPresent()) {
            loadDriverJar(db2Task.getDriverPath().get());
        }

        String url = String.format(ENGLISH, "jdbc:db2://%s:%d/%s",
                db2Task.getHost(), db2Task.getPort(), db2Task.getDatabase());

        Properties props = new Properties();
        props.putAll(db2Task.getOptions());
        props.setProperty("user", db2Task.getUser());
        if (db2Task.getPassword().isPresent()) {
            props.setProperty("password", db2Task.getPassword().get());
        }

        return new DB2OutputConnector(url, props, null);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        return new DB2BatchInsert(getConnector(task, true), mergeConfig);
    }
}
