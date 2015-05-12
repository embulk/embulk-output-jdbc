package org.embulk.output;

import java.util.List;
import java.util.Properties;
import java.io.IOException;
import java.sql.SQLException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.embulk.spi.Exec;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.setter.ColumnSetter;
import org.embulk.output.postgresql.PostgreSQLOutputConnector;
import org.embulk.output.postgresql.PostgreSQLCopyBatchInsert;
import org.embulk.spi.PageReader;

public class PostgreSQLOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    public interface PostgreSQLPluginTask
            extends PluginTask
    {
        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("5432")
        public int getPort();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

        @Config("database")
        public String getDatabase();

        @Config("schema")
        @ConfigDefault("\"public\"")
        public String getSchema();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return PostgreSQLPluginTask.class;
    }

    @Override
    protected Features getFeatures(PluginTask task)
    {
        return new Features()
            .setMaxTableNameLength(30)
            .setSupportedModes(ImmutableSet.of(Mode.INSERT, Mode.INSERT_DIRECT, Mode.MERGE, Mode.TRUNCATE_INSERT, Mode.REPLACE))
            .setIgnoreMergeKeys(false);
    }

    @Override
    protected PostgreSQLOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        PostgreSQLPluginTask t = (PostgreSQLPluginTask) task;

        String url = String.format("jdbc:postgresql://%s:%d/%s",
                t.getHost(), t.getPort(), t.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", t.getUser());
        props.setProperty("password", t.getPassword());
        props.setProperty("loginTimeout",   "300"); // seconds
        props.setProperty("socketTimeout", "1800"); // seconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        // TODO
        //switch t.getSssl() {
        //when "disable":
        //    break;
        //when "enable":
        //    props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");  // disable server-side validation
        //when "verify":
        //    props.setProperty("ssl", "true");
        //    break;
        //}

        if (!retryableMetadataOperation) {
            // non-retryable batch operation uses longer timeout
            props.setProperty("loginTimeout",    "300");  // seconds
            props.setProperty("socketTimeout", "28800");  // seconds
        }

        props.putAll(t.getOptions());

        return new PostgreSQLOutputConnector(url, props, t.getSchema());
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<List<String>> mergeKeys) throws IOException, SQLException
    {
        if (mergeKeys.isPresent()) {
            throw new UnsupportedOperationException("PostgreSQL output plugin doesn't support 'merge_direct' mode. Use 'merge' mode instead.");
        }
        return new PostgreSQLCopyBatchInsert(getConnector(task, true));
    }
}
