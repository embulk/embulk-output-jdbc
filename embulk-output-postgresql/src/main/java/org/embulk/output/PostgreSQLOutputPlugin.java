package org.embulk.output;

import java.util.Properties;
import java.io.IOException;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.embulk.spi.Exec;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.postgresql.PostgreSQLOutputConnector;
import org.embulk.output.postgresql.PostgreSQLCopyBatchInsert;

public class PostgreSQLOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    private static final String DEFAULT_SCHEMA = "public";
    private static final int DEFAULT_PORT = 5432;

    private final Logger logger = Exec.getLogger(PostgreSQLOutputPlugin.class);

    @Override
    protected PostgreSQLOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        String url = String.format("jdbc:postgresql://%s:%d/%s",
                task.getHost(), task.getPort().or(DEFAULT_PORT), task.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", task.getUser());
        props.setProperty("password", task.getPassword());
        props.setProperty("loginTimeout",   "300"); // seconds
        props.setProperty("socketTimeout", "1800"); // seconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        // TODO
        //switch task.getSssl() {
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

        props.putAll(task.getOptions());

        return new PostgreSQLOutputConnector(url, props, task.getSchema().or(DEFAULT_SCHEMA));
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task) throws IOException, SQLException
    {
        return new PostgreSQLCopyBatchInsert(getConnector(task, true));
    }
}
