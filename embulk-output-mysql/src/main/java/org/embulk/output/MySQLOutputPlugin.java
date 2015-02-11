package org.embulk.output;

import java.util.Properties;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Connection;
import org.slf4j.Logger;
import org.embulk.spi.Exec;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.mysql.MySQLOutputConnector;
import org.embulk.output.mysql.MySQLBatchInsert;

public class MySQLOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    private static final int DEFAULT_PORT = 3306;

    private final Logger logger = Exec.getLogger(MySQLOutputPlugin.class);

    @Override
    protected MySQLOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        String url = String.format("jdbc:mysql://%s:%d/%s",
                task.getHost(), task.getPort().or(DEFAULT_PORT), task.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", task.getUser());
        props.setProperty("password", task.getPassword());

        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("useCompression", "true");

        props.setProperty("connectTimeout", "300000"); // milliseconds
        props.setProperty("socketTimeout", "1800000"); // smillieconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        // TODO
        //switch task.getSssl() {
        //when "disable":
        //    break;
        //when "enable":
        //    props.setProperty("useSSL", "true");
        //    props.setProperty("requireSSL", "false");
        //    props.setProperty("verifyServerCertificate", "false");
        //    break;
        //when "verify":
        //    props.setProperty("useSSL", "true");
        //    props.setProperty("requireSSL", "true");
        //    props.setProperty("verifyServerCertificate", "true");
        //    break;
        //}

        if (!retryableMetadataOperation) {
            // non-retryable batch operation uses longer timeout
            props.setProperty("connectTimeout",  "300000");  // milliseconds
            props.setProperty("socketTimeout", "2700000");   // milliseconds
        }

        props.putAll(task.getOptions());

        return new MySQLOutputConnector(url, props);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task) throws IOException, SQLException
    {
        return new MySQLBatchInsert(getConnector(task, true));
    }
}
