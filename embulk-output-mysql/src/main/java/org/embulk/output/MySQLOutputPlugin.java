package org.embulk.output;

import java.util.Properties;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.Ssl;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.TableIdentifier;
import org.embulk.output.mysql.MySQLOutputConnection;
import org.embulk.output.mysql.MySQLOutputConnector;
import org.embulk.output.mysql.MySQLBatchInsert;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;

public class MySQLOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    public interface MySQLPluginTask
            extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("3306")
        public int getPort();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

        @Config("database")
        public String getDatabase();

        @Config("temp_database")
        @ConfigDefault("null")
        public Optional<String> getTempDatabase();

        @Config("ssl")
        @ConfigDefault("\"disable\"") // backward compatibility
        public Ssl getSsl();

    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return MySQLPluginTask.class;
    }

    @Override
    protected Features getFeatures(PluginTask task)
    {
        return new Features()
            .setMaxTableNameLength(64)
            .setIgnoreMergeKeys(true);
    }

    @Override
    protected JdbcOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        MySQLPluginTask t = (MySQLPluginTask) task;

        loadDriver("com.mysql.jdbc.Driver", t.getDriverPath());

        String url = String.format("jdbc:mysql://%s:%d/%s",
                t.getHost(), t.getPort(), t.getDatabase());

        Properties props = new Properties();

        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("useCompression", "true");

        props.setProperty("connectTimeout", "300000"); // milliseconds
        props.setProperty("socketTimeout", "1800000"); // smillieconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        switch (t.getSsl()) {
            case DISABLE:
                props.setProperty("useSSL", "false");
                break;
            case ENABLE:
                props.setProperty("useSSL", "true");
                props.setProperty("requireSSL", "true");
                props.setProperty("verifyServerCertificate", "false");
                break;
            case VERIFY:
                props.setProperty("useSSL", "true");
                props.setProperty("requireSSL", "true");
                props.setProperty("verifyServerCertificate", "true");
                break;
        }

        if (!retryableMetadataOperation) {
            // non-retryable batch operation uses longer timeout
            props.setProperty("connectTimeout",  "300000");  // milliseconds
            props.setProperty("socketTimeout", "2700000");   // milliseconds
        }

        props.putAll(t.getOptions());

        // TODO validate task.getMergeKeys is null

        props.setProperty("user", t.getUser());
        props.setProperty("password", t.getPassword());
        logConnectionProperties(url, props);

        return new MySQLOutputConnector(url, props, task.getTransactionIsolation());
    }

    @Override
    protected TableIdentifier buildIntermediateTableId(JdbcOutputConnection con, PluginTask task, String tableName) {
        MySQLPluginTask t = (MySQLPluginTask) task;
        if (t.getTempDatabase().isPresent()) {
            return new TableIdentifier(t.getTempDatabase().get(), null, tableName);
        }
        return super.buildIntermediateTableId(con, task, tableName);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        return new MySQLBatchInsert(getConnector(task, true), mergeConfig);
    }


    @Override
    protected boolean isRetryableException(String sqlState, int errorCode)
    {
        switch (errorCode) {
            case 1213: // ER_LOCK_DEADLOCK (Message: Deadlock found when trying to get lock; try restarting transaction)
                return true;
            case 1205: // ER_LOCK_WAIT_TIMEOUT (Message: Lock wait timeout exceeded; try restarting transaction)
                return true;
            default:
                return false;
        }
    }

    @Override
    protected void doBegin(JdbcOutputConnection con,
                           PluginTask task, final Schema schema, int taskCount) throws SQLException
    {
        MySQLOutputConnection mySQLCon = (MySQLOutputConnection)con;
        mySQLCon.compareTimeZone();
        super.doBegin(con,task,schema,taskCount);
    }
}
