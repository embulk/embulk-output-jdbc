package org.embulk.output;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.StandardBatchInsert;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.embulk.output.oracle.OracleOutputConnection;
import org.embulk.output.oracle.OracleOutputConnector;
import org.embulk.output.oracle.setter.OracleColumnSetterFactory;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.TimestampFormatter;

import com.google.common.base.Optional;


public class OracleOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    private static final int MAX_TABLE_NAME_LENGTH = 30;

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

        @Config("url")
        @ConfigDefault("null")
        public Optional<String> getUrl();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return OraclePluginTask.class;
    }

    @Override
    protected OracleOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        OraclePluginTask oracleTask = (OraclePluginTask) task;

        if (oracleTask.getDriverPath().isPresent()) {
            loadDriverJar(oracleTask.getDriverPath().get());
        }

        String url;
        if (oracleTask.getUrl().isPresent()) {
            if (oracleTask.getHost().isPresent() || oracleTask.getDatabase().isPresent()) {
                throw new IllegalArgumentException("'host', 'port' and 'database' parameters are invalid if 'url' parameter is set.");
            }

            url = oracleTask.getUrl().get();
        } else {
            if (!oracleTask.getHost().isPresent()) {
                throw new IllegalArgumentException("Field 'host' is not set.");
            }
            if (!oracleTask.getDatabase().isPresent()) {
                throw new IllegalArgumentException("Field 'database' is not set.");
            }

            url = String.format("jdbc:oracle:thin:@%s:%d:%s",
                    oracleTask.getHost().get(), oracleTask.getPort(), oracleTask.getDatabase().get());
        }

        Properties props = new Properties();
        props.setProperty("user", oracleTask.getUser());
        props.setProperty("password", oracleTask.getPassword());
        props.putAll(oracleTask.getOptions());

        return new OracleOutputConnector(url, props);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task) throws IOException, SQLException
    {
        return new StandardBatchInsert(getConnector(task, true));
    }

    @Override
    protected ColumnSetterFactory newColumnSetterFactory(BatchInsert batch, PageReader pageReader,
            TimestampFormatter timestampFormatter)
    {
        return new OracleColumnSetterFactory(batch, pageReader, timestampFormatter);
    }

    @Override
    protected String generateSwapTableName(PluginTask task) throws SQLException
    {
        return generateSwapTableName(task, "_bl_tmp", MAX_TABLE_NAME_LENGTH);
    }

    // TODO move this method to AbstractJdbcOutputPlugin
    protected String generateSwapTableName(PluginTask task, String suffix, int maxTableNameLength) throws SQLException
    {
        Stirng tableName = task.getTable();
        String uniqueSuffix = getTransactionUniqueName() + suffix;

        if (tableName.length() + uniqueSuffix.length() + 1 > maxTableNameLength) {  // + 1 for '_'
            // truncate transaction unique name
            int suffixLength = Math.max(maxTableNameLength - tableName.length() - 1, suffix.size() + 8);  // include 8 characters of the transaction name at least
            uniqueSuffix = uniqueSuffix.substring(uniqueSuffix.length() - suffixLength);
        }

        if (tableName.length() + uniqueSuffix.length() + 1 > maxTableNameLength) {
            // use truncated table name
            int truncLength = maxTableNameLength - uniqueSuffix.length() - 1;
            while (true) {
                truncLength--;
                if (truncLength <= 0) {
                    throw new ConfigException("Table name is too long to generate temporary table name");
                }
                tableName = tableName.substring(0, truncLength);
                if (!connection.tableExists(tableName)) {
                    return tableName + "_" + uniqueSuffix;
                }
            }
        }

        return tableName + "_" + uniqueSuffix;
    }
}
