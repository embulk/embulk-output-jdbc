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
            TimestampFormatter timestampFormatter) {
        return new OracleColumnSetterFactory(batch, pageReader, timestampFormatter);
    }
    
    @Override
    protected String generateSwapTableName(PluginTask task) throws SQLException {
        OracleOutputConnector connector = getConnector(task, true);

        int minLength = 3;
        String tablePrefix = task.getTable();
        if (tablePrefix.length() > MAX_TABLE_NAME_LENGTH - minLength) {
            tablePrefix = tablePrefix.substring(0, MAX_TABLE_NAME_LENGTH - minLength);
        }

        try (OracleOutputConnection connection = connector.connect(true)) {
            for (int i = 0; ; i++) {
                String s = Integer.toString(i);
                if (tablePrefix.length() + s.length() > MAX_TABLE_NAME_LENGTH) {
                    break; 
                }

                StringBuilder sb = new StringBuilder();
                sb.append(tablePrefix);
                for (int j = tablePrefix.length(); j < MAX_TABLE_NAME_LENGTH - s.length(); j++) {
                    sb.append("0");
                }
                sb.append(s);

                String table = sb.toString();
                if (!connection.tableExists(table)) {
                    return table;
                }
                
                if (i == Integer.MAX_VALUE) {
                	break;
                }
            }
        }
        
        throw new SQLException("Cannot generate a swap table name."); 
    }
    
}
