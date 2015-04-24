package org.embulk.output;

import java.io.IOException;
import java.lang.UnsupportedOperationException;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.StandardBatchInsert;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.embulk.output.oracle.DirectBatchInsert;
import org.embulk.output.oracle.InsertMethod;
import org.embulk.output.oracle.OracleCharset;
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

        @Config("insert_method")
        @ConfigDefault("\"normal\"")
        public InsertMethod getInsertMethod();
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
        if (oracleTask.getInsertMethod() == InsertMethod.oci) {
            if (!oracleTask.getHost().isPresent()) {
                throw new IllegalArgumentException("Field 'host' is not set.");
            }
            if (!oracleTask.getDatabase().isPresent()) {
                throw new IllegalArgumentException("Field 'database' is not set.");
            }
        } else {
            if (oracleTask.getUrl().isPresent()) {
                if (oracleTask.getHost().isPresent() || oracleTask.getDatabase().isPresent()) {
                    throw new IllegalArgumentException("'host', 'port' and 'database' parameters are invalid if 'url' parameter is set.");
                }
            } else {
                if (!oracleTask.getHost().isPresent()) {
                    throw new IllegalArgumentException("Field 'host' is not set.");
                }
                if (!oracleTask.getDatabase().isPresent()) {
                    throw new IllegalArgumentException("Field 'database' is not set.");
                }
            }
        }

        if (oracleTask.getUrl().isPresent()) {
            url = oracleTask.getUrl().get();
        } else {
            url = String.format("jdbc:oracle:thin:@%s:%d:%s",
                    oracleTask.getHost().get(), oracleTask.getPort(), oracleTask.getDatabase().get());
        }

        Properties props = new Properties();
        props.setProperty("user", oracleTask.getUser());
        props.setProperty("password", oracleTask.getPassword());
        props.putAll(oracleTask.getOptions());

        return new OracleOutputConnector(url, props, oracleTask.getInsertMethod() == InsertMethod.direct);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task) throws IOException, SQLException
    {
        if (task.getMode().isMerge()) {
            throw new UnsupportedOperationException("mode 'merge' is not implemented for this type");
        }

        OraclePluginTask oracleTask = (OraclePluginTask) task;
        JdbcOutputConnector connector = getConnector(task, true);

        if (oracleTask.getInsertMethod() == InsertMethod.oci) {
            OracleCharset charset;
            try (JdbcOutputConnection connection = connector.connect(true)) {
                charset = ((OracleOutputConnection)connection).getCharset();
            }

            return new DirectBatchInsert(
                    String.format("%s:%d/%s", oracleTask.getHost().get(), oracleTask.getPort(), oracleTask.getDatabase().get()),
                    oracleTask.getUser(),
                    oracleTask.getPassword(),
                    oracleTask.getTable(),
                    charset,
                    oracleTask.getBatchSize());
        }

        return new StandardBatchInsert(getConnector(task, true));
    }

    @Override
    protected ColumnSetterFactory newColumnSetterFactory(BatchInsert batch, PageReader pageReader,
            TimestampFormatter timestampFormatter)
    {
        return new OracleColumnSetterFactory(batch, pageReader, timestampFormatter);
    }

    @Override
    protected boolean isValidIdentifier(JdbcOutputConnection con, String identifier) throws SQLException
    {
        // TODO: support multibyte identifier
        return identifier.length() < MAX_TABLE_NAME_LENGTH;
    }

}
