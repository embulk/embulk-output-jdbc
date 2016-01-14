package org.embulk.output;

import java.util.List;
import java.util.Properties;
import java.io.IOException;
import java.sql.SQLException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.postgresql.PostgreSQLOutputConnector;
import org.embulk.output.postgresql.PostgreSQLCopyBatchInsert;

import com.google.common.collect.ImmutableList;
import java.sql.Types;
import org.embulk.spi.Schema;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Column;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcSchema;

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

        @Config("ssl")
        @ConfigDefault("false")
        public boolean getSsl();
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
        props.setProperty("loginTimeout",   "300"); // seconds
        props.setProperty("socketTimeout", "1800"); // seconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        if (t.getSsl()) {
            // TODO add ssl_verify (boolean) option to allow users to verify certification.
            //      see embulk-input-ftp for SSL implementation.
            props.setProperty("ssl", "true");
            props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");  // disable server-side validation
        }
        // setting ssl=false enables SSL. See org.postgresql.core.v3.openConnectionImpl.

        if (!retryableMetadataOperation) {
            // non-retryable batch operation uses longer timeout
            props.setProperty("loginTimeout",    "300");  // seconds
            props.setProperty("socketTimeout", "28800");  // seconds
        }

        props.putAll(t.getOptions());

        props.setProperty("user", t.getUser());
        logger.info("Connecting to {} options {}", url, props);
        props.setProperty("password", t.getPassword());

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

    // TODO This is almost copy from AbstractJdbcOutputPlugin excepting type of TIMESTAMP -> TIMESTAMP WITH TIME ZONE.
    //      AbstractJdbcOutputPlugin should have better extensibility.
    @Override
    protected JdbcSchema newJdbcSchemaForNewTable(Schema schema)
    {
        final ImmutableList.Builder<JdbcColumn> columns = ImmutableList.builder();
        for (Column c : schema.getColumns()) {
            final String columnName = c.getName();
            c.visit(new ColumnVisitor() {
                public void booleanColumn(Column column)
                {
                    columns.add(JdbcColumn.newGenericTypeColumn(
                            columnName, Types.BOOLEAN, "BOOLEAN",
                            1, 0, false, false));
                }

                public void longColumn(Column column)
                {
                    columns.add(JdbcColumn.newGenericTypeColumn(
                            columnName, Types.BIGINT, "BIGINT",
                            22, 0, false, false));
                }

                public void doubleColumn(Column column)
                {
                    columns.add(JdbcColumn.newGenericTypeColumn(
                            columnName, Types.FLOAT, "DOUBLE PRECISION",
                            24, 0, false, false));
                }

                public void stringColumn(Column column)
                {
                    columns.add(JdbcColumn.newGenericTypeColumn(
                            columnName, Types.CLOB, "CLOB",
                            4000, 0, false, false));  // TODO size type param
                }

                public void jsonColumn(Column column)
                {
                    throw new UnsupportedOperationException("This plugin doesn't support json type. Please try to upgrade version of the plugin using 'embulk gem update' command. If the latest version still doesn't support json type, please contact plugin developers, or change configuration of input plugin not to use json type.");
                }

                public void timestampColumn(Column column)
                {
                    columns.add(JdbcColumn.newGenericTypeColumn(
                            columnName, Types.TIMESTAMP, "TIMESTAMP WITH TIME ZONE",
                            26, 0, false, false));  // size type param is from postgresql
                }
            });
        }
        return new JdbcSchema(columns.build());
    }
}
