package org.embulk.output;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;
import java.util.Optional;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.*;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.embulk.output.postgresql.PostgreSQLCopyBatchInsert;
import org.embulk.output.postgresql.PostgreSQLOutputConnector;
import org.embulk.output.postgresql.setter.PostgreSQLColumnSetterFactory;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class PostgreSQLOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    public interface PostgreSQLPluginTask
            extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

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

        @Config("temp_schema")
        @ConfigDefault("null")
        public Optional<String> getTempSchema();

        @Config("ssl")
        @ConfigDefault("false")
        public boolean getSsl();

        @Config("role_name")
        @ConfigDefault("null")
        public Optional<String> getRoleName();
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
            .setMaxTableNameLength(63)
            .setSupportedModes(ImmutableSet.of(Mode.INSERT, Mode.INSERT_DIRECT, Mode.MERGE, Mode.MERGE_DIRECT, Mode.TRUNCATE_INSERT, Mode.REPLACE))
            .setIgnoreMergeKeys(false);
    }

    @Override
    protected JdbcOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        PostgreSQLPluginTask t = (PostgreSQLPluginTask) task;

        loadDriver("org.postgresql.Driver", t.getDriverPath());

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
        props.setProperty("password", t.getPassword());
        logConnectionProperties(url, props);

        return new PostgreSQLOutputConnector(url, props, t.getSchema(), t.getTransactionIsolation(),
                t.getRoleName().orElse(null));
    }

    @Override
    protected TableIdentifier buildIntermediateTableId(JdbcOutputConnection con, PluginTask task, String tableName) {
        PostgreSQLPluginTask t = (PostgreSQLPluginTask) task;
        // replace mode doesn't support temp_schema because ALTER TABLE cannot change schema of table
        if (t.getTempSchema().isPresent() && t.getMode() != Mode.REPLACE) {
            return new TableIdentifier(null, t.getTempSchema().get(), tableName);
        }
        return super.buildIntermediateTableId(con, task, tableName);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        if (mergeConfig.isPresent()) {
            return new StandardBatchInsert(getConnector(task, true), mergeConfig);
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
                    columns.add(JdbcColumn.newGenericTypeColumn(
                            columnName, Types.OTHER, "JSON",
                            4000, 0, false, false));  // TODO size type param
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

    @Override
    protected ColumnSetterFactory newColumnSetterFactory(final BatchInsert batch, final String defaultTimeZone)
    {
        return new PostgreSQLColumnSetterFactory(batch, defaultTimeZone);
    }
}
