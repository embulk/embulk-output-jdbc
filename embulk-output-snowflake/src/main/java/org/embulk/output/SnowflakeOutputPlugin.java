package org.embulk.output;

import java.sql.*;
import java.util.Properties;
import java.util.List;
import java.io.IOException;
import java.util.Locale;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableList;


import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.*;
import org.embulk.output.snowflake.SnowflakeOutputConnector;
import org.embulk.spi.Schema;
import org.embulk.output.snowflake.SnowflakeOutputConnection;

public class SnowflakeOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    public interface GenericPluginTask extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("url")
        public String getUrl();

        @Config("user")
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();

        @Config("schema")
        @ConfigDefault("null")
        public Optional<String> getSchema();

        @Config("max_table_name_length")
        @ConfigDefault("30")
        public int getMaxTableNameLength();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return GenericPluginTask.class;
    }

    @Override
    protected Features getFeatures(PluginTask task)
    {
        GenericPluginTask t = (GenericPluginTask) task;
        return new Features()
            .setMaxTableNameLength(t.getMaxTableNameLength())
            .setSupportedModes(ImmutableSet.of(Mode.INSERT, Mode.INSERT_DIRECT, Mode.TRUNCATE_INSERT, Mode.REPLACE));
    }

    @Override
    protected SnowflakeOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        GenericPluginTask t = (GenericPluginTask) task;

        if (t.getDriverPath().isPresent()) {
            addDriverJarToClasspath(t.getDriverPath().get());
        }

        Properties props = new Properties();

        props.putAll(t.getOptions());

        if (t.getUser().isPresent()) {
            props.setProperty("user", t.getUser().get());
        }
        if (t.getPassword().isPresent()) {
            props.setProperty("password", t.getPassword().get());
        }
        logConnectionProperties(t.getUrl(), props);

        return new SnowflakeOutputConnector(t.getUrl(), props,
                t.getSchema().orNull(), t.getTransactionIsolation());
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        return new StandardBatchInsert(getConnector(task, true), mergeConfig);
    }

    @Override
    protected void doBegin(JdbcOutputConnection con,
                           PluginTask task, final Schema schema, int taskCount) throws SQLException
    {

        SnowflakeOutputConnection snowflakeCon = (SnowflakeOutputConnection)con;

        super.doBegin(snowflakeCon, task, schema, taskCount);
    }

    @Override
    public Optional<JdbcSchema> newJdbcSchemaFromTableIfExists(JdbcOutputConnection connection,
                                                               TableIdentifier table) throws SQLException
    {
        if (!connection.tableExists(table)) {
            // DatabaseMetaData.getPrimaryKeys fails if table does not exist
            return Optional.absent();
        }

        DatabaseMetaData dbm = connection.getMetaData();
        String escape = dbm.getSearchStringEscape();
        String catalog = dbm.getConnection().getCatalog();

        ResultSet rs = dbm.getPrimaryKeys(catalog, table.getSchemaName(), table.getTableName());
        ImmutableSet.Builder<String> primaryKeysBuilder = ImmutableSet.builder();
        try {
            while(rs.next()) {
                primaryKeysBuilder.add(rs.getString("COLUMN_NAME"));
            }
        } finally {
            rs.close();
        }
        ImmutableSet<String> primaryKeys = primaryKeysBuilder.build();

        ImmutableList.Builder<JdbcColumn> builder = ImmutableList.builder();
        rs = dbm.getColumns(
                catalog,
                JdbcUtils.escapeSearchString(table.getSchemaName(), escape),
                JdbcUtils.escapeSearchString(table.getTableName(), escape),
                null);
        try {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String simpleTypeName = rs.getString("TYPE_NAME").toUpperCase(Locale.ENGLISH);
                boolean isUniqueKey = primaryKeys.contains(columnName);
                int sqlType = rs.getInt("DATA_TYPE");
                int colSize = rs.getInt("COLUMN_SIZE");
                int decDigit = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    decDigit = -1;
                }
                int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
                boolean isNotNull = "NO".equals(rs.getString("IS_NULLABLE"));
                //rs.getString("COLUMN_DEF") // or null  // TODO
                builder.add(JdbcColumn.newGenericTypeColumn(
                        columnName, sqlType, simpleTypeName, colSize, decDigit, charOctetLength, isNotNull, isUniqueKey));
                // We can't get declared column name using JDBC API.
                // Subclasses need to overwrite it.
            }
        } finally {
            rs.close();
        }
        List<JdbcColumn> columns = builder.build();
        if (columns.isEmpty()) {
            return Optional.absent();
        } else {
            return Optional.of(new JdbcSchema(columns));
        }
    }
}
