package org.embulk.output.jdbc;

import java.util.List;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;

import com.google.common.base.Optional;

import org.embulk.spi.Exec;

public class JdbcOutputConnection
        implements AutoCloseable
{
    private final Logger logger = Exec.getLogger(JdbcOutputConnection.class);
    protected final Connection connection;
    protected final String schemaName;
    protected final DatabaseMetaData databaseMetaData;
    protected String identifierQuoteString;

    public JdbcOutputConnection(Connection connection, String schemaName)
            throws SQLException
    {
        this.connection = connection;
        this.schemaName = schemaName;
        this.databaseMetaData = connection.getMetaData();
        this.identifierQuoteString = databaseMetaData.getIdentifierQuoteString();
        if (schemaName != null) {
            setSearchPath(schemaName);
        }
    }

    @Override
    public void close() throws SQLException
    {
        if (!connection.isClosed()) {
            connection.close();
        }
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public DatabaseMetaData getMetaData() throws SQLException
    {
        return databaseMetaData;
    }

    public Charset getTableNameCharset() throws SQLException
    {
        return StandardCharsets.UTF_8;
    }

    protected void setSearchPath(String schema) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            String sql = "SET search_path TO " + quoteIdentifierString(schema);
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } finally {
            stmt.close();
        }
    }

    public boolean tableExists(TableIdentifier table) throws SQLException
    {
        try (ResultSet rs = connection.getMetaData().getTables(null, table.getSchemaName(), table.getTableName(), null)) {
            return rs.next();
        }
    }

    public boolean tableExists(String tableName) throws SQLException
    {
        return tableExists(new TableIdentifier(schemaName, tableName));
    }

    public void dropTableIfExists(TableIdentifier table) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            dropTableIfExists(stmt, table);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected void dropTableIfExists(Statement stmt, TableIdentifier table) throws SQLException
    {
        String sql = String.format("DROP TABLE IF EXISTS %s", quoteTableIdentifier(table));
        executeUpdate(stmt, sql);
    }

    public void dropTable(TableIdentifier table) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            dropTable(stmt, table);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected void dropTable(Statement stmt, TableIdentifier table) throws SQLException
    {
        String sql = String.format("DROP TABLE %s", quoteTableIdentifier(table));
        executeUpdate(stmt, sql);
    }

    public void createTableIfNotExists(TableIdentifier targetTable, JdbcSchema schema) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            String sql = buildCreateTableIfNotExistsSql(targetTable, schema);
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected String buildCreateTableIfNotExistsSql(TableIdentifier table, JdbcSchema schema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE IF NOT EXISTS ");
        quoteTableIdentifier(sb, table);
        sb.append(buildCreateTableSchemaSql(schema));
        return sb.toString();
    }

    public void createTable(TableIdentifier table, JdbcSchema schema) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            String sql = buildCreateTableSql(table, schema);
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected String buildCreateTableSql(TableIdentifier table, JdbcSchema schema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ");
        quoteTableIdentifier(sb, table);
        sb.append(buildCreateTableSchemaSql(schema));
        return sb.toString();
    }

    protected String buildCreateTableSchemaSql(JdbcSchema schema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(" (");
        for (int i=0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, schema.getColumnName(i));
            sb.append(" ");
            String typeName = getCreateTableTypeName(schema.getColumn(i));
            sb.append(typeName);
        }
        sb.append(")");

        return sb.toString();
    }

    protected String buildRenameTableSql(TableIdentifier fromTable, TableIdentifier toTable)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        quoteTableIdentifier(sb, fromTable);
        sb.append(" RENAME TO ");
        quoteTableIdentifier(sb, toTable);
        return sb.toString();
    }

    public static enum ColumnDeclareType
    {
        SIMPLE,
        SIZE,
        SIZE_AND_SCALE,
        SIZE_AND_OPTIONAL_SCALE,
    };

    protected String getCreateTableTypeName(JdbcColumn c)
    {
        if (c.getDeclaredType().isPresent()) {
            return c.getDeclaredType().get();
        } else {
            return buildColumnTypeName(c);
        }
    }

    protected String buildColumnTypeName(JdbcColumn c)
    {
        String simpleTypeName = c.getSimpleTypeName();
        switch (getColumnDeclareType(simpleTypeName, c)) {
        case SIZE:
            return String.format("%s(%d)", simpleTypeName, c.getSizeTypeParameter());
        case SIZE_AND_SCALE:
            if (c.getScaleTypeParameter() < 0) {
                return String.format("%s(%d,0)", simpleTypeName, c.getSizeTypeParameter());
            } else {
                return String.format("%s(%d,%d)", simpleTypeName, c.getSizeTypeParameter(), c.getScaleTypeParameter());
            }
        case SIZE_AND_OPTIONAL_SCALE:
            if (c.getScaleTypeParameter() < 0) {
                return String.format("%s(%d)", simpleTypeName, c.getSizeTypeParameter());
            } else {
                return String.format("%s(%d,%d)", simpleTypeName, c.getSizeTypeParameter(), c.getScaleTypeParameter());
            }
        default:  // SIMPLE
            return simpleTypeName;
        }
    }

    // TODO
    private static final String[] STANDARD_SIZE_TYPE_NAMES = new String[] {
        "CHAR",
        "VARCHAR", "CHAR VARYING", "CHARACTER VARYING", "LONGVARCHAR",
        "NCHAR",
        "NVARCHAR", "NCHAR VARYING", "NATIONAL CHAR VARYING", "NATIONAL CHARACTER VARYING",
        "BINARY",
        "VARBINARY", "BINARY VARYING", "LONGVARBINARY",
        "BIT",
        "VARBIT", "BIT VARYING",
        "FLOAT",  // SQL standard's FLOAT[(p)] optionally accepts precision
    };

    private static final String[] STANDARD_SIZE_AND_SCALE_TYPE_NAMES = new String[] {
        "DECIMAL",
        "NUMERIC",
    };

    protected ColumnDeclareType getColumnDeclareType(String convertedTypeName, JdbcColumn col)
    {
        for (String x : STANDARD_SIZE_TYPE_NAMES) {
            if (x.equals(convertedTypeName)) {
                return ColumnDeclareType.SIZE;
            }
        }

        for (String x : STANDARD_SIZE_AND_SCALE_TYPE_NAMES) {
            if (x.equals(convertedTypeName)) {
                return ColumnDeclareType.SIZE_AND_SCALE;
            }
        }

        return ColumnDeclareType.SIMPLE;
    }

    public PreparedStatement prepareBatchInsertStatement(TableIdentifier toTable, JdbcSchema toTableSchema, Optional<MergeConfig> mergeConfig) throws SQLException
    {
        String sql;
        if (mergeConfig.isPresent()) {
            sql = buildPreparedMergeSql(toTable, toTableSchema, mergeConfig.get());
        } else {
            sql = buildPreparedInsertSql(toTable, toTableSchema);
        }
        logger.info("Prepared SQL: {}", sql);
        return connection.prepareStatement(sql);
    }

    protected String buildPreparedInsertSql(TableIdentifier toTable, JdbcSchema toTableSchema) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ");
        quoteTableIdentifier(sb, toTable);

        sb.append(" (");
        for (int i=0; i < toTableSchema.getCount(); i++) {
            if(i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, toTableSchema.getColumnName(i));
        }
        sb.append(") VALUES (");
        for(int i=0; i < toTableSchema.getCount(); i++) {
            if(i != 0) { sb.append(", "); }
            sb.append("?");
        }
        sb.append(")");

        return sb.toString();
    }

    protected String buildPreparedMergeSql(TableIdentifier toTable, JdbcSchema toTableSchema, MergeConfig mergeConfig) throws SQLException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    protected void executeSql(String sql) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected void collectInsert(List<TableIdentifier> fromTables, JdbcSchema schema, TableIdentifier toTable,
            boolean truncateDestinationFirst, Optional<String> preSql, Optional<String> postSql) throws SQLException
    {
        if (fromTables.isEmpty()) {
            return;
        }

        Statement stmt = connection.createStatement();
        try {
            if (truncateDestinationFirst) {
                String sql = buildTruncateSql(toTable);
                executeUpdate(stmt, sql);
            }

            if (preSql.isPresent()) {
                executeUpdate(stmt, preSql.get());
            }

            String sql = buildCollectInsertSql(fromTables, schema, toTable);
            executeUpdate(stmt, sql);

            if (postSql.isPresent()) {
                executeUpdate(stmt, postSql.get());
            }

            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected String buildTruncateSql(TableIdentifier table)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("DELETE FROM ");
        quoteTableIdentifier(sb, table);

        return sb.toString();
    }

    protected String buildCollectInsertSql(List<TableIdentifier> fromTables, JdbcSchema schema, TableIdentifier toTable)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ");
        quoteTableIdentifier(sb, toTable);
        sb.append(" (");
        for (int i=0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, schema.getColumnName(i));
        }
        sb.append(") ");
        for (int i=0; i < fromTables.size(); i++) {
            if (i != 0) { sb.append(" UNION ALL "); }
            sb.append("SELECT ");
            for (int j=0; j < schema.getCount(); j++) {
                if (j != 0) { sb.append(", "); }
                quoteIdentifierString(sb, schema.getColumnName(j));
            }
            sb.append(" FROM ");
            quoteTableIdentifier(sb, fromTables.get(i));
        }

        return sb.toString();
    }

    protected void collectMerge(List<TableIdentifier> fromTables, JdbcSchema schema, TableIdentifier toTable, MergeConfig mergeConfig,
            Optional<String> preSql, Optional<String> postSql) throws SQLException
    {
        if (fromTables.isEmpty()) {
            return;
        }

        Statement stmt = connection.createStatement();
        try {
            if (preSql.isPresent()) {
                executeUpdate(stmt, preSql.get());
            }

            String sql = buildCollectMergeSql(fromTables, schema, toTable, mergeConfig);
            executeUpdate(stmt, sql);

            if (postSql.isPresent()) {
                executeUpdate(stmt, postSql.get());
            }

            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected String buildCollectMergeSql(List<TableIdentifier> fromTables, JdbcSchema schema, TableIdentifier toTable, MergeConfig mergeConfig) throws SQLException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    public void replaceTable(TableIdentifier fromTable, JdbcSchema schema, TableIdentifier toTable, Optional<String> postSql) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            dropTableIfExists(stmt, toTable);

            executeUpdate(stmt, buildRenameTableSql(fromTable, toTable));

            if (postSql.isPresent()) {
                executeUpdate(stmt, postSql.get());
            }

            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected String quoteTableIdentifier(TableIdentifier tableId)
    {
        return quoteIdentifierString(tableId.getTableName(), identifierQuoteString);
    }

    protected void quoteTableIdentifier(StringBuilder sb, TableIdentifier tableId)
    {
        sb.append(quoteIdentifierString(tableId.getTableName(), identifierQuoteString));
    }

    protected void quoteIdentifierString(StringBuilder sb, String str)
    {
        sb.append(quoteIdentifierString(str, identifierQuoteString));
    }

    protected String quoteIdentifierString(String str)
    {
        return quoteIdentifierString(str, identifierQuoteString);
    }

    protected String quoteIdentifierString(String str, String quoteString)
    {
        // TODO if identifierQuoteString.equals(" ") && str.contains([^a-zA-Z0-9_connection.getMetaData().getExtraNameCharacters()])
        // TODO if str.contains(identifierQuoteString);
        return quoteString + str + quoteString;
    }

    // PostgreSQL JDBC driver implements isValid() method. But the
    // implementation throws following exception:
    // "java.io.IOException: Method org.postgresql.jdbc4.Jdbc4Connection.isValid(int) is not yet implemented."
    //
    // So, checking mechanism doesn't work at all.
    // Thus here just runs "SELECT 1" to check connectivity.
    //
    public boolean isValidConnection(int timeout) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            stmt.executeQuery("SELECT 1").close();
            return true;
        } catch (SQLException ex) {
            return false;
        } finally {
            stmt.close();
        }
    }

    protected String[] getDeterministicSqlStates()
    {
        return new String[0];
    }

    protected int[] getDeterministicErrorCodes()
    {
        return new int[0];
    }

    protected Class[] getDeterministicRootCauses()
    {
        return new Class[] {
            // Don't retry on UnknownHostException.
            java.net.UnknownHostException.class,

            //// we should not retry on connect() error?
            //java.net.ConnectException.class,
        };
    }

    public boolean isRetryableException(SQLException exception)
    {
        String sqlState = exception.getSQLState();
        for (String deterministic : getDeterministicSqlStates()) {
            if (sqlState.equals(deterministic)) {
                return false;
            }
        }

        int errorCode = exception.getErrorCode();
        for (int deterministic : getDeterministicErrorCodes()) {
            if (errorCode == deterministic) {
                return false;
            }
        }

        Throwable rootCause = getRootCause(exception);
        for (Class deterministic : getDeterministicRootCauses()) {
            if (deterministic.equals(rootCause.getClass())) {
                return false;
            }
        }

        return true;
    }

    private Throwable getRootCause(Throwable e) {
        while (e.getCause() != null) {
            e = e.getCause();
        }
        return e;
    }

    protected int executeUpdate(Statement stmt, String sql) throws SQLException
    {
        logger.info("SQL: " + sql);
        long startTime = System.currentTimeMillis();
        int count = stmt.executeUpdate(sql);
        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
        if (count == 0) {
            logger.info(String.format("> %.2f seconds", seconds));
        } else {
            logger.info(String.format("> %.2f seconds (%,d rows)", seconds, count));
        }
        return count;
    }

    protected void commitIfNecessary(Connection con) throws SQLException
    {
        if (!con.getAutoCommit()) {
            con.commit();
        }
    }

    protected SQLException safeRollback(Connection con, SQLException cause)
    {
        try {
            if (!con.getAutoCommit()) {
                con.rollback();
            }
            return cause;
        } catch (SQLException ex) {
            if (cause != null) {
                cause.addSuppressed(ex);
                return cause;
            }
            return ex;
        }
    }
}
