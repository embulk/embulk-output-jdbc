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
        connection.close();
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

    public boolean tableExists(String tableName) throws SQLException
    {
        try (ResultSet rs = connection.getMetaData().getTables(null, schemaName, tableName, null)) {
            return rs.next();
        }
    }

    public void dropTableIfExists(String tableName) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            dropTableIfExists(stmt, tableName);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected void dropTableIfExists(Statement stmt, String tableName) throws SQLException
    {
        String sql = String.format("DROP TABLE IF EXISTS %s", quoteIdentifierString(tableName));
        executeUpdate(stmt, sql);
    }

    public void dropTable(String tableName) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            dropTable(stmt, tableName);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected void dropTable(Statement stmt, String tableName) throws SQLException
    {
        String sql = String.format("DROP TABLE %s", quoteIdentifierString(tableName));
        executeUpdate(stmt, sql);
    }

    public void createTableIfNotExists(String tableName, JdbcSchema schema) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            String sql = buildCreateTableIfNotExistsSql(tableName, schema);
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected String buildCreateTableIfNotExistsSql(String name, JdbcSchema schema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE IF NOT EXISTS ");
        quoteIdentifierString(sb, name);
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

    protected String buildRenameTableSql(String fromTable, String toTable)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        quoteIdentifierString(sb, fromTable);
        sb.append(" RENAME TO ");
        quoteIdentifierString(sb, toTable);
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

    public PreparedStatement prepareBatchInsertStatement(String toTable, JdbcSchema toTableSchema, Optional<List<String>> mergeKeys, Optional<String> onDuplicateKeyUpdateSql) throws SQLException
    {
        String sql;
        if (mergeKeys.isPresent()) {
            sql = buildPreparedMergeSql(toTable, toTableSchema, mergeKeys.get(), onDuplicateKeyUpdateSql);
        } else {
            sql = buildPreparedInsertSql(toTable, toTableSchema);
        }
        logger.info("Prepared SQL: {}", sql);
        return connection.prepareStatement(sql);
    }

    protected String buildPreparedInsertSql(String toTable, JdbcSchema toTableSchema) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ");
        quoteIdentifierString(sb, toTable);

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

    protected String buildPreparedMergeSql(String toTable, JdbcSchema toTableSchema, List<String> mergeKeys, Optional<String> onDuplicateKeyUpdateSql) throws SQLException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    protected void collectInsert(List<String> fromTables, JdbcSchema schema, String toTable,
            boolean truncateDestinationFirst) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            if (truncateDestinationFirst) {
                String sql = buildTruncateSql(toTable);
                executeUpdate(stmt, sql);
            }
            String sql = buildCollectInsertSql(fromTables, schema, toTable);
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected String buildTruncateSql(String table)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("DELETE FROM ");
        quoteIdentifierString(sb, table);

        return sb.toString();
    }

    protected String buildCollectInsertSql(List<String> fromTables, JdbcSchema schema, String toTable)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ");
        quoteIdentifierString(sb, toTable);
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
            quoteIdentifierString(sb, fromTables.get(i));
        }

        return sb.toString();
    }

    protected void collectMerge(List<String> fromTables, JdbcSchema schema, String toTable, List<String> mergeKeys, Optional<String> onDuplicateKeyUpdateSql) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            String sql = buildCollectMergeSql(fromTables, schema, toTable, mergeKeys, onDuplicateKeyUpdateSql);
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected String buildCollectMergeSql(List<String> fromTables, JdbcSchema schema, String toTable, List<String> mergeKeys, Optional<String> onDuplicateKeyUpdateSql) throws SQLException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    public void replaceTable(String fromTable, JdbcSchema schema, String toTable) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            dropTableIfExists(stmt, toTable);

            executeUpdate(stmt, buildRenameTableSql(fromTable, toTable));

            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
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
