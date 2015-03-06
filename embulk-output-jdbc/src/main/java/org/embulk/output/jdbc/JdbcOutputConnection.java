package org.embulk.output.jdbc;

import java.util.List;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
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

    public void dropTableIfExists(String tableName) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            String sql = String.format("DROP TABLE IF EXISTS %s", quoteIdentifierString(tableName));
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
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
        sb.append(buildColumnsOfCreateTableSql(schema));
        return sb.toString();
    }

    public void createTable(String tableName, JdbcSchema schema) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            String sql = buildCreateTableSql(tableName, schema);
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected String buildCreateTableSql(String name, JdbcSchema schema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ");
        quoteIdentifierString(sb, name);
        sb.append(buildColumnsOfCreateTableSql(schema));
        return sb.toString();
    }
    
    private String buildColumnsOfCreateTableSql(JdbcSchema schema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(" (");
        boolean first = true;
        for (JdbcColumn c : schema.getColumns()) {
            if (first) { first = false; }
            else { sb.append(", "); }
            quoteIdentifierString(sb, c.getName());
            sb.append(" ");
            String typeName = getCreateTableTypeName(c);
            sb.append(typeName);
        }
        sb.append(")");

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
        String convertedTypeName = convertTypeName(c.getTypeName());
        switch (getColumnDeclareType(convertedTypeName, c)) {
        case SIZE:
            return String.format("%s(%d)", convertedTypeName, c.getSizeTypeParameter());
        case SIZE_AND_SCALE:
            if (c.getScaleTypeParameter() < 0) {
                return String.format("%s(%d,0)", convertedTypeName, c.getSizeTypeParameter());
            } else {
                return String.format("%s(%d,%d)", convertedTypeName, c.getSizeTypeParameter(), c.getScaleTypeParameter());
            }
        case SIZE_AND_OPTIONAL_SCALE:
            if (c.getScaleTypeParameter() < 0) {
                return String.format("%s(%d)", convertedTypeName, c.getSizeTypeParameter());
            } else {
                return String.format("%s(%d,%d)", convertedTypeName, c.getSizeTypeParameter(), c.getScaleTypeParameter());
            }
        default:  // SIMPLE
            return convertedTypeName;
        }
    }

    // hook point for subclasses
    protected String convertTypeName(String typeName)
    {
        return typeName;
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

    protected String buildInsertTableSql(String fromTable, JdbcSchema fromTableSchema, String toTable)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ");
        quoteIdentifierString(sb, toTable);
        sb.append(" (");
        boolean first = true;
        for (JdbcColumn c : fromTableSchema.getColumns()) {
            if (first) { first = false; }
            else { sb.append(", "); }
            quoteIdentifierString(sb, c.getName());
        }
        sb.append(") ");
        sb.append("SELECT ");
        for (JdbcColumn c : fromTableSchema.getColumns()) {
            if (first) { first = false; }
            else { sb.append(", "); }
            quoteIdentifierString(sb, c.getName());
        }
        sb.append(" FROM ");
        quoteIdentifierString(sb, fromTable);

        return sb.toString();
    }

    protected String buildTruncateSql(String table)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("DELETE FROM ");
        quoteIdentifierString(sb, table);

        return sb.toString();
    }

    protected void insertTable(String fromTable, JdbcSchema fromTableSchema, String toTable,
            boolean truncateDestinationFirst) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            if(truncateDestinationFirst) {
                String sql = buildTruncateSql(toTable);
                executeUpdate(stmt, sql);
            }
            String sql = buildInsertTableSql(fromTable, fromTableSchema, toTable);
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            connection.rollback();
            throw ex;
        } finally {
            stmt.close();
        }
    }

    public PreparedStatement prepareInsertSql(String toTable, JdbcSchema toTableSchema) throws SQLException
    {
        String insertSql = buildPrepareInsertSql(toTable, toTableSchema);
        logger.info("Prepared SQL: {}", insertSql);
        return connection.prepareStatement(insertSql);
    }

    protected String buildPrepareInsertSql(String toTable, JdbcSchema toTableSchema) throws SQLException
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

    // TODO
    //protected void gatherInsertTables(List<String> fromTables, JdbcSchema fromTableSchema, String toTable,
    //        boolean truncateDestinationFirst) throws SQLException
    //{
    //    Statement stmt = connection.createStatement();
    //    try {
    //        if(truncateDestinationFirst) {
    //            String sql = buildTruncateSql(toTable);
    //            executeUpdate(stmt, sql);
    //        }
    //        String sql = buildGatherInsertTables(fromTable, fromTableSchema, toTable);
    //        executeUpdate(stmt, sql);
    //        commitIfNecessary(connection);
    //    } catch (SQLException ex) {
    //        throw safeRollback(connection, ex);
    //    } finally {
    //        stmt.close();
    //    }
    //}

    public void replaceTable(String fromTable, JdbcSchema schema, String toTable) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            {
                StringBuilder sb = new StringBuilder();
                sb.append("DROP TABLE IF EXISTS ");
                quoteIdentifierString(sb, toTable);
                String sql = sb.toString();
                executeUpdate(stmt, sql);
            }

            {
                StringBuilder sb = new StringBuilder();
                sb.append("ALTER TABLE ");
                quoteIdentifierString(sb, fromTable);
                sb.append(" RENAME TO ");
                quoteIdentifierString(sb, toTable);
                String sql = sb.toString();
                executeUpdate(stmt, sql);
            }

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
