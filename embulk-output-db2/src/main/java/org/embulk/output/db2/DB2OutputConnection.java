package org.embulk.output.db2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcSchema;

import static java.util.Locale.ENGLISH;

public class DB2OutputConnection
        extends JdbcOutputConnection
{
    public DB2OutputConnection(Connection connection, String schemaName, boolean autoCommit)
            throws SQLException
    {
        super(connection, schemaName);
        connection.setAutoCommit(autoCommit);
    }

    @Override
    protected String buildRenameTableSql(String fromTable, String toTable)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("RENAME TABLE ");
        sb.append(quoteIdentifierString(fromTable));
        sb.append(" TO ");
        sb.append(quoteIdentifierString(toTable));
        return sb.toString();
    }

    @Override
    protected void setSearchPath(String schema) throws SQLException
    {
        // NOP
    }

    @Override
    public void dropTableIfExists(String tableName) throws SQLException
    {
        if (tableExists(tableName)) {
            dropTable(tableName);
        }
    }

    @Override
    protected void dropTableIfExists(Statement stmt, String tableName) throws SQLException
    {
        if (tableExists(tableName)) {
            dropTable(stmt, tableName);
        }
    }

    @Override
    public void createTableIfNotExists(String tableName, JdbcSchema schema) throws SQLException
    {
        if (!tableExists(tableName)) {
            createTable(tableName, schema);
        }
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
        sb.append(buildCreateTableSchemaSql(schema));
        return sb.toString();
    }

    @Override
    protected String buildColumnTypeName(JdbcColumn c)
    {
        switch(c.getSimpleTypeName()) {
        case "BOOLEAN":
            return "SMALLINT";

        // NCHAR/NVARCHAR/NCLOB are synonyms for CHAR/VARCHAR/CLOB/GRAPHIC/VARGRAPHIC
        case "CHAR":
        case "VARCHAR":
        case "CLOB":
            String charUnit;
            if (c.getSizeTypeParameter() == c.getDataLength()) {
                charUnit = "OCTETS";
            } else if (c.getSizeTypeParameter() * 2 == c.getDataLength()) {
                charUnit = "CODEUNITS16";
            } else if (c.getSizeTypeParameter() * 4 == c.getDataLength()) {
                charUnit = "CODEUNITS32";
            } else {
                throw new IllegalArgumentException(String.format(ENGLISH, "Column %s has unexpected size %d and length %d.",
                        c.getName(), c.getSizeTypeParameter(), c.getDataLength()));
            }
            return String.format(ENGLISH, "%s(%d %s)", c.getSimpleTypeName(), c.getSizeTypeParameter(), charUnit);

        case "GRAPHIC":
        case "VARGRAPHIC":
            String graphicUnit;
            if (c.getSizeTypeParameter() == c.getDataLength()) {
                graphicUnit = "CODEUNITS16";
            } else if (c.getSizeTypeParameter() * 2 == c.getDataLength()) {
                graphicUnit = "CODEUNITS32";
            } else {
                throw new IllegalArgumentException(String.format(ENGLISH, "Column %s has unexpected size %d and length %d.",
                        c.getName(), c.getSizeTypeParameter(), c.getDataLength()));
            }
            return String.format(ENGLISH, "%s(%d %s)", c.getSimpleTypeName(), c.getSizeTypeParameter(), graphicUnit);

        default:
            return super.buildColumnTypeName(c);
        }
    }

    @Override
    public void close() throws SQLException
    {
        if (!connection.isClosed()) {
            // DB2 JDBC Driver requires explicit commit/rollback before closing connection.
            connection.rollback();
        }

        super.close();
    }
}
