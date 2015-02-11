package org.embulk.output.redshift;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.embulk.spi.Exec;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcSchema;

public class RedshiftOutputConnection
        extends JdbcOutputConnection
{
    private final Logger logger = Exec.getLogger(RedshiftOutputConnection.class);

    public RedshiftOutputConnection(Connection connection, String schemaName, boolean autoCommit)
            throws SQLException
    {
        super(connection, schemaName);
        connection.setAutoCommit(autoCommit);
    }

    // Redshift does not support DROP TABLE IF EXISTS.
    // Here runs DROP TABLE and ignores errors.
    @Override
    public void dropTableIfExists(String tableName) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            String sql = String.format("DROP TABLE IF EXISTS %s", quoteIdentifierString(tableName));
            executeUpdate(stmt, sql);
            connection.commit();
        } catch (SQLException ex) {
            // ignore errors.
            // TODO here should ignore only 'table "XXX" does not exist' errors.
            connection.rollback();
        } finally {
            stmt.close();
        }
    }

    // Redshift does not support DROP TABLE IF EXISTS.
    // Dropping part runs DROP TABLE and ignores errors.
    @Override
    public void replaceTable(String fromTable, JdbcSchema schema, String toTable) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            try {
                StringBuilder sb = new StringBuilder();
                sb.append("DROP TABLE ");
                quoteIdentifierString(sb, toTable);
                String sql = sb.toString();
                executeUpdate(stmt, sql);
            } catch (SQLException ex) {
                // ignore errors.
                // TODO here should ignore only 'table "XXX" does not exist' errors.
                // rollback or comimt is required to recover failed transaction
                connection.rollback();
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

            connection.commit();
        } catch (SQLException ex) {
            connection.rollback();
            throw ex;
        } finally {
            stmt.close();
        }
    }

    @Override
    protected String convertTypeName(String typeName)
    {
        // Redshift does not support TEXT type.
        switch(typeName) {
        case "CLOB":
            return "VARCHAR(65535)";
        case "TEXT":
            return "VARCHAR(65535)";
        case "BLOB":
            return "BYTEA";
        default:
            return typeName;
        }
    }

    public String buildCopySQLBeforeFrom(String tableName, JdbcSchema tableSchema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("COPY ");
        quoteIdentifierString(sb, tableName);
        sb.append(" (");
        for(int i=0; i < tableSchema.getCount(); i++) {
            if(i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, tableSchema.getColumnName(i));
        }
        sb.append(")");

        return sb.toString();
    }

    public void runCopy(String sql) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            stmt.executeUpdate(sql);
        } finally {
            stmt.close();
        }
    }
}
