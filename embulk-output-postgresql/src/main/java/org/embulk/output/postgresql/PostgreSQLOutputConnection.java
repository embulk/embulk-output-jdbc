package org.embulk.output.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.embulk.spi.Exec;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcSchema;

public class PostgreSQLOutputConnection
        extends JdbcOutputConnection
{
    public PostgreSQLOutputConnection(Connection connection, String schemaName, boolean autoCommit)
            throws SQLException
    {
        super(connection, schemaName);
        connection.setAutoCommit(autoCommit);
    }

    public String buildCopySql(String toTable, JdbcSchema toTableSchema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("COPY ");
        quoteIdentifierString(sb, toTable);
        sb.append(" (");
        for (int i=0; i < toTableSchema.getCount(); i++) {
            if(i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, toTableSchema.getColumnName(i));
        }
        sb.append(") ");
        sb.append("FROM STDIN");

        return sb.toString();
    }

    public CopyManager newCopyManager() throws SQLException
    {
        return new CopyManager((BaseConnection) connection);
    }

    @Override
    protected String convertTypeName(String typeName)
    {
        switch(typeName) {
        case "CLOB":
            return "TEXT";
        case "BLOB":
            return "BYTEA";
        default:
            return typeName;
        }
    }

    @Override
    protected String buildPreparedMergeSql(String toTable, JdbcSchema toTableSchema) throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        int size = toTableSchema.getCount();
        String table = quoteIdentifierString(toTable);
        int idx = 0;

        sb.append("WITH upsert AS (UPDATE ").append(table).append(" SET ");

        for (int i=0; i < size; i++) {
            JdbcColumn c = toTableSchema.getColumn(i);
            if (!c.isPrimaryKey()) {
                if(idx != 0) { sb.append(", "); }
                idx++;
                quoteIdentifierString(sb, toTableSchema.getColumnName(i));
                sb.append("=?");
            }
        }

        sb.append(" WHERE ");
        idx = 0;
        for(int i=0; i < size; i++) {
            JdbcColumn c = toTableSchema.getColumn(i);
            if (c.isPrimaryKey()) {
                if(idx != 0) { sb.append(" AND "); }
                idx++;
                quoteIdentifierString(sb, toTableSchema.getColumnName(i));
                sb.append("=?");
            }
        }
        sb.append(" RETURNING true as result)");

        sb.append(" INSERT INTO ").append(table).append(" (");
        for (int i=0; i < size; i++) {
            if(i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, toTableSchema.getColumnName(i));
        }
        sb.append(")");

        sb.append(" SELECT ");
        for (int i=0; i < size; i++) {
            if(i != 0) { sb.append(", "); }
            sb.append("?");
        }
        sb.append(" WHERE (SELECT result FROM upsert) is null");

        return sb.toString();
    }

}
