package org.embulk.output.postgresql;

import java.util.List;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.common.base.Optional;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
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
            if (i != 0) { sb.append(", "); }
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
    protected String buildCollectMergeSql(List<String> fromTables, JdbcSchema schema, String toTable, List<String> mergeKeys, Optional<String> onDuplicateKeyUpdateSql) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("WITH updated AS (");
        sb.append("UPDATE ");
        quoteIdentifierString(sb, toTable);
        sb.append(" SET ");
        for (int i=0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, schema.getColumnName(i));
            sb.append(" = S.");
            quoteIdentifierString(sb, schema.getColumnName(i));
        }
        sb.append(" FROM (");
        for (int i=0; i < fromTables.size(); i++) {
            if (i != 0) { sb.append(" UNION ALL "); }
            sb.append("SELECT ");
            for(int j=0; j < schema.getCount(); j++) {
                if (j != 0) { sb.append(", "); }
                quoteIdentifierString(sb, schema.getColumnName(j));
            }
            sb.append(" FROM ");
            quoteIdentifierString(sb, fromTables.get(i));
        }
        sb.append(") S");
        sb.append(" WHERE ");
        for (int i=0; i < mergeKeys.size(); i++) {
            if (i != 0) { sb.append(" AND "); }
            quoteIdentifierString(sb, toTable);
            sb.append(".");
            quoteIdentifierString(sb, mergeKeys.get(i));
            sb.append(" = ");
            sb.append("S.");
            quoteIdentifierString(sb, mergeKeys.get(i));
        }
        sb.append(" RETURNING ");
        for (int i=0; i < mergeKeys.size(); i++) {
            if (i != 0) { sb.append(", "); }
            sb.append("S.");
            quoteIdentifierString(sb, mergeKeys.get(i));
        }
        sb.append(") ");

        sb.append("INSERT INTO ");
        quoteIdentifierString(sb, toTable);
        sb.append(" (");
        for (int i=0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, schema.getColumnName(i));
        }
        sb.append(") ");
        sb.append("SELECT DISTINCT ON (");
        for (int i=0; i < mergeKeys.size(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, mergeKeys.get(i));
        }
        sb.append(") * FROM (");
        for (int i=0; i < fromTables.size(); i++) {
            if (i != 0) { sb.append(" UNION ALL "); }
            sb.append("SELECT ");
            for(int j=0; j < schema.getCount(); j++) {
                if (j != 0) { sb.append(", "); }
                quoteIdentifierString(sb, schema.getColumnName(j));
            }
            sb.append(" FROM ");
            quoteIdentifierString(sb, fromTables.get(i));
        }
        sb.append(") S ");
        sb.append("WHERE NOT EXISTS (");
        sb.append("SELECT 1 FROM updated WHERE ");
        for (int i=0; i < mergeKeys.size(); i++) {
            if (i != 0) { sb.append(" AND "); }
            sb.append("S.");
            quoteIdentifierString(sb, mergeKeys.get(i));
            sb.append(" = ");
            sb.append("updated.");
            quoteIdentifierString(sb, mergeKeys.get(i));
        }
        sb.append(") ");

        return sb.toString();
    }

    protected void collectReplaceView(List<String> fromTables, JdbcSchema schema, String toTable) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            String sql = buildCollectInsertSql(fromTables, schema, toTable);
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    @Override
    protected String buildColumnTypeName(JdbcColumn c)
    {
        switch(c.getSimpleTypeName()) {
        case "CLOB":
            return "TEXT";
        case "BLOB":
            return "BYTEA";
        default:
            return super.buildColumnTypeName(c);
        }
    }
}
