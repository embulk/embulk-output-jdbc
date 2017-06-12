package org.embulk.output.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.TableIdentifier;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class PostgreSQLOutputConnection
        extends JdbcOutputConnection
{
    private static final int MAX_NUMERIC_PRECISION = 1000;

    public PostgreSQLOutputConnection(Connection connection, String schemaName, boolean autoCommit)
            throws SQLException
    {
        super(connection, schemaName);
        connection.setAutoCommit(autoCommit);
    }

    public String buildCopySql(TableIdentifier toTable, JdbcSchema toTableSchema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("COPY ");
        quoteTableIdentifier(sb, toTable);
        sb.append(" (");
        for (int i = 0; i < toTableSchema.getCount(); i++) {
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
    protected String buildPreparedMergeSql(TableIdentifier toTable, JdbcSchema schema, MergeConfig mergeConfig) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("WITH S AS (");
        sb.append("SELECT ");
        for (int i = 0; i < schema.getCount(); i++) {
            JdbcColumn column = schema.getColumn(i);
            if (i != 0) { sb.append(", "); }
            sb.append("CAST(? AS " + column.getSimpleTypeName() + ") AS ");
            quoteIdentifierString(sb, column.getName());
        }
        sb.append("),");
        sb.append("updated AS (");
        sb.append("UPDATE ");
        quoteTableIdentifier(sb, toTable);
        sb.append(" SET ");
        if (mergeConfig.getMergeRule().isPresent()) {
            List<String> rule = mergeConfig.getMergeRule().get();
            for (int i = 0; i < rule.size(); i++) {
                if (i != 0) { sb.append(", "); }
                sb.append(rule.get(i));
            }
        } else {
            for (int i = 0; i < schema.getCount(); i++) {
                if (i != 0) { sb.append(", "); }
                quoteIdentifierString(sb, schema.getColumnName(i));
                sb.append(" = S.");
                quoteIdentifierString(sb, schema.getColumnName(i));
            }
        }
        sb.append(" FROM S");
        sb.append(" WHERE ");
        List<String> mergeKeys = mergeConfig.getMergeKeys();
        for (int i = 0; i < mergeKeys.size(); i++) {
            if (i != 0) { sb.append(" AND "); }
            quoteTableIdentifier(sb, toTable);
            sb.append(".");
            quoteIdentifierString(sb, mergeKeys.get(i));
            sb.append(" = ");
            sb.append("S.");
            quoteIdentifierString(sb, mergeKeys.get(i));
        }
        sb.append(" RETURNING ");
        for (int i = 0; i < mergeKeys.size(); i++) {
            if (i != 0) { sb.append(", "); }
            sb.append("S.");
            quoteIdentifierString(sb, mergeKeys.get(i));
        }
        sb.append(") ");

        sb.append("INSERT INTO ");
        quoteTableIdentifier(sb, toTable);
        sb.append(" (");
        for (int i = 0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, schema.getColumnName(i));
        }
        sb.append(") ");
        sb.append("SELECT ");
        for(int i = 0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, schema.getColumnName(i));
        }
        sb.append(" FROM S ");
        sb.append("WHERE NOT EXISTS (");
        sb.append("SELECT 1 FROM updated WHERE ");
        for (int i = 0; i < mergeKeys.size(); i++) {
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

    @Override
    protected String buildCollectMergeSql(List<TableIdentifier> fromTables, JdbcSchema schema, TableIdentifier toTable, MergeConfig mergeConfig) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("WITH updated AS (");
        sb.append("UPDATE ");
        quoteTableIdentifier(sb, toTable);
        sb.append(" SET ");
        if (mergeConfig.getMergeRule().isPresent()) {
            List<String> rule = mergeConfig.getMergeRule().get();
            for (int i = 0; i < rule.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(rule.get(i));
            }
        } else {
            for (int i = 0; i < schema.getCount(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                quoteIdentifierString(sb, schema.getColumnName(i));
                sb.append(" = S.");
                quoteIdentifierString(sb, schema.getColumnName(i));
            }
        }
        sb.append(" FROM (");
        for (int i = 0; i < fromTables.size(); i++) {
            if (i != 0) { sb.append(" UNION ALL "); }
            sb.append("SELECT ");
            for(int j = 0; j < schema.getCount(); j++) {
                if (j != 0) { sb.append(", "); }
                quoteIdentifierString(sb, schema.getColumnName(j));
            }
            sb.append(" FROM ");
            quoteTableIdentifier(sb, fromTables.get(i));
        }
        sb.append(") S");
        sb.append(" WHERE ");
        List<String> mergeKeys = mergeConfig.getMergeKeys();
        for (int i = 0; i < mergeKeys.size(); i++) {
            if (i != 0) { sb.append(" AND "); }
            quoteTableIdentifier(sb, toTable);
            sb.append(".");
            quoteIdentifierString(sb, mergeKeys.get(i));
            sb.append(" = ");
            sb.append("S.");
            quoteIdentifierString(sb, mergeKeys.get(i));
        }
        sb.append(" RETURNING ");
        for (int i = 0; i < mergeKeys.size(); i++) {
            if (i != 0) { sb.append(", "); }
            sb.append("S.");
            quoteIdentifierString(sb, mergeKeys.get(i));
        }
        sb.append(") ");

        sb.append("INSERT INTO ");
        quoteTableIdentifier(sb, toTable);
        sb.append(" (");
        for (int i = 0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, schema.getColumnName(i));
        }
        sb.append(") ");
        sb.append("SELECT DISTINCT ON (");
        for (int i = 0; i < mergeKeys.size(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, mergeKeys.get(i));
        }
        sb.append(") * FROM (");
        for (int i = 0; i < fromTables.size(); i++) {
            if (i != 0) { sb.append(" UNION ALL "); }
            sb.append("SELECT ");
            for(int j = 0; j < schema.getCount(); j++) {
                if (j != 0) { sb.append(", "); }
                quoteIdentifierString(sb, schema.getColumnName(j));
            }
            sb.append(" FROM ");
            quoteTableIdentifier(sb, fromTables.get(i));
        }
        sb.append(") S ");
        sb.append("WHERE NOT EXISTS (");
        sb.append("SELECT 1 FROM updated WHERE ");
        for (int i = 0; i < mergeKeys.size(); i++) {
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

    protected void collectReplaceView(List<TableIdentifier> fromTables, JdbcSchema schema, TableIdentifier toTable) throws SQLException
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
        case "VARCHAR":
            if (c.getDataLength() == Integer.MAX_VALUE) {
                // getDataLength for varchar without length specifier will return 2147483647 .
                // but cannot create column of varchar(2147483647) .
                return "VARCHAR";
            }
            break;
        case "NUMERIC": // only "NUMERIC" because PostgreSQL JDBC driver will return also "NUMERIC" for the type name of decimal.
            if (c.getDataLength() > MAX_NUMERIC_PRECISION) {
                // getDataLength for numeric without precision will return 131089 .
                // but cannot create column of numeric(131089) .
                return "NUMERIC";
            }
            break;
        default:
            break;
        }
        return super.buildColumnTypeName(c);
    }
}
