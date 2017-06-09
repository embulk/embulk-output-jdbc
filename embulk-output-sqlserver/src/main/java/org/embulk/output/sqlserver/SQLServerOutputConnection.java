package org.embulk.output.sqlserver;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.TableIdentifier;

public class SQLServerOutputConnection
        extends JdbcOutputConnection
{
    public SQLServerOutputConnection(Connection connection, String schemaName, boolean autoCommit)
            throws SQLException
    {
        super(connection, schemaName);
        connection.setAutoCommit(autoCommit);
    }

    @Override
    protected String buildRenameTableSql(TableIdentifier fromTable, TableIdentifier toTable)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("EXEC sp_rename ");
        sb.append(quoteTableIdentifier(fromTable));
        sb.append(", ");
        sb.append(quoteTableIdentifier(toTable));
        sb.append(", 'OBJECT'");
        return sb.toString();
    }

    @Override
    protected String buildColumnTypeName(JdbcColumn c)
    {
        switch(c.getSimpleTypeName()) {
        case "BOOLEAN":
            return "BIT";
        case "CLOB":
            return "TEXT";
        case "TIMESTAMP":
            return "DATETIME2";
        default:
            return super.buildColumnTypeName(c);
        }
    }

    @Override
    protected void setSearchPath(String schema) throws SQLException
    {
        // NOP
    }

    @Override
    public void dropTableIfExists(TableIdentifier table) throws SQLException
    {
        if (tableExists(table)) {
            dropTable(table);
        }
    }

    @Override
    protected void dropTableIfExists(Statement stmt, TableIdentifier table) throws SQLException
    {
        if (tableExists(table)) {
            dropTable(stmt, table);
        }
    }

    @Override
    public void createTableIfNotExists(TableIdentifier table, JdbcSchema schema) throws SQLException
    {
        if (!tableExists(table)) {
            createTable(table, schema);
        }
    }

    private static final String[] SIMPLE_TYPE_NAMES = {
        "BIT", "FLOAT",
    };

    @Override
    protected ColumnDeclareType getColumnDeclareType(String convertedTypeName, JdbcColumn col)
    {
        if (Arrays.asList(SIMPLE_TYPE_NAMES).contains(convertedTypeName)) {
            return ColumnDeclareType.SIMPLE;
        }
        return super.getColumnDeclareType(convertedTypeName, col);
    }

    @Override
    protected String buildCollectMergeSql(List<TableIdentifier> fromTables, JdbcSchema schema, TableIdentifier toTable, MergeConfig mergeConfig) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("MERGE INTO ");
        sb.append(quoteTableIdentifier(toTable));
        sb.append(" AS T");
        sb.append(" USING (");
        for (int i = 0; i < fromTables.size(); i++) {
            if (i != 0) { sb.append(" UNION ALL "); }
            sb.append(" SELECT ");
            sb.append(buildColumns(schema, ""));
            sb.append(" FROM ");
            sb.append(quoteTableIdentifier(fromTables.get(i)));
        }
        sb.append(") AS S");
        sb.append(" ON (");
        for (int i = 0; i < mergeConfig.getMergeKeys().size(); i++) {
            if (i != 0) { sb.append(" AND "); }
            String mergeKey = quoteIdentifierString(mergeConfig.getMergeKeys().get(i));
            sb.append("T.");
            sb.append(mergeKey);
            sb.append(" = S.");
            sb.append(mergeKey);
        }
        sb.append(")");
        sb.append(" WHEN MATCHED THEN");
        sb.append(" UPDATE SET ");
        if (mergeConfig.getMergeRule().isPresent()) {
            for (int i = 0; i < mergeConfig.getMergeRule().get().size(); i++) {
                if (i != 0) { sb.append(", "); }
                sb.append(mergeConfig.getMergeRule().get().get(i));
            }
        } else {
            for (int i = 0; i < schema.getCount(); i++) {
                if (i != 0) { sb.append(", "); }
                String column = quoteIdentifierString(schema.getColumnName(i));
                sb.append(column);
                sb.append(" = S.");
                sb.append(column);
            }
        }
        sb.append(" WHEN NOT MATCHED THEN");
        sb.append(" INSERT (");
        sb.append(buildColumns(schema, ""));
        sb.append(") VALUES (");
        sb.append(buildColumns(schema, "S."));
        sb.append(");");

        return sb.toString();
    }

    private String buildColumns(JdbcSchema schema, String prefix)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            sb.append(prefix);
            sb.append(quoteIdentifierString(schema.getColumnName(i)));
        }
        return sb.toString();
    }
}
