package org.embulk.output.mysql;

import java.util.List;
import java.sql.Connection;
import java.sql.SQLException;

import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.TableIdentifier;

public class MySQLOutputConnection
        extends JdbcOutputConnection
{
    public MySQLOutputConnection(Connection connection, boolean autoCommit)
            throws SQLException
    {
        super(connection, null);
        connection.setAutoCommit(autoCommit);
    }

    @Override
    protected String buildPreparedMergeSql(TableIdentifier toTable, JdbcSchema toTableSchema, MergeConfig mergeConfig) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ");
        quoteTableIdentifier(sb, toTable);
        sb.append(" (");
        for (int i = 0; i < toTableSchema.getCount(); i++) {
            if(i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, toTableSchema.getColumnName(i));
        }
        sb.append(") VALUES (");
        for(int i = 0; i < toTableSchema.getCount(); i++) {
            if(i != 0) { sb.append(", "); }
            sb.append("?");
        }
        sb.append(")");
        sb.append(" ON DUPLICATE KEY UPDATE ");
        if (mergeConfig.getMergeRule().isPresent()) {
            List<String> rule = mergeConfig.getMergeRule().get();
            for (int i = 0; i < rule.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(rule.get(i));
            }
        } else {
            for (int i = 0; i < toTableSchema.getCount(); i++) {
                if(i != 0) { sb.append(", "); }
                String columnName = quoteIdentifierString(toTableSchema.getColumnName(i));
                sb.append(columnName).append(" = VALUES(").append(columnName).append(")");
            }
        }

        return sb.toString();
    }

    @Override
    protected String buildCollectMergeSql(List<TableIdentifier> fromTables, JdbcSchema schema, TableIdentifier toTable, MergeConfig mergeConfig) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ");
        quoteTableIdentifier(sb, toTable);
        sb.append(" (");
        for (int i = 0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, schema.getColumnName(i));
        }
        sb.append(") ");
        for (int i = 0; i < fromTables.size(); i++) {
            if (i != 0) { sb.append(" UNION ALL "); }
            sb.append("SELECT ");
            for (int j = 0; j < schema.getCount(); j++) {
                if (j != 0) { sb.append(", "); }
                quoteIdentifierString(sb, schema.getColumnName(j));
            }
            sb.append(" FROM ");
            quoteTableIdentifier(sb, fromTables.get(i));
        }
        sb.append(" ON DUPLICATE KEY UPDATE ");
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
                if(i != 0) { sb.append(", "); }
                String columnName = quoteIdentifierString(schema.getColumnName(i));
                sb.append(columnName).append(" = VALUES(").append(columnName).append(")");
            }
        }

        return sb.toString();
    }

    @Override
    protected String buildColumnTypeName(JdbcColumn c)
    {
        switch(c.getSimpleTypeName()) {
        case "CLOB":
            return "TEXT";
        default:
            return super.buildColumnTypeName(c);
        }
    }
}
