package org.embulk.output.mysql;

import java.util.List;
import java.sql.Connection;
import java.sql.SQLException;

import com.google.common.base.Optional;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.JdbcOutputConnection;

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
    protected String buildPreparedMergeSql(String toTable, JdbcSchema toTableSchema, List<String> mergeKeys, Optional<String> onDuplicateKeyUpdateSql) throws SQLException
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
        sb.append(" ON DUPLICATE KEY UPDATE ");
        if (onDuplicateKeyUpdateSql.isPresent()) {
            sb.append(onDuplicateKeyUpdateSql.get());
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
    protected String buildCollectMergeSql(List<String> fromTables, JdbcSchema schema, String toTable, List<String> mergeKeys, Optional<String> onDuplicateKeyUpdateSql) throws SQLException
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
        sb.append(" ON DUPLICATE KEY UPDATE ");
        if (onDuplicateKeyUpdateSql.isPresent()) {
            sb.append(onDuplicateKeyUpdateSql.get());
        } else {
            for (int i=0; i < schema.getCount(); i++) {
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
