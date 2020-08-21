package org.embulk.output.redshift;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedshiftOutputConnection
        extends JdbcOutputConnection
{
    private static final Logger logger = LoggerFactory.getLogger(RedshiftOutputConnection.class);

    public RedshiftOutputConnection(Connection connection, String schemaName)
            throws SQLException
    {
        super(connection, schemaName);
    }

    // ALTER TABLE cannot change the schema of a table
    //
    // Standard JDBC:
    //     ALTER TABLE "public"."source" RENAME TO "public"."target"
    // Redshift:
    //     ALTER TABLE "public"."source" RENAME TO "target"
    @Override
    protected String buildRenameTableSql(TableIdentifier fromTable, TableIdentifier toTable)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        quoteTableIdentifier(sb, fromTable);
        sb.append(" RENAME TO ");
        quoteIdentifierString(sb, toTable.getTableName());
        return sb.toString();
    }

    @Override
    protected String buildColumnTypeName(JdbcColumn c)
    {
        switch(c.getSimpleTypeName()) {
        case "CLOB":
            return "VARCHAR(65535)";
        case "BLOB":
            return "BYTEA";
        default:
            return super.buildColumnTypeName(c);
        }
    }

    public String buildCopySQLBeforeFrom(TableIdentifier table, JdbcSchema tableSchema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("COPY ");
        quoteTableIdentifier(sb, table);
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

    @Override
    protected String buildCollectMergeSql(List<TableIdentifier> fromTables, JdbcSchema schema, TableIdentifier toTable, MergeConfig mergeConfig) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        List<String> mergeKeys = mergeConfig.getMergeKeys();

        List<String> updateKeys = new ArrayList<String>();
        for (int i = 0; i < schema.getCount(); i++) {
            String updateKey = schema.getColumnName(i);
            if (!mergeKeys.contains(updateKey)) {
                updateKeys.add(updateKey);
            }
        }

        sb.append("BEGIN TRANSACTION;");

        sb.append("UPDATE ");
        quoteTableIdentifier(sb, toTable);
        sb.append(" SET ");
        for (int i = 0; i < updateKeys.size(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, updateKeys.get(i).toString());
            sb.append(" = ");
            sb.append("S.");
            quoteIdentifierString(sb, updateKeys.get(i).toString());
        }
        sb.append(" FROM ( ");
        for (int i = 0; i < fromTables.size(); i++) {
            if (i != 0) { sb.append(" UNION ALL "); }
            sb.append(" SELECT ");
            for (int j = 0; j < schema.getCount(); j++) {
                if (j != 0) { sb.append(", "); }
                quoteIdentifierString(sb, schema.getColumnName(j));
            }
            sb.append(" FROM ");
            quoteTableIdentifier(sb, fromTables.get(i));
        }
        sb.append(" ) S WHERE ");

        for (int i = 0; i < mergeKeys.size(); i++) {
            if (i != 0) { sb.append(" AND "); }
            sb.append("S.");
            quoteIdentifierString(sb, mergeKeys.get(i));
            sb.append(" = ");
            quoteTableIdentifier(sb, toTable);
            sb.append(".");
            quoteIdentifierString(sb, mergeKeys.get(i));
        }
        sb.append(";");

        sb.append("INSERT INTO ");
        quoteTableIdentifier(sb, toTable);
        sb.append(" (");
        for (int i = 0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, schema.getColumnName(i));
        }
        sb.append(") (");
        for (int i = 0; i < fromTables.size(); i++) {
            if (i != 0) { sb.append(" UNION ALL ("); }
            sb.append("SELECT ");
            for (int j = 0; j < schema.getCount(); j++) {
                if (j != 0) { sb.append(", "); }
                quoteIdentifierString(sb, schema.getColumnName(j));
            }
            sb.append(" FROM ");
            quoteTableIdentifier(sb, fromTables.get(i));

            sb.append(" WHERE (");
            for (int k = 0; k < mergeKeys.size(); k++) {
                if (k != 0) { sb.append(", "); }
                quoteTableIdentifier(sb, fromTables.get(i));
                sb.append(".");
                quoteIdentifierString(sb, mergeKeys.get(k));
            }
            sb.append(") NOT IN (SELECT ");
            for (int k = 0; k < mergeKeys.size(); k++) {
                if (k != 0) { sb.append(", "); }
                quoteTableIdentifier(sb, toTable);
                sb.append(".");
                quoteIdentifierString(sb, mergeKeys.get(k));
            }
            sb.append(" FROM ");
            quoteTableIdentifier(sb, toTable);
            sb.append(")) ");
        }
        sb.append(";");

        sb.append("END TRANSACTION;");

        return sb.toString();

    }

}
