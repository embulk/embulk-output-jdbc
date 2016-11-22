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
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import com.google.common.base.Optional;

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
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            // ignore errors.
            // TODO here should ignore only 'table "XXX" does not exist' errors.
            SQLException ignored = safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    // Redshift does not support DROP TABLE IF EXISTS.
    // Dropping part runs DROP TABLE and ignores errors.
    @Override
    public void replaceTable(String fromTable, JdbcSchema schema, String toTable, Optional<String> additionalSql) throws SQLException
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
                SQLException ignored = safeRollback(connection, ex);
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

            if (additionalSql.isPresent()) {
                executeUpdate(stmt, additionalSql.get());
            }

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
        // Redshift does not support TEXT type.
        switch(c.getSimpleTypeName()) {
        case "CLOB":
            return "VARCHAR(65535)";
        case "TEXT":
            return "VARCHAR(65535)";
        case "BLOB":
            return "BYTEA";
        default:
            return super.buildColumnTypeName(c);
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

    @Override
    protected String buildCollectMergeSql(List<String> fromTables, JdbcSchema schema, String toTable, MergeConfig mergeConfig) throws SQLException
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
        quoteIdentifierString(sb, toTable);
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
            quoteIdentifierString(sb, fromTables.get(i));
        }
        sb.append(" ) S WHERE ");

        for (int i = 0; i < mergeKeys.size(); i++) {
            if (i != 0) { sb.append(" AND "); }
            sb.append("S.");
            quoteIdentifierString(sb, mergeKeys.get(i));
            sb.append(" = ");
            quoteIdentifierString(sb, toTable);
            sb.append(".");
            quoteIdentifierString(sb, mergeKeys.get(i));
        }
        sb.append(";");

        sb.append("INSERT INTO ");
        quoteIdentifierString(sb, toTable);
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
            quoteIdentifierString(sb, fromTables.get(i));
            sb.append(" WHERE NOT EXISTS (SELECT 1 FROM ");
            quoteIdentifierString(sb, toTable);
            sb.append(" WHERE ");

            for (int k = 0; k < mergeKeys.size(); k++) {
                if (k != 0) { sb.append(" AND "); }
                quoteIdentifierString(sb, fromTables.get(i));
                sb.append(".");
                quoteIdentifierString(sb, mergeKeys.get(k));
                sb.append(" = ");
                quoteIdentifierString(sb, toTable);
                sb.append(".");
                quoteIdentifierString(sb, mergeKeys.get(k));
            }
            sb.append(")) ");
        }
        sb.append(";");

        sb.append("END TRANSACTION;");

        return sb.toString();

    }

}