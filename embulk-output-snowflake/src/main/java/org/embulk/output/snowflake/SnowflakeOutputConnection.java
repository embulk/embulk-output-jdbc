package org.embulk.output.snowflake;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcUtils;
import org.embulk.output.jdbc.TableIdentifier;

public class SnowflakeOutputConnection
        extends JdbcOutputConnection
{
    public SnowflakeOutputConnection(Connection connection, String schemaName)
        throws SQLException
    {
        super(connection, schemaName);
    }

    @Override
    public boolean tableExists(TableIdentifier table) throws SQLException
    {
        String schemaName = JdbcUtils.escapeSearchString(table.getSchemaName(), connection.getMetaData().getSearchStringEscape());
        String database = connection.getCatalog();
        try (ResultSet rs = connection.getMetaData().getTables(database, schemaName, table.getTableName(), null)) {
            return rs.next();
        }
    }

    @Override
    public boolean tableExists(String tableName) throws SQLException
    {
        return tableExists(new TableIdentifier(connection.getCatalog(), schemaName, tableName));
    }

    @Override
    protected void setSearchPath(String schema) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            String sql = "USE SCHEMA " + quoteIdentifierString(schema);
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } finally {
            stmt.close();
        }
    }


}
