package org.embulk.output.oracle;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcSchema;

public class OracleOutputConnection
        extends JdbcOutputConnection
{
    public OracleOutputConnection(Connection connection, boolean autoCommit)
            throws SQLException
    {
        super(connection, getSchema(connection));
        connection.setAutoCommit(autoCommit);
    }

    @Override
    protected String convertTypeName(String typeName)
    {
        switch(typeName) {
        case "BIGINT":
            return "NUMBER(19,0)";
        default:
            return typeName;
        }
    }

    @Override
    protected void setSearchPath(String schema) throws SQLException {
        // NOP
    }


    @Override
    public void dropTableIfExists(String tableName) throws SQLException
    {
        if (tableExists(tableName)) {
            dropTable(tableName);
        }
    }

    @Override
    protected void dropTableIfExists(Statement stmt, String tableName) throws SQLException {
        if (tableExists(tableName)) {
            dropTable(stmt, tableName);
        }
    }

    @Override
    public void createTableIfNotExists(String tableName, JdbcSchema schema) throws SQLException
    {
        if (!tableExists(tableName)) {
            createTable(tableName, schema);
        }
    }

    private static String getSchema(Connection connection) throws SQLException
    {
        // Because old Oracle JDBC drivers don't support Connection#getSchema method.
        String sql = "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                if (resultSet.next()) {
                    return resultSet.getString(1);
                }
                throw new SQLException(String.format("Cannot get schema becase \"%s\" didn't return any value.", sql));
            }
        }
    }
}
