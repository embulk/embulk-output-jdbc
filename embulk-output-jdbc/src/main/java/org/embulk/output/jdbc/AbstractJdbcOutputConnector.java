package org.embulk.output.jdbc;

import java.sql.SQLException;

public abstract class AbstractJdbcOutputConnector implements JdbcOutputConnector
{
    public JdbcOutputConnection connect(boolean autoCommit) throws SQLException
    {
        JdbcOutputConnection connection = connect();
        connection.initialize(autoCommit);
        return connection;
    }

    protected abstract JdbcOutputConnection connect() throws SQLException;
}
