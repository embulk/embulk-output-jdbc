package org.embulk.output.jdbc;

import java.sql.SQLException;

import com.google.common.base.Optional;

public abstract class AbstractJdbcOutputConnector implements JdbcOutputConnector
{
    private final Optional<TransactionIsolation> transactionIsolation;

    public AbstractJdbcOutputConnector(Optional<TransactionIsolation> transactionIsolation)
    {
        this.transactionIsolation = transactionIsolation;
    }

    public JdbcOutputConnection connect(boolean autoCommit) throws SQLException
    {
        JdbcOutputConnection connection = connect();
        connection.initialize(autoCommit, transactionIsolation);
        return connection;
    }

    protected abstract JdbcOutputConnection connect() throws SQLException;
}
