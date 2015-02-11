package org.embulk.output.jdbc;

import java.sql.SQLException;

public interface JdbcOutputConnector
{
    public JdbcOutputConnection connect(boolean autoCommit) throws SQLException;
}
