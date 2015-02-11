package org.embulk.output.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.embulk.spi.Exec;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcColumn;

public class MySQLOutputConnection
        extends JdbcOutputConnection
{
    private final Logger logger = Exec.getLogger(MySQLOutputConnection.class);

    public MySQLOutputConnection(Connection connection, boolean autoCommit)
            throws SQLException
    {
        super(connection, null);
        connection.setAutoCommit(autoCommit);
    }

    @Override
    protected String convertTypeName(String typeName)
    {
        switch(typeName) {
        case "CLOB":
            return "TEXT";
        default:
            return typeName;
        }
    }
}
