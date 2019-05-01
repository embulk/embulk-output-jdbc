package org.embulk.output.mysql;

import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcOutputConnector;

public class MySQLOutputConnector
        implements JdbcOutputConnector
{
    private final String url;
    private final Properties properties;

    public MySQLOutputConnector(String url, Properties properties)
    {
        this.url = url;
        this.properties = properties;
    }

    @Override
    public JdbcOutputConnection connect(boolean autoCommit) throws SQLException
    {
        Connection c = DriverManager.getConnection(url, properties);
        try {
            MySQLOutputConnection con = new MySQLOutputConnection(c, autoCommit);
            c = null;
            return con;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
