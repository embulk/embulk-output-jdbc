package org.embulk.output.mysql;

import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.AbstractJdbcOutputConnector;

public class MySQLOutputConnector
        extends AbstractJdbcOutputConnector
{
    private final String url;
    private final Properties properties;

    public MySQLOutputConnector(String url, Properties properties)
    {
        this.url = url;
        this.properties = properties;
    }

    @Override
    protected JdbcOutputConnection connect() throws SQLException
    {
        Connection c = DriverManager.getConnection(url, properties);
        try {
            MySQLOutputConnection con = new MySQLOutputConnection(c);
            c = null;
            return con;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
