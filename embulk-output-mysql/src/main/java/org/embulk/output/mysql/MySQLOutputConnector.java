package org.embulk.output.mysql;

import java.util.Properties;
import java.sql.Driver;
import java.sql.Connection;
import java.sql.SQLException;
import org.embulk.output.jdbc.JdbcOutputConnector;

public class MySQLOutputConnector
        implements JdbcOutputConnector
{
    private final Driver driver;
    private final String url;
    private final Properties properties;

    public MySQLOutputConnector(String url, Properties properties)
    {
        try {
            this.driver = new com.mysql.jdbc.Driver();  // new com.mysql.jdbc.Driver throws SQLException
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        this.url = url;
        this.properties = properties;
    }

    @Override
    public MySQLOutputConnection connect(boolean autoCommit) throws SQLException
    {
        Connection c = driver.connect(url, properties);
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
