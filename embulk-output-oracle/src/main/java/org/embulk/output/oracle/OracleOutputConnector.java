package org.embulk.output.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.output.jdbc.JdbcOutputConnector;

public class OracleOutputConnector
        implements JdbcOutputConnector
{
    private final String url;
    private final Properties properties;
    private final boolean direct;

    public OracleOutputConnector(String url, Properties properties, boolean direct)
    {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        this.url = url;
        this.properties = properties;
        this.direct = direct;
    }

    @Override
    public OracleOutputConnection connect(boolean autoCommit) throws SQLException
    {
        Connection c = DriverManager.getConnection(url, properties);
        if (c == null) {
            // driver.connect returns null when url is "jdbc:mysql://...".
            throw new SQLException("Invalid url : " + url);
        }

        try {
            OracleOutputConnection con = new OracleOutputConnection(c, autoCommit, direct);
            c = null;
            return con;

        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
