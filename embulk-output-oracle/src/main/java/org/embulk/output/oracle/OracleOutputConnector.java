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

    public OracleOutputConnector(String url, Properties properties)
    {
        try {
        	Class.forName("oracle.jdbc.OracleDriver");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        this.url = url;
        this.properties = properties;
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
            OracleOutputConnection con = new OracleOutputConnection(c, autoCommit);
            c = null;
            return con;
            
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
