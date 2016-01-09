package org.embulk.output.sqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.output.jdbc.JdbcOutputConnector;

public class SQLServerOutputConnector
        implements JdbcOutputConnector
{
    private final String url;
    private final Properties properties;
    private final String schemaName;

    public SQLServerOutputConnector(String url, Properties properties, String schemaName)
    {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        this.url = url;
        this.properties = properties;
        this.schemaName = schemaName;
    }

    @Override
    public SQLServerOutputConnection connect(boolean autoCommit) throws SQLException
    {
        Connection c = DriverManager.getConnection(url, properties);
        if (c == null) {
            // driver.connect returns null when url is "jdbc:mysql://...".
            throw new SQLException("Invalid url : " + url);
        }

        try {
            SQLServerOutputConnection con = new SQLServerOutputConnection(c, schemaName, autoCommit);
            c = null;
            return con;

        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
