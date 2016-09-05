package org.embulk.output.sqlserver;

import org.embulk.output.jdbc.JdbcOutputConnector;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public class SQLServerOutputConnector
        implements JdbcOutputConnector
{
    private final String url;
    private final Properties properties;
    private final String schemaName;
    private final Driver driver;

    public SQLServerOutputConnector(Driver driver, String url, Properties properties, String schemaName)
    {
        this.driver = driver;
        this.url = url;
        this.properties = properties;
        this.schemaName = schemaName;
    }

    @Override
    public SQLServerOutputConnection connect(boolean autoCommit) throws SQLException
    {
        Connection c = driver.connect(url, properties);
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
