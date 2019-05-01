package org.embulk.output.db2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcOutputConnector;

public class DB2OutputConnector
        implements JdbcOutputConnector
{
    private final String url;
    private final Properties properties;
    private final String schemaName;

    public DB2OutputConnector(String url, Properties properties, String schemaName)
    {
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        this.url = url;
        this.properties = properties;
        this.schemaName = schemaName;
    }

    @Override
    public JdbcOutputConnection connect(boolean autoCommit) throws SQLException
    {
        Connection c = DriverManager.getConnection(url, properties);
        if (c == null) {
            // driver.connect returns null when url is "jdbc:mysql://...".
            throw new SQLException("Invalid url : " + url);
        }

        try {
            DB2OutputConnection con = new DB2OutputConnection(c, schemaName, autoCommit);
            c = null;
            return con;

        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
