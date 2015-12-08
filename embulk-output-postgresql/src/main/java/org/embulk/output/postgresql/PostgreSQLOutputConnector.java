package org.embulk.output.postgresql;

import java.util.Properties;
import java.sql.Driver;
import java.sql.Connection;
import java.sql.SQLException;
import org.embulk.output.jdbc.JdbcOutputConnector;

public class PostgreSQLOutputConnector
        implements JdbcOutputConnector
{
    private static final Driver driver = new org.postgresql.Driver();

    private final String url;
    private final Properties properties;
    private final String schemaName;

    public PostgreSQLOutputConnector(String url, Properties properties, String schemaName)
    {
        this.url = url;
        this.properties = properties;
        this.schemaName = schemaName;
    }

    @Override
    public PostgreSQLOutputConnection connect(boolean autoCommit) throws SQLException
    {
        Connection c = driver.connect(url, properties);
        try {
            PostgreSQLOutputConnection con = new PostgreSQLOutputConnection(c, schemaName, autoCommit);
            c = null;
            return con;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
