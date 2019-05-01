package org.embulk.output.redshift;

import java.util.Properties;
import java.sql.Driver;
import java.sql.Connection;
import java.sql.SQLException;
import org.embulk.output.jdbc.AbstractJdbcOutputConnector;
import org.embulk.output.jdbc.JdbcOutputConnection;

public class RedshiftOutputConnector
        extends AbstractJdbcOutputConnector
{
    private static final Driver driver = new org.postgresql.Driver();

    private final String url;
    private final Properties properties;
    private final String schemaName;

    public RedshiftOutputConnector(String url, Properties properties, String schemaName)
    {
        this.url = url;
        this.properties = properties;
        this.schemaName = schemaName;
    }

    @Override
    protected JdbcOutputConnection connect() throws SQLException
    {
        Connection c = driver.connect(url, properties);
        try {
            RedshiftOutputConnection con = new RedshiftOutputConnection(c, schemaName);
            c = null;
            return con;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
