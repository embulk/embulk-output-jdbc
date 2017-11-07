package org.embulk.output.redshift;

import java.util.Properties;
import java.sql.Driver;
import java.sql.Connection;
import java.sql.SQLException;

import com.google.common.base.Throwables;
import org.embulk.output.jdbc.JdbcOutputConnector;


public class RedshiftOutputConnector
        implements JdbcOutputConnector
{
    private final Driver driver;

    private final String url;
    private final Properties properties;
    private final String schemaName;

    public RedshiftOutputConnector(String url, Properties properties, String schemaName, String driverClass) {
        this.url = url;
        this.properties = properties;
        this.schemaName = schemaName;
        try {
            this.driver = (Driver) Class.forName(driverClass).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e){
            throw Throwables.propagate(e);
        }

    }

    @Override
    public RedshiftOutputConnection connect(boolean autoCommit) throws SQLException
    {
        Connection c = driver.connect(url, properties);
        try {
            RedshiftOutputConnection con = new RedshiftOutputConnection(c, schemaName, autoCommit);
            c = null;
            return con;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
