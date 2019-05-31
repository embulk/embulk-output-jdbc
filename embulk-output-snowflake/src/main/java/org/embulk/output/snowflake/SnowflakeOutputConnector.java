package org.embulk.output.snowflake;

import com.google.common.base.Optional;
import org.embulk.output.jdbc.AbstractJdbcOutputConnector;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.TransactionIsolation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class SnowflakeOutputConnector
        extends AbstractJdbcOutputConnector
{
    private final String url;
    private final Properties properties;
    private final String schemaName;

    public SnowflakeOutputConnector(String url, Properties properties, String schemaName,
                                    Optional<TransactionIsolation> transactionIsolation)
    {
        super(transactionIsolation);
        try {
            Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        this.url = url;
        this.properties = properties;
        this.schemaName = schemaName;
    }

    @Override
    protected JdbcOutputConnection connect() throws SQLException
    {
        Connection c = DriverManager.getConnection(url, properties);
        try {
            SnowflakeOutputConnection con = new SnowflakeOutputConnection(c, schemaName);
            c = null;
            return con;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}