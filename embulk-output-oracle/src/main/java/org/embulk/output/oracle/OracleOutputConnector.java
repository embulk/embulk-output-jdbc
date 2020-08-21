package org.embulk.output.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Optional;

import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.AbstractJdbcOutputConnector;
import org.embulk.output.jdbc.TransactionIsolation;

public class OracleOutputConnector
        extends AbstractJdbcOutputConnector
{
    private final String url;
    private final Properties properties;
    private final String schemaName;
    private final boolean direct;

    public OracleOutputConnector(String url, Properties properties, String schemaName, boolean direct,
            Optional<TransactionIsolation> transactionIsolation)
    {
        super(transactionIsolation);
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        this.url = url;
        this.properties = properties;
        this.schemaName = schemaName;
        this.direct = direct;
    }

    @Override
    protected JdbcOutputConnection connect() throws SQLException
    {
        Connection c = DriverManager.getConnection(url, properties);
        if (c == null) {
            // driver.connect returns null when url is "jdbc:mysql://...".
            throw new SQLException("Invalid url : " + url);
        }

        try {
            OracleOutputConnection con = new OracleOutputConnection(c, schemaName, direct);
            c = null;
            return con;

        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
