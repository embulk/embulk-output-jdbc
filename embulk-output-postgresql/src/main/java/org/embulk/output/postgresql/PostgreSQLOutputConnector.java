package org.embulk.output.postgresql;

import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.AbstractJdbcOutputConnector;
import org.embulk.output.jdbc.TransactionIsolation;

public class PostgreSQLOutputConnector
        extends AbstractJdbcOutputConnector
{
    private final String url;
    private final Properties properties;
    private final String schemaName;
    private final String roleName;

    public PostgreSQLOutputConnector(String url, Properties properties, String schemaName,
            Optional<TransactionIsolation> transactionIsolation, String roleName)
    {
        super(transactionIsolation);
        this.url = url;
        this.properties = properties;
        this.schemaName = schemaName;
        this.roleName = roleName;
    }

    @Override
    protected JdbcOutputConnection connect() throws SQLException
    {
        Connection c = DriverManager.getConnection(url, properties);
        try {
            PostgreSQLOutputConnection con = new PostgreSQLOutputConnection(c, schemaName, roleName);
            c = null;
            return con;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
