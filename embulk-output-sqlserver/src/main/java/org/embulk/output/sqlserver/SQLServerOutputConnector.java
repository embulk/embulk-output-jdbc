package org.embulk.output.sqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.AbstractJdbcOutputConnector;
import org.embulk.spi.Exec;
import org.slf4j.Logger;


public class SQLServerOutputConnector
        extends AbstractJdbcOutputConnector
{
    private final Logger logger = Exec.getLogger(getClass());

    private final String url;
    private final Properties properties;
    private final String schemaName;

    public SQLServerOutputConnector(String url, Properties properties, String schemaName)
    {
        this.url = url;
        this.properties = properties;
        this.schemaName = schemaName;
    }

    @Override
    protected JdbcOutputConnection connect() throws SQLException
    {
        Connection c = DriverManager.getConnection(url, properties);
        if (c == null) {
            // driver.connect returns null when url is "jdbc:mysql://...".
            throw new SQLException("Invalid url : " + url);
        }

        String schemaName = this.schemaName;
        if (schemaName == null) {
            // get default schema name for user (Connection#getSchema won't work)
            try {
                try (PreparedStatement statement = c.prepareStatement("SELECT default_schema_name FROM sys.database_principals WHERE name = ?")) {
                    statement.setString(1, properties.getProperty("user"));
                    try (ResultSet rs = statement.executeQuery()) {
                        if (rs.next()) {
                            schemaName = rs.getString(1);
                        }
                    }
                }
            } catch (SQLException e) {
                logger.warn("Cannot specify default schema : " + e);
            }
        }

        try {
            SQLServerOutputConnection con = new SQLServerOutputConnection(c, schemaName);
            c = null;
            return con;

        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
