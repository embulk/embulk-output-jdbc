package org.embulk.output.db2;

import static org.embulk.test.EmbulkTests.readResource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;

public class DB2Tests
{
    public static ConfigSource baseConfig()
    {
        return EmbulkTests.config("EMBULK_OUTPUT_DB2_TEST_CONFIG");
    }

    public static void execute(String sqlName)
    {
        executeSQL(readResource(sqlName));
    }

    public static String selectRecords(TestingEmbulk embulk, String tableName)
    {
        return executeQuery("SELECT * FROM " + tableName + " ORDER BY ID");
    }

    public static Connection connect() throws SQLException
    {
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        ConfigSource config = baseConfig();

        String url = String.format("jdbc:db2://%s:%s/%s",
                config.get(String.class, "host"),
                config.get(String.class, "port", "3306"),
                config.get(String.class, "database"));
        return DriverManager.getConnection(url, config.get(String.class, "user"), config.get(String.class, "password"));
    }

    public static String executeQuery(String sql)
    {
        StringBuilder result = new StringBuilder();
        try (Connection conn = connect();
             PreparedStatement preparedStatement = conn.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery();
        ) {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                result.append(metaData.getColumnName(i)).append(" ");
            }
            result.setLength(result.length() - 1);
            result.append("\n");
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    result.append(resultSet.getString(i)).append(" ");
                }
                result.setLength(result.length() - 1);
                result.append("\n");
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result.toString();
    }

    /**
     * execute sql content file with ; delimiter
     *
     * @param sqlContent
     */
    public static void executeSQL(String sqlContent)
    {
        try (Connection conn = connect();) {
            for (String s : sqlContent.split(";")) {
                if (StringUtils.isBlank(s)) {
                    continue;
                }
                try (PreparedStatement preparedStatement = conn.prepareStatement(s);) {
                    preparedStatement.execute();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
