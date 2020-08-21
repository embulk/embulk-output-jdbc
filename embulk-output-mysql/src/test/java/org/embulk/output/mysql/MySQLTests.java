package org.embulk.output.mysql;

import org.apache.commons.lang3.StringUtils;
import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkTests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static java.util.stream.Collectors.joining;

public class MySQLTests
{
    public static ConfigSource baseConfig()
    {
        return EmbulkTests.config("EMBULK_OUTPUT_MYSQL_TEST_CONFIG");
    }

    public static Connection connect() throws SQLException
    {
        ConfigSource config = baseConfig();

        String url = String.format("jdbc:mysql://%s:%s/%s",
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
            while (resultSet.next()) {
                result.append(resultSet.getString(1)).append(System.lineSeparator());
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result.toString();
    }

    /**
     * execute sql content file
     *
     * @param sqlContent
     */
    public static void execute(String sqlContent)
    {
        try (Connection conn = connect()) {
            for (String s : sqlContent.split(";")) {
                if (StringUtils.isBlank(s)) {
                    continue;
                }
                try (PreparedStatement preparedStatement = conn.prepareStatement(s)) {
                    preparedStatement.execute();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static String selectRecords(String tableName, List<String> columnList)
    {
        final String cols = columnList.stream().collect((joining(",")));
        return executeQuery(String.format("SELECT concat(CONCAT_WS(',', %s)) from %s", cols, tableName));
    }
}
