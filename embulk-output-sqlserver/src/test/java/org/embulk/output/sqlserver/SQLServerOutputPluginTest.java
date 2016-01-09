package org.embulk.output.sqlserver;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.embulk.output.SQLServerOutputPlugin;
import org.embulk.output.tester.EmbulkPluginTester;
import org.embulk.spi.OutputPlugin;
import org.junit.Test;

import com.google.common.io.Files;

public class SQLServerOutputPluginTest {

    private static EmbulkPluginTester tester = new EmbulkPluginTester();
    static {
        tester.addPlugin(OutputPlugin.class, "sqlserver", SQLServerOutputPlugin.class);
    }

    @Test
    public void testInsertDirect() throws Exception
    {
        dropTable("TEST1");
        createTable("TEST1");

        tester.run(convertYml("/sqlserver/yml/test-insert-direct.yml"));
    }

    private void dropTable(String table) throws SQLException
    {
        String sql = String.format("DROP TABLE %s", table);
        executeSQL(sql, true);
    }

    private void createTable(String table) throws SQLException
    {
        String sql = String.format("CREATE TABLE %s ("
                + "ID             CHAR(4),"
                + "TINYINT_ITEM   TINYINT,"
                + "SMALLINT_ITEM  SMALLINT,"
                + "INT_ITEM       INT,"
                + "BIGINT_ITEM    BIGINT,"
                + "PRIMARY KEY (ID))", table);
        executeSQL(sql);
    }

    private void executeSQL(String sql) throws SQLException
    {
        executeSQL(sql, false);
    }

    private void executeSQL(String sql, boolean ignoreError) throws SQLException
    {
        try (Connection connection = connect()) {
            try {
                connection.setAutoCommit(true);

                try (Statement statement = connection.createStatement()) {
                    System.out.println(String.format("Execute SQL : \"%s\".", sql));
                    statement.execute(sql);
                }

            } catch (SQLException e) {
                if (!ignoreError) {
                    throw e;
                }
            }
        }
    }

    private String convertYml(String ymlName) throws Exception
    {
        StringBuilder builder = new StringBuilder();
        Pattern pathPrefixPattern = Pattern.compile("^ *path_prefix: '(.*)'$");
        for (String line : Files.readLines(convertPath(ymlName), Charset.defaultCharset())) {
            Matcher matcher = pathPrefixPattern.matcher(line);
            if (matcher.matches()) {
                int group = 1;
                builder.append(line.substring(0, matcher.start(group)));
                builder.append(convertPath(matcher.group(group)).getAbsolutePath());
                builder.append(line.substring(matcher.end(group)));
            } else {
                builder.append(line);
            }
            builder.append(System.lineSeparator());
        }
        return builder.toString();
    }

    private File convertPath(String name) throws URISyntaxException
    {
        return new File(getClass().getResource(name).toURI());
    }

    private static Connection connect() throws SQLException
    {
        return DriverManager.getConnection("jdbc:sqlserver://localhost\\SQLEXPRESS:1433;databasename=TESTDB", "TEST_USER", "test_pw");
    }

}
