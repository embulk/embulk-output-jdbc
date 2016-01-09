package org.embulk.output.sqlserver;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
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
        String tableName = "TEST1";
        dropTable(tableName);
        createTable(tableName);
        insertRecord(tableName);

        tester.run(convertYml("/sqlserver/yml/test-insert-direct.yml"));

        assertTable(1, tableName);
    }

    private void assertTable(int skip, String table) throws Exception
    {
        // datetime of UTC will be inserted by embulk.
        // datetime of default timezone will be selected by JDBC.
        TimeZone timeZone = TimeZone.getDefault();
        List<List<Object>> rows = select(table);
        assertEquals(skip + 3, rows.size());
        rows = rows.subList(skip, skip + 3);

        /*
        A001,ABCDE,abcde,,0,123.45,2015/03/05,2015/03/05 12:34:56
        A002,ＡＢ,ａｂｃｄｅｆ,-9999,-99999999.99,2015/03/06,2015/03/06 23:59:59
        A003,,,,,,
        */

        Iterator<List<Object>> i1 = rows.iterator();
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A001", i2.next());
            assertEquals(Short.valueOf((short)0), i2.next());
            assertEquals(Short.valueOf((short)1234), i2.next());
            assertEquals(Integer.valueOf(123456), i2.next());
            assertEquals(Long.valueOf(12345678901L), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A002", i2.next());
            assertEquals(Short.valueOf((short)255), i2.next());
            assertEquals(Short.valueOf((short)-32768), i2.next());
            assertEquals(Integer.valueOf(-2147483648), i2.next());
            assertEquals(Long.valueOf(-9223372036854775808L), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A003", i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
        }
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

    private void insertRecord(String table) throws SQLException
    {
        executeSQL(String.format("INSERT INTO %s VALUES('9999', NULL, NULL, NULL, NULL)", table));
    }

    private List<List<Object>> select(String table) throws SQLException
    {
        try (Connection connection = connect()) {
            try (Statement statement = connection.createStatement()) {
                List<List<Object>> rows = new ArrayList<List<Object>>();
                String sql = String.format("SELECT * FROM %s ORDER BY ID", table);
                System.out.println(sql);
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    while (resultSet.next()) {
                        List<Object> row = new ArrayList<Object>();
                        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                            Object value = resultSet.getObject(i);
                            if (value != null && value.getClass().getName().equals("oracle.sql.CLOB")) {
                                value = resultSet.getString(i);
                            }
                            row.add(value);
                        }
                        rows.add(row);
                    }
                }
                // cannot sort by CLOB, so sort by Java
                Collections.sort(rows, new Comparator<List<Object>>() {
                    @Override
                    public int compare(List<Object> o1, List<Object> o2) {
                        return o1.toString().compareTo(o2.toString());
                    }
                });
                return rows;
            }
        }

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
