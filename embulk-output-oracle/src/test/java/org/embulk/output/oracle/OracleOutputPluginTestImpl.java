package org.embulk.output.oracle;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.embulk.input.filesplit.LocalFileSplitInputPlugin;
import org.embulk.output.tester.EmbulkPluginTester;
import org.embulk.spi.InputPlugin;

import com.google.common.io.Files;


public class OracleOutputPluginTestImpl
{
    private EmbulkPluginTester tester;
    private String pluginName;

    public void setTester(EmbulkPluginTester tester) {
        this.tester = tester;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }

    public String beforeClass()
    {
        try {
            Class.forName("oracle.jdbc.OracleDriver");

            try (Connection connection = connect()) {
                String version = connection.getMetaData().getDriverVersion();
                System.out.println("Driver version = " + version);
                return version;
            }

        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            //throw new RuntimeException("You should put Oracle JDBC driver on 'driver' directory.");
            System.err.println("Warning: put Oracle JDBC driver on 'driver' directory in order to test embulk-output-oracle plugin.");

        } catch (SQLException e) {
            System.err.println(e);
            //throw new RuntimeException("You should prepare a schema on Oracle 12c (database = 'TESTDB', user = 'TEST_USER', password = 'test_pw', charset = UTF-8).");
            System.err.println("Warning: prepare a schema on Oracle 12c (database = 'TESTDB', user = 'TEST_USER', password = 'test_pw', charset = UTF-8).");
            // for example
            //   CREATE USER EMBULK_USER IDENTIFIED BY "embulk_pw";
            //   GRANT DBA TO EMBULK_USER;
        }

        return null;
    }

    public void testInsert() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        run("/yml/test-insert.yml");

        assertTable(table);
    }

    public void testInsertCreate() throws Exception
    {
        String table = "TEST1";

        dropTable(table);

        run("/yml/test-insert.yml");

        assertGeneratedTable1(table);
    }

    public void testInsertEmpty() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        new File(convertPath("/data/"), "test2").mkdir();
        run("/yml/test-insert-empty.yml");

        assertTableEmpty(table);
    }

    public void testTruncateInsert() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        run("/yml/test-truncate-insert.yml");

        assertTable(table);
    }

    public void testTruncateInsertOCIMethod() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        run("/yml/test-truncate-insert-oci-method.yml");

        assertTable(table);
    }

    public void testTruncateInsertCreate() throws Exception
    {
        String table = "TEST1";

        dropTable(table);

        run("/yml/test-truncate-insert.yml");

        assertGeneratedTable1(table);
    }

    public void testInsertDirect() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        run("/yml/test-insert-direct.yml");

        assertTable(table);
    }

    public void testInsertDirectEmpty() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        new File(convertPath("/data/"), "test2").mkdir();
        run("/yml/test-insert-direct-empty.yml");

        assertTableEmpty(table);
    }

    public void testInsertDirectCreate() throws Exception
    {
        String table = "TEST1";

        dropTable(table);

        run("/yml/test-insert-direct.yml");

        assertGeneratedTable1(table);
    }

    public void testInsertDirectDirectMethod() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        run("/yml/test-insert-direct-direct-method.yml");

        assertTable(table);
    }

    public void testInsertDirectOCIMethod() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        run("/yml/test-insert-direct-oci-method.yml");

        assertTable(table);
    }

    public void testInsertDirectOCIMethodSplit() throws Exception
    {
        tester.addPlugin(InputPlugin.class, "filesplit", LocalFileSplitInputPlugin.class);

        String table = "TEST1";

        dropTable(table);
        createTable(table);

        run("/yml/test-insert-direct-oci-method-split.yml");

        assertTable(table);
    }

    public void testUrl() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        run("/yml/test-url.yml");

        assertTable(table);
    }

    public void testReplace() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        run("/yml/test-replace.yml");

        assertGeneratedTable2(table);
    }

    public void testReplaceOCIMethod() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        run("/yml/test-replace-oci-method.yml");

        assertGeneratedTable2(table);
    }

    public void testReplaceEmpty() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        run("/yml/test-replace-empty.yml");

        assertTableEmpty(table);
    }

    public void testReplaceCreate() throws Exception
    {
        String table = "TEST1";

        dropTable(table);

        run("/yml/test-replace.yml");

        assertGeneratedTable2(table);
    }


    public void testReplaceLongName() throws Exception
    {
        String table = "TEST12345678901234567890123456";

        dropTable(table);
        createTable(table);

        run("/yml/test-replace-long-name.yml");

        assertGeneratedTable2(table);
    }

    public void testReplaceLongNameMultibyte() throws Exception
    {
        String table = "ＴＥＳＴ123456789012345678";

        run("/yml/test-replace-long-name-multibyte.yml");

        assertGeneratedTable2(table);
    }

    public void testStringTimestamp() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        run("/yml/test-string-timestamp.yml");

        assertTable(table);
    }

    private void dropTable(String table) throws SQLException
    {
        String sql = String.format("DROP TABLE %s", table);
        executeSQL(sql, true);
    }

    private void createTable(String table) throws SQLException
    {
        String sql = String.format("CREATE TABLE %s ("
                + "ID              CHAR(4),"
                + "VARCHAR2_ITEM   VARCHAR2(6),"
                + "NVARCHAR2_ITEM  NVARCHAR2(6),"
                + "INTEGER_ITEM    NUMBER(4,0),"
                + "NUMBER_ITEM     NUMBER(10,2),"
                + "DATE_ITEM       DATE,"
                + "TIMESTAMP_ITEM  TIMESTAMP,"
                + "PRIMARY KEY (ID))", table);
        executeSQL(sql);
    }

    private void insertRecord(String table) throws SQLException
    {
        executeSQL(String.format("INSERT INTO %s VALUES('9999', NULL, NULL, NULL, NULL, NULL, NULL)", table));
    }

    private void assertTable(String table) throws Exception
    {
        // datetime of UTC will be inserted by embulk.
        // datetime of default timezone will be selected by JDBC.
        TimeZone timeZone = TimeZone.getDefault();
        List<List<Object>> rows = select(table);

        /*
        A001,ABCDE,abcde,,0,123.45,2015/03/05,2015/03/05 12:34:56
        A002,ＡＢ,ａｂｃｄｅｆ,-9999,-99999999.99,2015/03/06,2015/03/06 23:59:59
        A003,,,,,,
        */

        assertEquals(3, rows.size());
        Iterator<List<Object>> i1 = rows.iterator();
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A001", i2.next());
            assertEquals("ABCDE", i2.next());
            assertEquals("abcde", i2.next());
            assertEquals(new BigDecimal("0"), i2.next());
            assertEquals(new BigDecimal("123.45"), i2.next());
            assertEquals(toTimestamp("2015/03/05 00:00:00", timeZone), i2.next());
            assertEquals(toOracleTimestamp("2015/03/05 12:34:56", timeZone), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A002", i2.next());
            assertEquals("ＡＢ", i2.next());
            assertEquals("ａｂｃｄｅｆ", i2.next());
            assertEquals(new BigDecimal("-9999"), i2.next());
            assertEquals(new BigDecimal("-99999999.99"), i2.next());
            assertEquals(toTimestamp("2015/03/06 00:00:00", timeZone), i2.next());
            assertEquals(toOracleTimestamp("2015/03/06 23:59:59", timeZone), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A003", i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
        }
    }

    private void assertTableEmpty(String table) throws Exception
    {
        List<List<Object>> rows = select(table);
        assertEquals(0, rows.size());
    }

    private void assertGeneratedTable1(String table) throws Exception
    {
        // datetime of UTC will be inserted by embulk.
        // datetime of default timezone will be selected by JDBC.
        TimeZone timeZone = TimeZone.getDefault();
        List<List<Object>> rows = select(table);

        /*
        A001,ABCDE,abcde,0,123.45,2015/03/05,2015/03/05 12:34:56
        A002,ＡＢ,ａｂｃｄｅｆ,-9999,-99999999.99,2015/03/06,2015/03/06 23:59:59
        A003,,,,,,
        */

        assertEquals(3, rows.size());
        Iterator<List<Object>> i1 = rows.iterator();
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A001", i2.next());
            assertEquals("ABCDE", i2.next());
            assertEquals("abcde", i2.next());
            assertEquals(new BigDecimal("0"), i2.next());
            assertEquals("123.45", i2.next());
            assertEquals(toOracleTimestamp("2015/03/05 00:00:00", timeZone), i2.next());
            assertEquals(toOracleTimestamp("2015/03/05 12:34:56", timeZone), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A002", i2.next());
            assertEquals("ＡＢ", i2.next());
            assertEquals("ａｂｃｄｅｆ", i2.next());
            assertEquals(new BigDecimal("-9999"), i2.next());
            assertEquals("-99999999.99", i2.next());
            assertEquals(toOracleTimestamp("2015/03/06 00:00:00", timeZone), i2.next());
            assertEquals(toOracleTimestamp("2015/03/06 23:59:59", timeZone), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A003", i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
        }
    }

    private void assertGeneratedTable2(String table) throws Exception
    {
        // datetime of UTC will be inserted by embulk.
        // datetime of default timezone will be selected by JDBC.
        TimeZone timeZone = TimeZone.getDefault();
        List<List<Object>> rows = select(table);

        /*
        A001,ABCDE,abcde,0,123.45,2015/03/05,2015/03/05 12:34:56
        A002,ＡＢ,ａｂｃｄｅｆ,-9999,-99999999.99,2015/03/06,2015/03/06 23:59:59
        A003,,,,,,
        */

        assertEquals(3, rows.size());
        Iterator<List<Object>> i1 = rows.iterator();
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A001", i2.next());
            assertEquals("ABCDE", i2.next());
            assertEquals("abcde", i2.next());
            assertEquals(new BigDecimal("0"), i2.next());
            assertEquals(new BigDecimal("123.45"), i2.next());
            assertEquals(toTimestamp("2015/03/05 00:00:00", timeZone), i2.next());
            assertEquals(toOracleTimestamp("2015/03/05 12:34:56", timeZone), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A002", i2.next());
            assertEquals("ＡＢ", i2.next());
            assertEquals("ａｂｃｄｅｆ", i2.next());
            assertEquals(new BigDecimal("-9999"), i2.next());
            assertEquals(new BigDecimal("-99999999.99"), i2.next());
            assertEquals(toTimestamp("2015/03/06 00:00:00", timeZone), i2.next());
            assertEquals(toOracleTimestamp("2015/03/06 23:59:59", timeZone), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A003", i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
        }
    }


    private Timestamp toTimestamp(String s, TimeZone timeZone)
    {
        for (String formatString : new String[]{"yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd"}) {
            DateFormat dateFormat = new SimpleDateFormat(formatString);
            dateFormat.setTimeZone(timeZone);
            try {
                Date date = dateFormat.parse(s);
                return new Timestamp(date.getTime());
            } catch (ParseException e) {
                // NOP
            }
        }
        throw new IllegalArgumentException(s);
    }

    private Object toOracleTimestamp(String s, TimeZone timeZone) throws Exception
    {
        Class<?> timestampClass = Class.forName("oracle.sql.TIMESTAMP");
        Constructor<?> constructor = timestampClass.getConstructor(Timestamp.class);
        return constructor.newInstance(toTimestamp(s, timeZone));
    }


    private List<List<Object>> select(String table) throws SQLException
    {
        try (Connection connection = connect()) {
            try (Statement statement = connection.createStatement()) {
                List<List<Object>> rows = new ArrayList<List<Object>>();
                String sql = "SELECT * FROM " + table;
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

    private static Connection connect() throws SQLException
    {
        return DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:TESTDB", "TEST_USER", "test_pw");
    }

    private void run(String ymlName) throws Exception
    {
        tester.run(convertYml(ymlName));
    }

    private String convertYml(String ymlName) throws Exception
    {
        StringBuilder builder = new StringBuilder();
        Pattern pathPrefixPattern = Pattern.compile("^ *path(_prefix)?: '(.*)'$");
        for (String line : Files.readLines(convertPath(ymlName), Charset.defaultCharset())) {
            line = line.replaceAll("type: oracle", "type: " + pluginName);
            Matcher matcher = pathPrefixPattern.matcher(line);
            if (matcher.matches()) {
                int group = 2;
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

}
