package org.embulk.output.oracle;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.net.URISyntaxException;
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
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.embulk.output.OracleOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.junit.BeforeClass;
import org.junit.Test;


public class OracleOutputPluginTest
{
    private static boolean test;
    private EmbulkPluginTester tester = new EmbulkPluginTester(OutputPlugin.class, "oracle", OracleOutputPlugin.class);

    private String dropTable = "DROP TABLE TEST1";
    private String createTable = "CREATE TABLE TEST1 ("
            + "ID              CHAR(4),"
            + "VARCHAR2_ITEM   VARCHAR2(20),"
            + "INTEGER_ITEM     NUMBER(4,0),"
            + "NUMBER_ITEM     NUMBER(10,2),"
            + "DATE_ITEM       DATE,"
            + "TIMESTAMP_ITEM  TIMESTAMP,"
            + "PRIMARY KEY (ID))";

    @BeforeClass
    public static void beforeClass()
    {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
            test = true;

            try (Connection connection = connect()) {
                // NOP
            } catch (SQLException e) {
                // NOP
            }
        } catch (ClassNotFoundException e) {
            //throw new RuntimeException("You should put Oracle JDBC driver on 'driver' directory.");
            test = false;
            System.err.println("Warning: put Oracle JDBC driver on 'driver' directory in order to test embulk-output-oracle plugin.");
        }
    }


    @Test
    public void testInsert() throws Exception {
        if (!test) {
            return;
        }

        executeSQL(dropTable, true);
        executeSQL(createTable);

        run("/yml/test-insert.yml");

        List<List<Object>> rows = select("TEST1");

        /*
        A001,ABCDE,0,123.45,2015/03/05,2015/03/05 12:34:56
        A002,あいうえお,-9999,-99999999.99,2015/03/06,2015/03/06 23:59:59
        A003,,,,,
        */

        assertEquals(3, rows.size());
        Iterator<List<Object>> i1 = rows.iterator();
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A001", i2.next());
            assertEquals("ABCDE", i2.next());
            assertEquals(new BigDecimal("0"), i2.next());
            assertEquals(new BigDecimal("123.45"), i2.next());
            assertEquals(toTimestamp("2015/03/05 00:00:00"), i2.next());
            assertEquals(toOracleTimestamp("2015/03/05 12:34:56"), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A002", i2.next());
            assertEquals("あいうえお", i2.next());
            assertEquals(new BigDecimal("-9999"), i2.next());
            assertEquals(new BigDecimal("-99999999.99"), i2.next());
            assertEquals(toTimestamp("2015/03/06 00:00:00"), i2.next());
            assertEquals(toOracleTimestamp("2015/03/06 23:59:59"), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A003", i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
        }
    }

    private Timestamp toTimestamp(String s) {
        for (String formatString : new String[]{"yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd"}) {
            DateFormat dateFormat = new SimpleDateFormat(formatString);
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            try {
                Date date = dateFormat.parse(s);
                return new Timestamp(date.getTime());
            } catch (ParseException e) {
                // NOP
            }
        }
        throw new IllegalArgumentException(s);
    }

    private Object toOracleTimestamp(String s) throws Exception {
        Class<?> timestampClass = Class.forName("oracle.sql.TIMESTAMP");
        Constructor<?> constructor = timestampClass.getConstructor(Timestamp.class);
        return constructor.newInstance(toTimestamp(s));
    }


    private List<List<Object>> select(String table) throws SQLException
    {
        try (Connection connection = connect()) {
            try (Statement statement = connection.createStatement()) {
                List<List<Object>> rows = new ArrayList<List<Object>>();
                try (ResultSet resultSet = statement.executeQuery("SELECT * FROM " + table)) {
                    while (resultSet.next()) {
                        List<Object> row = new ArrayList<Object>();
                        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                            row.add(resultSet.getObject(i));
                        }
                        rows.add(row);
                    }
                }
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

    private static Connection connect()
    {
        try {
            return DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:EMBULKDB", "EMBULK_USER", "embulk_pw");

        } catch (SQLException e) {
            throw new RuntimeException("You should prepare a schema on Oracle (database = 'EMBULKDB', user = 'EMBULK_USER', password = 'embulk_pw').");
            // for example
            //   CREATE USER EMBULK_USER IDENTIFIED BY "embulk_pw";
            //   GRANT DBA TO EMBULK_USER;
        }
    }

    private void run(String ymlName) throws Exception
    {
        tester.run(convertYml(ymlName));
    }

    private String convertYml(String ymlName)
    {
        try {
            File ymlPath = convertPath(ymlName);
            File tempYmlPath = new File(ymlPath.getParentFile(), "temp-" + ymlPath.getName());
            Pattern pathPrefixPattern = Pattern.compile("^ *path(_prefix)?: '(.*)'$");
            try (BufferedReader reader = new BufferedReader(new FileReader(ymlPath))) {
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempYmlPath))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        Matcher matcher = pathPrefixPattern.matcher(line);
                        if (matcher.matches()) {
                            int group = 2;
                            writer.write(line.substring(0, matcher.start(group)));
                            writer.write(convertPath(matcher.group(group)).getAbsolutePath());
                            writer.write(line.substring(matcher.end(group)));
                        } else {
                            writer.write(line);
                        }
                        writer.newLine();
                    }
                }
            }
            return tempYmlPath.getAbsolutePath();

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private File convertPath(String name) throws URISyntaxException {
        return new File(getClass().getResource(name).toURI());
    }

}
