package org.embulk.output.sqlserver;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.embulk.output.AbstractJdbcOutputPluginTest;
import org.embulk.output.SQLServerOutputPlugin;
import org.embulk.output.tester.EmbulkPluginTester;
import org.embulk.spi.OutputPlugin;
import org.junit.BeforeClass;
import org.junit.Test;


public class SQLServerOutputPluginTest extends AbstractJdbcOutputPluginTest
{
    private static boolean canTest;
    private static EmbulkPluginTester tester = new EmbulkPluginTester();
    static {
        tester.addPlugin(OutputPlugin.class, "sqlserver", SQLServerOutputPlugin.class);
    }

    @BeforeClass
    public static void beforeClass()
    {
        try {
            new SQLServerOutputPluginTest().connect();
            canTest = true;
        } catch (Throwable t) {
            System.out.println(t);
        } finally {
            if (!canTest) {
                System.out.println("Warning: you should put sqljdbc41.jar on classpath and prepare database.");
                System.out.println("(server = localhost, port = 1433, instance = SQLEXPRESS, database = TESTDB, user = TEST_USER, password = TEST_PW)");
            }
        }
    }

    @Test
    public void testInsertDirect() throws Exception
    {
        if (!canTest) {
            return;
        }

        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        tester.run(convertYml("/sqlserver/yml/test-insert-direct.yml"));

        assertTable(1, table);
    }

    @Test
    public void testInsertDirectCreate() throws Exception
    {
        if (!canTest) {
            return;
        }

        String table = "TEST1";

        dropTable(table);

        tester.run(convertYml("/sqlserver/yml/test-insert-direct.yml"));

        assertGeneratedTable(table);
    }

    @Test
    public void testInsert() throws Exception
    {
        if (!canTest) {
            return;
        }

        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        tester.run(convertYml("/sqlserver/yml/test-insert.yml"));

        assertTable(1, table);
    }

    @Test
    public void testInsertCreate() throws Exception
    {
        if (!canTest) {
            return;
        }

        String table = "TEST1";

        dropTable(table);

        tester.run(convertYml("/sqlserver/yml/test-insert.yml"));

        assertGeneratedTable(table);
    }

    @Test
    public void testTruncateInsert() throws Exception
    {
        if (!canTest) {
            return;
        }

        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        tester.run(convertYml("/sqlserver/yml/test-truncate-insert.yml"));

        assertTable(0, table);
    }

    @Test
    public void testReplace() throws Exception
    {
        if (!canTest) {
            return;
        }

        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        tester.run(convertYml("/sqlserver/yml/test-replace.yml"));

        assertGeneratedTable(table);
    }

    @Test
    public void testReplaceLongName() throws Exception
    {
        if (!canTest) {
            return;
        }

        String table = "TEST___Ａ１２３４５６７８９Ｂ１２３４５６７８９Ｃ１２３４５６７８９Ｄ１２３４５６７８９Ｅ１２３４５６７８９Ｆ１２３４５６７８９Ｇ１２３４５６７８９Ｈ１２３４５６７８９Ｉ１２３４５６７８９Ｊ１２３４５６７８９Ｋ１２３４５６７８９Ｌ１２３４５６７８９";
        assertEquals(127, table.length());

        dropTable(table);
        createTable(table);
        insertRecord(table);

        tester.run(convertYml("/sqlserver/yml/test-replace-long-name.yml"));

        assertGeneratedTable(table);
    }

    @Test
    public void testReplaceCreate() throws Exception
    {
        if (!canTest) {
            return;
        }

        String table = "TEST1";

        dropTable(table);

        tester.run(convertYml("/sqlserver/yml/test-replace.yml"));

        assertGeneratedTable(table);
    }

    @Test
    public void testStringToTimestamp() throws Exception
    {
        if (!canTest) {
            return;
        }

        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        tester.run(convertYml("/sqlserver/yml/test-string-timestamp.yml"));

        assertTable(1, table, true);
    }

    private void assertTable(int skip, String table) throws Exception
    {
        assertTable(skip, table, false);
    }

    private void assertTable(int skip, String table, boolean precise) throws Exception
    {
        List<List<Object>> rows = select(table);
        assertEquals(skip + 3, rows.size());
        rows = rows.subList(skip, skip + 3);

        Iterator<List<Object>> i1 = rows.iterator();
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A001", i2.next());
            assertEquals(Short.valueOf((short)0), i2.next());
            assertEquals(Short.valueOf((short)1234), i2.next());
            assertEquals(Integer.valueOf(123456), i2.next());
            assertEquals(Long.valueOf(12345678901L), i2.next());
            assertEquals(Boolean.FALSE, i2.next());
            assertEquals(new BigDecimal("1.23"), i2.next());
            assertEquals(new BigDecimal("3.456"), i2.next());
            assertEquals(new BigDecimal("12.3400"), i2.next());
            assertEquals(new BigDecimal("123.4567"), i2.next());
            assertEquals(Float.valueOf(0.1234567F), i2.next());
            assertEquals(Double.valueOf(0.12345678901234D), i2.next());
            assertEquals("a   ", i2.next());
            assertEquals("b", i2.next());
            assertEquals("c", i2.next());
            assertEquals("A   ", i2.next());
            assertEquals("B", i2.next());
            assertEquals("C", i2.next());
            assertEquals(createDate("2016/01/01"), i2.next());
            assertEquals(createTimestamp("2017/01/01 01:02:03", 123000000), i2.next());
            // Embulk timestamp doesn't support values under microseconds.
            assertEquals(createTimestamp("2018/01/01 01:02:03", precise? 123456700 : 123456000), i2.next());
            assertEquals(createTimestamp("2019/01/01 01:02:03", 120000000), i2.next());
            assertEquals(createTimestamp("2020/01/01 01:02:00", 0), i2.next());
            // Embulk timestamp doesn't support values under microseconds.
            assertEquals(createTime("03:04:05", precise? 123456700 : 123456000), i2.next());
            assertEquals(createTime("06:07:08", 120000000), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A002", i2.next());
            assertEquals(Short.valueOf((short)255), i2.next());
            assertEquals(Short.valueOf((short)-32768), i2.next());
            assertEquals(Integer.valueOf(-2147483648), i2.next());
            assertEquals(Long.valueOf(-9223372036854775808L), i2.next());
            assertEquals(Boolean.TRUE, i2.next());
            assertEquals(new BigDecimal("-9999999999.99"), i2.next());
            assertEquals(new BigDecimal("-99.999"), i2.next());
            assertEquals(new BigDecimal("-214748.3648"), i2.next());
            assertEquals(new BigDecimal("-922337203685477.5808"), i2.next());
            assertEquals(Float.valueOf(-9999000000F), i2.next());
            assertEquals(Double.valueOf(-999999999999000000D), i2.next());
            assertEquals("あい", i2.next());
            assertEquals("あいうえ", i2.next());
            assertEquals("あいうえお", i2.next());
            assertEquals("かき  ", i2.next());
            assertEquals("かきくけ", i2.next());
            assertEquals("かきくけこ", i2.next());
            assertEquals(createDate("2016/12/31"), i2.next());
            assertEquals(createTimestamp("2017/12/31 23:59:59", 997000000), i2.next());
            // Embulk timestamp doesn't support values under microseconds.
            assertEquals(createTimestamp("2018/12/31 23:59:59", precise? 999999900 : 999999000), i2.next());
            assertEquals(createTimestamp("2019/12/31 23:59:59", 990000000), i2.next());
            assertEquals(createTimestamp("2021/01/01 00:00:00", 0), i2.next());
            // Embulk timestamp doesn't support values under microseconds.
            assertEquals(createTime("23:59:59", precise? 999999900 : 999999000), i2.next());
            assertEquals(createTime("23:59:59", 990000000), i2.next());
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
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
        }
    }

    private void assertGeneratedTable(String table) throws Exception
    {
        List<List<Object>> rows = select(table);
        assertEquals(3, rows.size());

        Iterator<List<Object>> i1 = rows.iterator();
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A001", i2.next());
            assertEquals(Long.valueOf(0), i2.next());
            assertEquals(Long.valueOf(1234), i2.next());
            assertEquals(Long.valueOf(123456), i2.next());
            assertEquals(Long.valueOf(12345678901L), i2.next());
            assertEquals(Boolean.FALSE, i2.next());
            assertEquals("1.23", i2.next());
            assertEquals("3.456", i2.next());
            assertEquals("12.34", i2.next());
            assertEquals("123.4567", i2.next());
            assertEquals(Double.valueOf(0.1234567D), i2.next());
            assertEquals(Double.valueOf(0.12345678901234D), i2.next());
            assertEquals("a", i2.next());
            assertEquals("b", i2.next());
            assertEquals("c", i2.next());
            assertEquals("A", i2.next());
            assertEquals("B", i2.next());
            assertEquals("C", i2.next());
            assertEquals(createTimestamp("2016/01/01 00:00:00", 0), i2.next());
            assertEquals(createTimestamp("2017/01/01 01:02:03", 123000000), i2.next());
            // Embulk timestamp doesn't support values under microseconds.
            //assertEquals(createTimestamp("2018/01/01 01:02:03", 123456700), i2.next());
            assertEquals(createTimestamp("2018/01/01 01:02:03", 123456000), i2.next());
            assertEquals(createTimestamp("2019/01/01 01:02:03", 120000000), i2.next());
            assertEquals(createTimestamp("2020/01/01 01:02:03", 0), i2.next());
            // Embulk timestamp doesn't support values under microseconds.
            //assertEquals(createTime("03:04:05", 123456700), new PreciseTime((Timestamp)i2.next()));
            assertEquals(createTime("03:04:05", 123456000), new PreciseTime((Timestamp)i2.next()));
            assertEquals(createTime("06:07:08", 120000000), new PreciseTime((Timestamp)i2.next()));
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A002", i2.next());
            assertEquals(Long.valueOf((short)255), i2.next());
            assertEquals(Long.valueOf((short)-32768), i2.next());
            assertEquals(Long.valueOf(-2147483648), i2.next());
            assertEquals(Long.valueOf(-9223372036854775808L), i2.next());
            assertEquals(Boolean.TRUE, i2.next());
            assertEquals("-9999999999.99", i2.next());
            assertEquals("-99.999", i2.next());
            assertEquals("-214748.3648", i2.next());
            assertEquals("-922337203685477.5808", i2.next());
            assertEquals(Double.valueOf(-9999000000D), i2.next());
            assertEquals(Double.valueOf(-999999999999000000D), i2.next());
            assertEquals("あい", i2.next());
            assertEquals("あいうえ", i2.next());
            assertEquals("あいうえお", i2.next());
            assertEquals("かき", i2.next());
            assertEquals("かきくけ", i2.next());
            assertEquals("かきくけこ", i2.next());
            assertEquals(createTimestamp("2016/12/31 00:00:00", 0), i2.next());
            assertEquals(createTimestamp("2017/12/31 23:59:59", 997000000), i2.next());
            // Embulk timestamp doesn't support values under microseconds.
            //assertEquals(createTimestamp("2018/12/31 23:59:59", 999999900), i2.next());
            assertEquals(createTimestamp("2018/12/31 23:59:59", 999999000), i2.next());
            assertEquals(createTimestamp("2019/12/31 23:59:59", 990000000), i2.next());
            assertEquals(createTimestamp("2020/12/31 23:59:59", 0), i2.next());
            // Embulk timestamp doesn't support values under microseconds.
            //assertEquals(createTime("23:59:59", 999999900), new PreciseTime((Timestamp)i2.next()));
            assertEquals(createTime("23:59:59", 999999000), new PreciseTime((Timestamp)i2.next()));
            assertEquals(createTime("23:59:59", 990000000), new PreciseTime((Timestamp)i2.next()));
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
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
        }
    }

    @Override
    protected Object getValue(ResultSet resultSet, int index) throws SQLException {
        if (resultSet.getMetaData().getColumnTypeName(index).equals("time")) {
            Timestamp timestamp = resultSet.getTimestamp(index);
            if (timestamp == null) {
                return null;
            }
            return new PreciseTime(timestamp);
        }
        return super.getValue(resultSet, index);
    }

    private java.sql.Date createDate(String s) throws ParseException
    {
        DateFormat format = new SimpleDateFormat("yyyy/MM/dd");
        Date date = format.parse(s);
        return new java.sql.Date(date.getTime());
    }

    private Timestamp createTimestamp(String s, int nanos) throws ParseException
    {
        DateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = format.parse(s);
        Timestamp timestamp = new Timestamp(date.getTime());
        timestamp.setNanos(nanos);
        return timestamp;
    }

    private PreciseTime createTime(String s, int nanos) throws ParseException
    {
        DateFormat format = new SimpleDateFormat("HH:mm:ss");
        Date date = format.parse(s);
        return new PreciseTime(date.getTime(), nanos);
    }

    private void createTable(String table) throws SQLException
    {
        String sql = String.format("CREATE TABLE %s ("
                + "ID                  CHAR(4),"
                + "TINYINT_ITEM        TINYINT,"
                + "SMALLINT_ITEM       SMALLINT,"
                + "INT_ITEM            INT,"
                + "BIGINT_ITEM         BIGINT,"
                + "BIT_ITEM            BIT,"
                + "DECIMAL_ITEM        DECIMAL(12,2),"
                + "NUMERIC_ITEM        NUMERIC(5,3),"
                + "SMALLMONEY_ITEM     SMALLMONEY,"
                + "MONEY_ITEM          MONEY,"
                + "REAL_ITEM           REAL,"
                + "FLOAT_ITEM          FLOAT,"
                + "CHAR_ITEM           CHAR(4),"
                + "VARCHAR_ITEM        VARCHAR(8),"
                + "TEXT_ITEM           TEXT,"
                + "NCHAR_ITEM          NCHAR(4),"
                + "NVARCHAR_ITEM       NVARCHAR(8),"
                + "NTEXT_ITEM          NTEXT,"
                + "DATE_ITEM           DATE,"
                + "DATETIME_ITEM       DATETIME,"
                + "DATETIME2_ITEM      DATETIME2,"
                + "DATETIME2_2_ITEM    DATETIME2(2),"
                + "SMALLDATETIME_ITEM  SMALLDATETIME,"
                + "TIME_ITEM           TIME,"
                + "TIME_2_ITEM         TIME(2),"
                + "PRIMARY KEY (ID))", table);
        executeSQL(sql);
    }

    private void insertRecord(String table) throws SQLException
    {
        executeSQL(String.format("INSERT INTO %s VALUES('9999',"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL)", table));
    }

    @Override
    protected Connection connect() throws SQLException
    {
        return DriverManager.getConnection("jdbc:sqlserver://localhost\\SQLEXPRESS:1433;databasename=TESTDB", "TEST_USER", "test_pw");
    }

}
