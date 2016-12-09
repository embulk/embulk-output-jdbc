package org.embulk.output.sqlserver;

import org.embulk.output.AbstractJdbcOutputPluginTest;
import org.embulk.output.SQLServerOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.junit.Test;

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

import static java.util.Locale.ENGLISH;
import static org.junit.Assert.assertEquals;


public class SQLServerOutputPluginTest extends AbstractJdbcOutputPluginTest
{
    private boolean useJtdsDriver = false;


    @Override
    protected void prepare() throws SQLException {
        tester.addPlugin(OutputPlugin.class, "sqlserver", SQLServerOutputPlugin.class);

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("Warning: you should put 'sqljdbc41.jar' in 'embulk-output-sqlserver/driver' directory in order to test.");
            return;
        }

        try {
            connect();
            enabled = true;
        } catch (Throwable t) {
            System.out.println(t);
        } finally {
            if (!enabled) {
                System.out.println(String.format(ENGLISH, "Warning: you should prepare database in order to test (server = %s, port = %d, database = %s, user = %s, password = %s).",
                        getHost(), getPort(), getDatabase(), getUser(), getPassword()));
            }
        }
    }

    @Test
    public void testInsertDirect() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        test("/sqlserver/yml/test-insert-direct.yml");

        assertTable(1, table);
    }

    @Test
    public void testInsertDirectCreate() throws Exception
    {
        String table = "TEST1";

        dropTable(table);

        test("/sqlserver/yml/test-insert-direct.yml");

        assertGeneratedTable(table);
    }

    @Test
    public void testInsert() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        test("/sqlserver/yml/test-insert.yml");

        assertTable(1, table);
    }

    @Test
    public void testInsertCreate() throws Exception
    {
        String table = "TEST1";

        dropTable(table);

        test("/sqlserver/yml/test-insert.yml");

        assertGeneratedTable(table);
    }

    @Test
    public void testTruncateInsert() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        test("/sqlserver/yml/test-truncate-insert.yml");

        assertTable(0, table);
    }

    @Test
    public void testReplace() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        test("/sqlserver/yml/test-replace.yml");

        assertGeneratedTable(table);
    }

    @Test
    public void testReplaceLongName() throws Exception
    {
        String table = "TEST___Ａ１２３４５６７８９Ｂ１２３４５６７８９Ｃ１２３４５６７８９Ｄ１２３４５６７８９Ｅ１２３４５６７８９Ｆ１２３４５６７８９Ｇ１２３４５６７８９Ｈ１２３４５６７８９Ｉ１２３４５６７８９Ｊ１２３４５６７８９Ｋ１２３４５６７８９Ｌ１２３４５６７８９";
        assertEquals(127, table.length());

        dropTable(table);
        createTable(table);
        insertRecord(table);

        test("/sqlserver/yml/test-replace-long-name.yml");

        assertGeneratedTable(table);
    }

    @Test
    public void testReplaceCreate() throws Exception
    {
        String table = "TEST1";

        dropTable(table);

        test("/sqlserver/yml/test-replace.yml");

        assertGeneratedTable(table);
    }

    @Test
    public void testStringToTimestamp() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        test("/sqlserver/yml/test-string-timestamp.yml");

        assertTable(1, table, true);
    }

    @Test
    public void testNativeString() throws Exception
    {
        if (!enabled) {
            return;
        }

        String table = "TEST2";

        dropTable(table);
        executeSQL(String.format("CREATE TABLE %S (ITEM1 CHAR(4), ITEM2 VARCHAR(8), ITEM3 TEXT, ITEM4 NCHAR(4), ITEM5 NVARCHAR(8), ITEM6 NTEXT)", table));

        test("/sqlserver/yml/test-native-string.yml");

        List<List<Object>> rows = select(table);
        assertEquals(2, rows.size());
        {
            List<Object> row = rows.get(0);
            assertEquals("A001", row.get(0));
            assertEquals("TEST", row.get(1));
            assertEquals("Ａ", row.get(2));
            assertEquals("あいうえ", row.get(3));
            assertEquals("あいうえおかきく", row.get(4));
            assertEquals("あいうえお", row.get(5));
        }
        {
            List<Object> row = rows.get(1);
            assertEquals("A002", row.get(0));
            assertEquals(null, row.get(1));
            assertEquals(null, row.get(2));
            assertEquals(null, row.get(3));
            assertEquals(null, row.get(4));
            assertEquals(null, row.get(5));
        }
    }

    @Test
    public void testNativeInteger() throws Exception
    {
        if (!enabled) {
            return;
        }

        String table = "TEST3";

        dropTable(table);
        executeSQL(String.format("CREATE TABLE %S (ITEM1 TINYINT, ITEM2 SMALLINT, ITEM3 INT, ITEM4 BIGINT, ITEM5 BIT)", table));

        test("/sqlserver/yml/test-native-integer.yml");

        List<List<Object>> rows = select(table);
        assertEquals(2, rows.size());
        {
            List<Object> row = rows.get(0);
            assertEquals((short)1, row.get(0));
            assertEquals((short)1111, row.get(1));
            assertEquals(11111111, row.get(2));
            assertEquals(111111111111L, row.get(3));
            assertEquals(true, row.get(4));
        }
        {
            List<Object> row = rows.get(1);
            assertEquals((short)2, row.get(0));
            assertEquals(null, row.get(1));
            assertEquals(null, row.get(2));
            assertEquals(null, row.get(3));
            assertEquals(null, row.get(4));
        }
    }

    @Test
    public void testNativeDecimal() throws Exception
    {
        if (!enabled) {
            return;
        }

        String table = "TEST4";

        dropTable(table);
        executeSQL(String.format("CREATE TABLE %S (ITEM1 DECIMAL(20,2), ITEM2 NUMERIC(20,2), ITEM3 SMALLMONEY, ITEM4 MONEY, ITEM5 REAL, ITEM6 FLOAT)", table));

        test("/sqlserver/yml/test-native-decimal.yml");

        List<List<Object>> rows = select(table);
        assertEquals(2, rows.size());
        {
            List<Object> row = rows.get(0);
            assertEquals(new BigDecimal("1.20"), row.get(0));
            assertEquals(new BigDecimal("12345678901234567.89"), row.get(1));
            assertEquals(new BigDecimal("123.4500"), row.get(2));
            assertEquals(new BigDecimal("678.9000"), row.get(3));
            assertEquals(0.01234F, row.get(4));
            assertEquals(0.05678D, row.get(5));
        }
        {
            List<Object> row = rows.get(1);
            assertEquals(new BigDecimal("2.30"), row.get(0));
            assertEquals(null, row.get(1));
            assertEquals(null, row.get(2));
            assertEquals(null, row.get(3));
            assertEquals(null, row.get(4));
            assertEquals(null, row.get(5));
        }
    }

    @Test
    public void testNativeDate() throws Exception
    {
        if (!enabled) {
            return;
        }

        String table = "TEST5";

        dropTable(table);
        executeSQL(String.format("CREATE TABLE %S (ITEM1 DATE, ITEM2 SMALLDATETIME, ITEM3 DATETIME, ITEM4 DATETIME2, ITEM5 DATETIME2(2), ITEM6 TIME, ITEM7 TIME(2))", table));

        test("/sqlserver/yml/test-native-date.yml");

        List<List<Object>> rows = select(table);
        assertEquals(2, rows.size());
        {
            List<Object> row = rows.get(0);
            assertEquals(createDate("2016/01/23"), row.get(0));
            assertEquals(createTimestamp("2016/01/24 11:23:00", 0), row.get(1));
            assertEquals(createTimestamp("2016/01/25 11:22:33", 457000000), row.get(2));
            // Embulk timestamp doesn't support values under microseconds.
            assertEquals(createTimestamp("2016/01/26 11:22:33", 123456000), row.get(3));
            assertEquals(createTimestamp("2016/01/27 11:22:33", 890000000), row.get(4));
            // Embulk timestamp doesn't support values under microseconds.
            assertEquals(createTime("11:22:33", 123456000), row.get(5));
            assertEquals(createTime("11:22:33", 890000000), row.get(6));
        }
        {
            List<Object> row = rows.get(1);
            assertEquals(null, row.get(0));
            assertEquals(null, row.get(1));
            assertEquals(null, row.get(2));
            assertEquals(null, row.get(3));
            assertEquals(null, row.get(4));
            assertEquals(null, row.get(5));
            assertEquals(null, row.get(6));
        }
    }

    @Test
    public void testNative() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        test("/sqlserver/yml/test-native.yml");

        assertTable(1, table);
    }

    @Test
    public void testJtds() throws Exception
    {
        useJtdsDriver = true;
        try {
            connect();
        } catch (Throwable t) {
            System.out.println(t);
            System.out.println("Warning: jTDS driver can't connect to database.");
            System.out.println(String.format(ENGLISH, "(server = %s, port = %d, database = %s, user = %s, password = %s)",
                    getHost(), getPort(), getDatabase(), getUser(), getPassword()));
            return;
        }

        try {
            String table = "TEST1";
            dropTable(table);
            createTable(table);
            insertRecord(table);
            test("/sqlserver/yml/test-jtds.yml");
            assertTableJtds(1, table);
        } finally {
            useJtdsDriver = false;
        }
    }

    @Test
    public void testMerge() throws Exception
    {
        if (!enabled) {
            return;
        }

        String table = "TEST6";

        dropTable(table);
        executeSQL(String.format("CREATE TABLE %S (ITEM1 INT, ITEM2 INT, ITEM3 VARCHAR(4), PRIMARY KEY(ITEM1, ITEM2))", table));
        executeSQL(String.format("INSERT INTO %S VALUES(10, 20, 'A')", table));
        executeSQL(String.format("INSERT INTO %S VALUES(10, 21, 'B')", table));
        executeSQL(String.format("INSERT INTO %S VALUES(11, 20, 'C')", table));

        test("/sqlserver/yml/test-merge.yml");

        List<List<Object>> rows = select(table);
        assertEquals(6, rows.size());
        {
            List<Object> row = rows.get(0);
            assertEquals(Integer.valueOf(10), row.get(0));
            assertEquals(Integer.valueOf(20), row.get(1));
            assertEquals("A", row.get(2));
        }
        {
            List<Object> row = rows.get(1);
            assertEquals(Integer.valueOf(10), row.get(0));
            assertEquals(Integer.valueOf(21), row.get(1));
            assertEquals("aa", row.get(2));
        }
        {
            List<Object> row = rows.get(2);
            assertEquals(Integer.valueOf(10), row.get(0));
            assertEquals(Integer.valueOf(22), row.get(1));
            assertEquals("dd", row.get(2));
        }
        {
            List<Object> row = rows.get(3);
            assertEquals(Integer.valueOf(11), row.get(0));
            assertEquals(Integer.valueOf(10), row.get(1));
            assertEquals("bb", row.get(2));
        }
        {
            List<Object> row = rows.get(4);
            assertEquals(Integer.valueOf(11), row.get(0));
            assertEquals(Integer.valueOf(20), row.get(1));
            assertEquals("C", row.get(2));
        }
        {
            List<Object> row = rows.get(5);
            assertEquals(Integer.valueOf(12), row.get(0));
            assertEquals(Integer.valueOf(20), row.get(1));
            assertEquals("cc", row.get(2));
        }
    }


    private void assertTable(int skip, String table) throws Exception
    {
        assertTable(skip, table, false);
    }

    private void assertTableJtds(int skip, String table) throws Exception
    {
        List<List<Object>> rows = select(table);
        assertEquals(skip + 3, rows.size());
        rows = rows.subList(skip, skip + 3);

        Iterator<List<Object>> i1 = rows.iterator();
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A001", i2.next());
            assertEquals(0, i2.next());
            assertEquals(1234, i2.next());
            assertEquals(123456, i2.next());
            assertEquals(12345678901L, i2.next());
            assertEquals(false, i2.next());
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
            assertEquals("2016-01-01", i2.next());
            assertEquals(createTimestamp("2017/01/01 01:02:03", 123000000), i2.next());
            // Embulk timestamp doesn't support values under microseconds.
            assertEquals("2018-01-01 01:02:03.1234560", i2.next().toString());

            assertEquals(createTimestamp("2019/01/01 01:02:03", 120000000).toString(), i2.next());
            assertEquals(createTimestamp("2020/01/01 01:02:00", 0), i2.next());

            // Embulk timestamp doesn't support values under microseconds.
            assertEquals("03:04:05.1234560", i2.next().toString());
            assertEquals("06:07:08.12", i2.next().toString());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A002", i2.next());
            assertEquals(255, i2.next());
            assertEquals(-32768, i2.next());
            assertEquals(-2147483648, i2.next());
            assertEquals(-9223372036854775808L, i2.next());
            assertEquals(true, i2.next());
            assertEquals(new BigDecimal("-9999999999.99"), i2.next());
            assertEquals(new BigDecimal("-99.999"), i2.next());
            assertEquals(new BigDecimal("-214748.3648"), i2.next());
            assertEquals(new BigDecimal("-922337203685477.5808"), i2.next());
            assertEquals(Float.valueOf(-9999000000F), i2.next());
            assertEquals(Double.valueOf(-999999999999000000D), i2.next());
            // char, varchar, text don't be capable on Unicode chars
            //assertEquals("あい", i2.next());
            i2.next();
            //assertEquals("あいうえ", i2.next());
            i2.next();
            //assertEquals("あいうえお", i2.next());
            i2.next();

            // nchar, nvarcar, ntext
            assertEquals("かき  ", i2.next());
            assertEquals("かきくけ", i2.next());
            assertEquals("かきくけこ", i2.next());

            assertEquals("2016-12-31", i2.next());
            assertEquals(createTimestamp("2017/12/31 23:59:59", 997000000), i2.next());
            // Embulk timestamp doesn't support values under microseconds.
            assertEquals("2018-12-31 23:59:59.9999990", i2.next().toString());
            assertEquals(createTimestamp("2019/12/31 23:59:59", 990000000).toString(), i2.next());
            assertEquals(createTimestamp("2021/01/01 00:00:00", 0), i2.next());
            // Embulk timestamp doesn't support values under microseconds.
            assertEquals("23:59:59.9999990", i2.next().toString());
            assertEquals("23:59:59.99", i2.next().toString());
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

    private void assertTable(int skip, String table, boolean precise) throws Exception
    {
        if (!enabled) {
            return;
        }

        List<List<Object>> rows = select(table);
        assertEquals(skip + 3, rows.size());
        rows = rows.subList(skip, skip + 3);

        Iterator<List<Object>> i1 = rows.iterator();
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A001", i2.next());
            assertEquals((short)0, i2.next());
            assertEquals((short)1234, i2.next());
            assertEquals(123456, i2.next());
            assertEquals(12345678901L, i2.next());
            assertEquals(false, i2.next());
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
            assertEquals((short)255, i2.next());
            assertEquals((short)-32768, i2.next());
            assertEquals(-2147483648, i2.next());
            assertEquals(-9223372036854775808L, i2.next());
            assertEquals(true, i2.next());
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
        if (!enabled) {
            return;
        }

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
        String url;
        if(useJtdsDriver) {
            url = "jdbc:jtds:sqlserver://%s:%d/%s;useLOBs=false";
        } else {
            url = "jdbc:sqlserver://%s:%d;databasename=%s";
        }
        return DriverManager.getConnection(String.format(ENGLISH, url, getHost(), getPort(), getDatabase()), getUser(), getPassword());
    }

}
