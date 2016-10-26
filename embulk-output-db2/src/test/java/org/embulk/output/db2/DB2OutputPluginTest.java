package org.embulk.output.db2;

import static java.util.Locale.ENGLISH;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.embulk.output.AbstractJdbcOutputPluginTest;
import org.embulk.output.DB2OutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.junit.Test;


public class DB2OutputPluginTest extends AbstractJdbcOutputPluginTest
{
    @Override
    protected void prepare() throws SQLException {
        tester.addPlugin(OutputPlugin.class, "db2", DB2OutputPlugin.class);

        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Warning: you should put 'db2jcc4.jar' in 'embulk-input-db2/driver' directory in order to test.");
            return;
        }

        try {
            connect();
        } catch (SQLException e) {
            System.err.println(e);
            System.err.println(String.format(ENGLISH, "Warning: prepare a schema on DB2 (server = %s, port = %d, database = %s, user = %s, password = %s).",
                    getHost(), getPort(), getDatabase(), getUser(), getPassword()));
            return;
        }

        enabled = true;
    }

    @Test
    public void testInsertDirectNumber() throws Exception
    {
        String table = "TEST_NUMBER";

        dropTable(table);
        createNumberTable(table);

        test("/db2/yml/test-insert-direct-number.yml");

        assertNumberTable(table);
    }

    @Test
    public void testInsertDirectChar() throws Exception
    {
        String table = "TEST_CHAR";

        dropTable(table);
        createCharTable(table);

        test("/db2/yml/test-insert-direct-char.yml");

        assertCharTable(table, false);
    }

    @Test
    public void testInsertDirectDateTime() throws Exception
    {
        String table = "TEST_DATETIME";

        dropTable(table);
        createDateTimeTable(table);

        test("/db2/yml/test-insert-direct-datetime.yml");

        assertDateTimeTable(table);
    }

    @Test
    public void testInsertNumber() throws Exception
    {
        String table = "TEST_NUMBER";

        dropTable(table);
        createNumberTable(table);

        test("/db2/yml/test-insert-number.yml");

        assertNumberTable(table);
    }

    @Test
    public void testInsertChar() throws Exception
    {
        String table = "TEST_CHAR";

        dropTable(table);
        createCharTable(table);

        test("/db2/yml/test-insert-char.yml");

        assertCharTable(table, false);
    }

    @Test
    public void testInsertDateTime() throws Exception
    {
        String table = "TEST_DATETIME";

        dropTable(table);
        createDateTimeTable(table);

        test("/db2/yml/test-insert-datetime.yml");

        assertDateTimeTable(table);
    }

    @Test
    public void testInsertCreateNumber() throws Exception
    {
        String table = "TEST_NUMBER";

        dropTable(table);

        test("/db2/yml/test-insert-number.yml");

        List<List<Object>> rows = select(table);
        assertEquals(3, rows.size());
        {
            List<Object> row = rows.get(0);
            assertEquals("A001", row.get(0));
            assertEquals(12345L, row.get(1));
            assertEquals(123456789L, row.get(2));
            assertEquals(123456789012L, row.get(3));
            assertEquals("123456.78", row.get(4));
            assertEquals("876543.21", row.get(5));
            assertEquals(1.23456D, row.get(6));
            assertEquals(1.23456789012D, row.get(7));
            assertEquals(3.45678901234D, row.get(8));
        }
        {
            List<Object> row = rows.get(1);
            assertEquals("A002", row.get(0));
            assertEquals(-9999L, row.get(1));
            assertEquals(-999999999L, row.get(2));
            assertEquals(-999999999999L, row.get(3));
            assertEquals("-999999.99", row.get(4));
            assertEquals("-999999.99", row.get(5));
            assertEquals(-9.999999D, row.get(6));
            assertEquals(-9.999999D, row.get(7));
            assertEquals(-9.99999999999999D, row.get(8));
        }
        {
            List<Object> row = rows.get(2);
            assertEquals("A003", row.get(0));
            assertEquals(null, row.get(1));
            assertEquals(null, row.get(2));
            assertEquals(null, row.get(3));
            assertEquals(null, row.get(4));
            assertEquals(null, row.get(5));
            assertEquals(null, row.get(6));
            assertEquals(null, row.get(7));
            assertEquals(null, row.get(8));
        }
    }

    @Test
    public void testInsertCreateChar() throws Exception
    {
        String table = "TEST_CHAR";

        dropTable(table);

        test("/db2/yml/test-insert-char.yml");

        assertCharTable(table, true, 0);
    }

    @Test
    public void testInsertCreateDateTime() throws Exception
    {
        String table = "TEST_DATETIME";

        dropTable(table);

        test("/db2/yml/test-insert-datetime.yml");

        assertDateTimeTable(table, 0);
    }

    @Test
    public void testTruncateInsert() throws Exception
    {
        String table = "TEST_NUMBER";

        dropTable(table);
        createNumberTable(table);

        test("/db2/yml/test-truncate-insert.yml");

        assertNumberTable(table, 0);
    }

    @Test
    public void testReplace() throws Exception
    {
        String table = "TEST_CHAR";

        dropTable(table);
        createCharTable(table);

        test("/db2/yml/test-replace.yml");

        assertCharTable(table, true, 0);
    }

    @Test
    public void testReplaceCreate() throws Exception
    {
        String table = "TEST_CHAR";

        dropTable(table);

        test("/db2/yml/test-replace.yml");

        assertCharTable(table, true, 0);
    }


    @Override
    protected Object getValue(ResultSet resultSet, int index) throws SQLException {
        Object value = super.getValue(resultSet, index);
        if (value instanceof Clob) {
            return resultSet.getString(index);
        }
        return value;
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

    private Time createTime(String s) throws ParseException
    {
        DateFormat format = new SimpleDateFormat("HH:mm:ss");
        Date date = format.parse(s);
        return new Time(date.getTime());
    }

    private void createNumberTable(String table) throws SQLException
    {
        String sql = String.format("CREATE TABLE %s ("
                + "ID               CHAR(4) NOT NULL,"
                + "SMALLINT_ITEM    SMALLINT,"
                + "INTEGER_ITEM     INTEGER,"
                + "BIGINT_ITEM      BIGINT,"
                + "DECIMAL_ITEM     DECIMAL(8,2),"
                + "NUMERIC_ITEM     NUMERIC(8,2),"
                + "REAL_ITEM        REAL,"
                + "DOUBLE_ITEM      DOUBLE,"
                + "FLOAT_ITEM       FLOAT,"
                + "PRIMARY KEY (ID))", table);
        executeSQL(sql);

        executeSQL(String.format("INSERT INTO %s VALUES('9999',"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL)", table));
    }

    private void createCharTable(String table) throws SQLException
    {
        String sql = String.format("CREATE TABLE %s ("
                + "ID               CHAR(4) NOT NULL,"
                + "CHAR_ITEM        CHAR(4),"
                + "VARCHAR_ITEM     VARCHAR(8),"
                + "CLOB_ITEM        CLOB,"
                + "GRAPHIC_ITEM     GRAPHIC(4),"
                + "VARGRAPHIC_ITEM  VARGRAPHIC(8),"
                + "NCHAR_ITEM       NCHAR(4),"
                + "NVARCHAR_ITEM    NVARCHAR(8),"
                + "NCLOB_ITEM       NCLOB,"
                + "PRIMARY KEY (ID))", table);
        executeSQL(sql);

        executeSQL(String.format("INSERT INTO %s VALUES('9999',"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL)", table));
    }

    private void createDateTimeTable(String table) throws SQLException
    {
        String sql = String.format("CREATE TABLE %s ("
                + "ID               CHAR(4) NOT NULL,"
                + "DATE_ITEM        DATE,"
                + "TIME_ITEM        TIME,"
                + "TIMESTAMP_ITEM   TIMESTAMP,"
                + "TIMESTAMP0_ITEM  TIMESTAMP(0),"
                + "TIMESTAMP12_ITEM TIMESTAMP(12),"
                + "PRIMARY KEY (ID))", table);
        executeSQL(sql);

        executeSQL(String.format("INSERT INTO %s VALUES('9999',"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL,"
                + "NULL)", table));
    }

    private void assertNumberTable(String table) throws SQLException
    {
        assertNumberTable(table, 1);
    }

    private void assertNumberTable(String table, int skip) throws SQLException
    {
        List<List<Object>> rows = select(table);
        assertEquals(skip + 3, rows.size());
        {
            List<Object> row = rows.get(skip + 0);
            assertEquals("A001", row.get(0));
            assertEquals(12345, row.get(1));
            assertEquals(123456789, row.get(2));
            assertEquals(123456789012L, row.get(3));
            assertEquals(new BigDecimal("123456.78"), row.get(4));
            assertEquals(new BigDecimal("876543.21"), row.get(5));
            assertEquals(1.23456F, row.get(6));
            assertEquals(1.23456789012D, row.get(7));
            assertEquals(3.45678901234D, row.get(8));
        }
        {
            List<Object> row = rows.get(skip + 1);
            assertEquals("A002", row.get(0));
            assertEquals(-9999, row.get(1));
            assertEquals(-999999999, row.get(2));
            assertEquals(-999999999999L, row.get(3));
            assertEquals(new BigDecimal("-999999.99"), row.get(4));
            assertEquals(new BigDecimal("-999999.99"), row.get(5));
            assertEquals(-9.999999F, row.get(6));
            assertEquals(-9.999999D, row.get(7));
            assertEquals(-9.99999999999999D, row.get(8));
        }
        {
            List<Object> row = rows.get(skip + 2);
            assertEquals("A003", row.get(0));
            assertEquals(null, row.get(1));
            assertEquals(null, row.get(2));
            assertEquals(null, row.get(3));
            assertEquals(null, row.get(4));
            assertEquals(null, row.get(5));
            assertEquals(null, row.get(6));
            assertEquals(null, row.get(7));
            assertEquals(null, row.get(8));
        }
    }

    private void assertCharTable(String table, boolean trimming) throws SQLException
    {
        assertCharTable(table, trimming, 1);
    }

    private void assertCharTable(String table, boolean trimming, int skip) throws SQLException
    {
        List<List<Object>> rows = select(table);
        assertEquals(skip + 3, rows.size());
        {
            List<Object> row = rows.get(skip + 0);
            assertEquals("A001", row.get(0));
            assertEquals(trimming ? "aa" : "aa  ", row.get(1));
            assertEquals("AA", row.get(2));
            assertEquals("aaaaaaaaaaaa", row.get(3));
            assertEquals(trimming? "ああ" : "ああ  ", row.get(4));
            assertEquals("いいいい", row.get(5));
            assertEquals(trimming? "ａａ" : "ａａ  ", row.get(6));
            assertEquals("ＡＡ", row.get(7));
            assertEquals("ａａａａａａａａ", row.get(8));
        }
        {
            List<Object> row = rows.get(skip + 1);
            assertEquals("A002", row.get(0));
            assertEquals("XXXX", row.get(1));
            assertEquals("XXXXXXXX", row.get(2));
            assertEquals("XXXXXXXXXXXXXXXX", row.get(3));
            assertEquals("XXXX", row.get(4));
            assertEquals("XXXXXXXX", row.get(5));
            assertEquals("XXXX", row.get(6));
            assertEquals("XXXXXXXX", row.get(7));
            assertEquals("XXXXXXXXXXXXXXXX", row.get(8));
        }
        {
            List<Object> row = rows.get(skip + 2);
            assertEquals("A003", row.get(0));
            assertEquals(null, row.get(1));
            assertEquals(null, row.get(2));
            assertEquals(null, row.get(3));
            assertEquals(null, row.get(4));
            assertEquals(null, row.get(5));
            assertEquals(null, row.get(6));
            assertEquals(null, row.get(7));
            assertEquals(null, row.get(8));
        }
    }

    private void assertDateTimeTable(String table) throws SQLException, ParseException
    {
        assertDateTimeTable(table, 1);
    }

    private void assertDateTimeTable(String table, int skip) throws SQLException, ParseException
    {
        List<List<Object>> rows = select(table);
        assertEquals(skip + 3, rows.size());
        {
            List<Object> row = rows.get(skip + 0);
            assertEquals("A001", row.get(0));
            assertEquals(createDate("2016/09/08"), row.get(1));
            assertEquals(createTime("12:34:45"), row.get(2));
            assertEquals(createTimestamp("2016/09/09 12:34:45", 123456000), row.get(3));
            assertEquals(createTimestamp("2016/09/10 12:34:45", 0), row.get(4));
            // Embulk TimestampParser cannot parse values under microseconds.
            assertEquals(createTimestamp("2016/09/11 12:34:45", 123456000), row.get(5));
        }
        {
            List<Object> row = rows.get(skip + 1);
            assertEquals("A002", row.get(0));
            assertEquals(createDate("2016/12/31"), row.get(1));
            assertEquals(createTime("23:59:59"), row.get(2));
            assertEquals(createTimestamp("2016/12/31 23:59:59", 999999000), row.get(3));
            assertEquals(createTimestamp("2016/12/31 23:59:59", 0), row.get(4));
            assertEquals(createTimestamp("2016/12/31 23:59:59", 999999000), row.get(5));
        }
        {
            List<Object> row = rows.get(skip + 2);
            assertEquals("A003", row.get(0));
            assertEquals(null, row.get(1));
            assertEquals(null, row.get(2));
            assertEquals(null, row.get(3));
            assertEquals(null, row.get(4));
            assertEquals(null, row.get(5));
        }
    }

    @Override
    protected Connection connect() throws SQLException
    {
        return DriverManager.getConnection(String.format(ENGLISH, "jdbc:db2://%s:%d/%s", getHost(), getPort(), getDatabase()),
                getUser(), getPassword());
    }

}
