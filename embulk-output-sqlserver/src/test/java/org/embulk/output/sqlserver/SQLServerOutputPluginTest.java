package org.embulk.output.sqlserver;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import org.embulk.output.AbstractJdbcOutputPluginTest;
import org.embulk.output.SQLServerOutputPlugin;
import org.embulk.output.tester.EmbulkPluginTester;
import org.embulk.spi.OutputPlugin;
import org.junit.Test;

public class SQLServerOutputPluginTest extends AbstractJdbcOutputPluginTest
{
    private static EmbulkPluginTester tester = new EmbulkPluginTester();
    static {
        tester.addPlugin(OutputPlugin.class, "sqlserver", SQLServerOutputPlugin.class);
    }

    @Test
    public void testInsertDirect() throws Exception
    {
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
        String table = "TEST1";

        dropTable(table);

        tester.run(convertYml("/sqlserver/yml/test-insert-direct.yml"));

        assertGeneratedTable(table);
    }

    private void assertTable(int skip, String table) throws Exception
    {
        // datetime of UTC will be inserted by embulk.
        // datetime of default timezone will be selected by JDBC.
        TimeZone timeZone = TimeZone.getDefault();
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
        }
    }

    private void assertGeneratedTable(String table) throws Exception
    {
        // datetime of UTC will be inserted by embulk.
        // datetime of default timezone will be selected by JDBC.
        TimeZone timeZone = TimeZone.getDefault();
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
        }
    }

    private void createTable(String table) throws SQLException
    {
        String sql = String.format("CREATE TABLE %s ("
                + "ID               CHAR(4),"
                + "TINYINT_ITEM     TINYINT,"
                + "SMALLINT_ITEM    SMALLINT,"
                + "INT_ITEM         INT,"
                + "BIGINT_ITEM      BIGINT,"
                + "BIT_ITEM         BIT,"
                + "DECIMAL_ITEM     DECIMAL(12,2),"
                + "NUMERIC_ITEM     NUMERIC(5,3),"
                + "SMALLMONEY_ITEM  SMALLMONEY,"
                + "MONEY_ITEM       MONEY,"
                + "REAL_ITEM        REAL,"
                + "FLOAT_ITEM       FLOAT,"
                + "CHAR_ITEM        CHAR(4),"
                + "VARCHAR_ITEM     VARCHAR(8),"
                + "TEXT_ITEM        TEXT,"
                + "NCHAR_ITEM       NCHAR(4),"
                + "NVARCHAR_ITEM    NVARCHAR(8),"
                + "NTEXT_ITEM       NTEXT,"
                + "PRIMARY KEY (ID))", table);
        executeSQL(sql);
    }

    private void insertRecord(String table) throws SQLException
    {
        executeSQL(String.format("INSERT INTO %s VALUES('9999', "
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
