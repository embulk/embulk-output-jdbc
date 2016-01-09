package org.embulk.output.sqlserver;

import static org.junit.Assert.assertEquals;

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

    @Override
    protected Connection connect() throws SQLException
    {
        return DriverManager.getConnection("jdbc:sqlserver://localhost\\SQLEXPRESS:1433;databasename=TESTDB", "TEST_USER", "test_pw");
    }

}
