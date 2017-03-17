package org.embulk.output.oracle;

import static java.util.Locale.ENGLISH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.Constructor;
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
import java.util.TimeZone;

import org.embulk.exec.PartialExecutionException;
import org.embulk.input.filesplit.LocalFileSplitInputPlugin;
import org.embulk.output.AbstractJdbcOutputPluginTest;
import org.embulk.output.OracleOutputPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.OutputPlugin;
import org.junit.Test;


public class OracleOutputPluginTest extends AbstractJdbcOutputPluginTest
{
    @Override
    protected void prepare() throws SQLException
    {
        tester.addPlugin(OutputPlugin.class, "oracle", OracleOutputPlugin.class);
        tester.addPlugin(InputPlugin.class, "filesplit", LocalFileSplitInputPlugin.class);

        if (System.getProperty("path.separator").equals(";")) {
            // for Windows
            System.setProperty("file.encoding", "MS932");
        }

        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            System.err.println("Warning: you should put 'ojdbc7.jar' in 'embulk-output-oracle/driver' directory in order to test.");
            return;
        }

        try (Connection connection = connect()) {
            String version = connection.getMetaData().getDriverVersion();
            System.out.println("Driver version = " + version);
            enabled = true;

        } catch (SQLException e) {
            System.err.println(e);
            System.err.println(String.format(ENGLISH, "Warning: prepare a schema on Oracle 12c (server = %s, port = %d, database = %s, user = %s, password = %s, charset = UTF-8).",
                    getHost(), getPort(), getDatabase(), getUser(), getPassword()));
            // for example
            //   CREATE USER EMBULK_USER IDENTIFIED BY "embulk_pw";
            //   GRANT DBA TO EMBULK_USER;
        }
    }

    @Test
    public void testInsert() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-insert.yml");

        assertTable(table);
    }

    @Test
    public void testInsertCreate() throws Exception
    {
        String table = "TEST1";

        dropTable(table);

        test("/oracle/yml/test-insert.yml");

        assertGeneratedTable1(table);
    }

    @Test
    public void testInsertEmpty() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        new File(convertPath("/oracle/data/"), "test2").mkdir();
        test("/oracle/yml/test-insert-empty.yml");

        assertTableEmpty(table);
    }

    @Test
    public void testTruncateInsert() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        test("/oracle/yml/test-truncate-insert.yml");

        assertTable(table);
    }

    @Test
    public void testTruncateInsertOCIMethod() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table);

        test("/oracle/yml/test-truncate-insert-oci-method.yml");

        assertTable(table);
    }

    @Test
    public void testTruncateInsertCreate() throws Exception
    {
        String table = "TEST1";

        dropTable(table);

        test("/oracle/yml/test-truncate-insert.yml");

        assertGeneratedTable1(table);
    }

    @Test
    public void testInsertDirect() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-insert-direct.yml");

        assertTable(table);
    }

    @Test
    public void testInsertDirectDuplicate() throws Exception
    {
        if (!enabled) {
            return;
        }

        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table, "A002");

        try {
            test("/oracle/yml/test-insert-direct.yml");
            fail("Exception expected.");
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @Test
    public void testInsertDirectEmpty() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        new File(convertPath("/oracle/data/"), "test2").mkdir();
        test("/oracle/yml/test-insert-direct-empty.yml");

        assertTableEmpty(table);
    }

    @Test
    public void testInsertDirectCreate() throws Exception
    {
        String table = "TEST1";

        dropTable(table);

        test("/oracle/yml/test-insert-direct.yml");

        assertGeneratedTable1(table);
    }

    @Test
    public void testInsertDirectDirectMethod() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        try {
            test("/oracle/yml/test-insert-direct-direct-method.yml");
        } catch (PartialExecutionException e) {
            if (e.getCause() != null && e.getCause().getClass().equals(RuntimeException.class)
                    && e.getCause().getCause() != null && e.getCause().getCause().getClass().equals(AssertionError.class)) {
                // ignore error
                e.printStackTrace();
                System.err.println("The 'direct' mode works if running embulk directly, but fails if using EmbulkPluginTester.");
                return;
            }
            throw e;
        }

        assertTable(table);
    }

    @Test
    public void testInsertDirectOCIMethod() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-insert-direct-oci-method.yml");

        assertTable(table);
    }

    @Test
    public void testInsertDirectOCIMethodLarge() throws Exception
    {
        if (!enabled) {
            return;
        }

        String table = "TEST1";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-insert-direct-oci-method-large.yml");

        List<List<Object>> rows = select(table);
        assertEquals(9999, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertEquals(String.format("%04d", i + 1), rows.get(i).get(0));
        }
    }

    @Test
    public void testInsertDirectOCIMethodDuplicate() throws Exception
    {
        if (!enabled) {
            return;
        }

        String table = "TEST1";

        dropTable(table);
        createTable(table);
        insertRecord(table, "A002");

        try {
            test("/oracle/yml/test-insert-direct-oci-method.yml");
            fail("Exception expected.");
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @Test
    public void testInsertDirectOCIMethodMultibyte() throws Exception
    {
        String table = "ＴＥＳＴ１";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-insert-direct-oci-method-multibyte.yml");

        assertTable(table);
    }

    @Test
    public void testInsertDirectOCIMethodMultibyteDuplicate() throws Exception
    {
        if (!enabled) {
            return;
        }

        String table = "ＴＥＳＴ１";

        dropTable(table);
        createTable(table);
        insertRecord(table, "A002");

        try {
            test("/oracle/yml/test-insert-direct-oci-method-multibyte.yml");
            fail("Exception expected.");
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @Test
    public void testInsertDirectOCIMethodSplit() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-insert-direct-oci-method-split.yml");

        assertTable(table);
    }

    @Test
    public void testMerge() throws Exception
    {
        if (!enabled) {
            return;
        }

        String table = "TEST1";

        dropTable(table);
        executeSQL("CREATE TABLE TEST1 ("
                + "ID              CHAR(4),"
                + "VARCHAR2_ITEM   VARCHAR2(6),"
                + "NUMBER_ITEM     NUMBER(10,2),"
                + "PRIMARY KEY (ID))");
        executeSQL("INSERT INTO TEST1 VALUES('A001', 'AAA', 12.34)");
        executeSQL("INSERT INTO TEST1 VALUES('A003', NULL, NULL)");
        executeSQL("INSERT INTO TEST1 VALUES('A005', 'EEE', 56.78)");
        executeSQL("INSERT INTO TEST1 VALUES('A006', 'FFF', 0)");

        test("/oracle/yml/test-merge.yml");

        List<List<Object>> rows = select(table);
        assertEquals(6, rows.size());
        Iterator<List<Object>> i1 = rows.iterator();
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A001", i2.next());
            assertEquals("aaa", i2.next());
            assertEquals(new BigDecimal("99.99"), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A002", i2.next());
            assertEquals("bbb", i2.next());
            assertEquals(new BigDecimal("88.88"), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A003", i2.next());
            assertEquals("ccc", i2.next());
            assertEquals(new BigDecimal("77.77"), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A004", i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A005", i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A006", i2.next());
            assertEquals("FFF", i2.next());
            assertEquals(new BigDecimal("0"), i2.next());
        }
    }

    @Test
    public void testMergeWithKeys() throws Exception
    {
        if (!enabled) {
            return;
        }

        String table = "TEST1";

        dropTable(table);
        executeSQL("CREATE TABLE TEST1 ("
                + "ID              CHAR(4),"
                + "VARCHAR2_ITEM   VARCHAR2(6),"
                + "NUMBER_ITEM     NUMBER(10,2),"
                + "PRIMARY KEY (ID))");
        executeSQL("INSERT INTO TEST1 VALUES('A001', 'AAA', 11.11)");
        executeSQL("INSERT INTO TEST1 VALUES('A002', 'BBB', 22.22)");
        executeSQL("INSERT INTO TEST1 VALUES('A003', 'CCC', 33.33)");

        test("/oracle/yml/test-merge-keys.yml");

        List<List<Object>> rows = select(table);
        assertEquals(5, rows.size());
        Iterator<List<Object>> i1 = rows.iterator();
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A001", i2.next());
            assertEquals("AAA", i2.next());
            assertEquals(new BigDecimal("11.11"), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A004", i2.next());
            assertEquals("BBB", i2.next());
            assertEquals(new BigDecimal("22.21"), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A005", i2.next());
            assertEquals("BBB", i2.next());
            assertEquals(new BigDecimal("22.22"), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A006", i2.next());
            assertEquals("BBB", i2.next());
            assertEquals(new BigDecimal("22.23"), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A007", i2.next());
            assertEquals("CCC", i2.next());
            assertEquals(new BigDecimal("33.33"), i2.next());
        }
    }

    @Test
    public void testMergeWithRule() throws Exception
    {
        if (!enabled) {
            return;
        }

        String table = "TEST1";

        dropTable(table);
        executeSQL("CREATE TABLE TEST1 ("
                + "ID              CHAR(4),"
                + "VARCHAR2_ITEM   VARCHAR2(6),"
                + "NUMBER_ITEM     NUMBER(10,2),"
                + "PRIMARY KEY (ID))");
        executeSQL("INSERT INTO TEST1 VALUES('A002', 'BBB', 22.22)");
        executeSQL("INSERT INTO TEST1 VALUES('A004', 'DDD', 44.44)");
        executeSQL("INSERT INTO TEST1 VALUES('A006', 'FFF', 66.66)");

        /*
        A001,aaa,99.99
        A002,bbb,88.88
        A003,ccc,77.77
        A004,,
        A005,,
*/

        test("/oracle/yml/test-merge-rule.yml");

        List<List<Object>> rows = select(table);
        assertEquals(6, rows.size());
        Iterator<List<Object>> i1 = rows.iterator();
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A001", i2.next());
            assertEquals("aaa", i2.next());
            assertEquals(new BigDecimal("99.99"), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A002", i2.next());
            assertEquals("x", i2.next());
            assertEquals(new BigDecimal("111.1"), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A003", i2.next());
            assertEquals("ccc", i2.next());
            assertEquals(new BigDecimal("77.77"), i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A004", i2.next());
            assertEquals("x", i2.next());
            assertEquals(null, i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A005", i2.next());
            assertEquals(null, i2.next());
            assertEquals(null, i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A006", i2.next());
            assertEquals("FFF", i2.next());
            assertEquals(new BigDecimal("66.66"), i2.next());
        }
    }

    @Test
    public void testUrl() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-url.yml");

        assertTable(table);
    }

    @Test
    public void testLowerTable() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-lower-table.yml");

        assertTable(table);
    }

    @Test
    public void testLowerColumn() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-lower-column.yml");

        assertTable(table);
    }

    @Test
    public void testLowerColumnOptions() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-lower-column-options.yml");

        assertTable(table);
    }

    @Test
    public void testReplace() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-replace.yml");

        assertGeneratedTable2(table);
    }

    @Test
    public void testReplaceOCIMethod() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-replace-oci-method.yml");

        assertGeneratedTable2(table);
    }

    @Test
    public void testReplaceEmpty() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-replace-empty.yml");

        assertTableEmpty(table);
    }

    @Test
    public void testReplaceCreate() throws Exception
    {
        String table = "TEST1";

        dropTable(table);

        test("/oracle/yml/test-replace.yml");

        assertGeneratedTable2(table);
    }


    @Test
    public void testReplaceLongName() throws Exception
    {
        String table = "TEST12345678901234567890123456";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-replace-long-name.yml");

        assertGeneratedTable2(table);
    }

    @Test
    public void testReplaceLongNameMultibyte() throws Exception
    {
        String table = "ＴＥＳＴ123456789012345678";

        test("/oracle/yml/test-replace-long-name-multibyte.yml");

        assertGeneratedTable2(table);
    }

    @Test
    public void testStringTimestamp() throws Exception
    {
        String table = "TEST1";

        dropTable(table);
        createTable(table);

        test("/oracle/yml/test-string-timestamp.yml");

        assertTable(table);
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
        insertRecord(table, "9999");
    }

    private void insertRecord(String table, String id) throws SQLException
    {
        executeSQL(String.format("INSERT INTO %s VALUES('%s', NULL, NULL, NULL, NULL, NULL, NULL)", table, id));
    }

    private void assertTable(String table) throws Exception
    {
        if (!enabled) {
            return;
        }

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
        if (!enabled) {
            return;
        }

        List<List<Object>> rows = select(table);
        assertEquals(0, rows.size());
    }

    private void assertGeneratedTable1(String table) throws Exception
    {
        if (!enabled) {
            return;
        }

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
        if (!enabled) {
            return;
        }

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

    @Override
    protected Object getValue(ResultSet resultSet, int index) throws SQLException
    {
        if (resultSet.getMetaData().getColumnTypeName(index).equals("CLOB")) {
            return resultSet.getString(index);
        }
        return super.getValue(resultSet, index);
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

    @Override
    protected Connection connect() throws SQLException
    {
        return DriverManager.getConnection(String.format(ENGLISH, "jdbc:oracle:thin:@%s:%d:%s", getHost(), getPort(), getDatabase()),
                getUser(), getPassword());
    }

}
