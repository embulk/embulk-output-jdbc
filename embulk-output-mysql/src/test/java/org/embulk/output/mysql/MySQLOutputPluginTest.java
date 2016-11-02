package org.embulk.output.mysql;

import static java.util.Locale.ENGLISH;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.embulk.output.AbstractJdbcOutputPluginTest;
import org.embulk.output.MySQLOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.junit.Test;


public class MySQLOutputPluginTest extends AbstractJdbcOutputPluginTest
{
    @Override
    protected void prepare() throws SQLException
    {
        tester.addPlugin(OutputPlugin.class, "mysql", MySQLOutputPlugin.class);

        try {
            connect();
        } catch (SQLException e) {
            System.err.println(e);
            System.err.println(String.format(ENGLISH, "Warning: prepare a schema on MySQL (server = %s, port = %d, database = %s, user = %s, password = %s).",
                    getHost(), getPort(), getDatabase(), getUser(), getPassword()));
            return;
        }

        enabled = true;
    }

    @Test
    public void testInsertDirectAfterLoad() throws Exception
    {
        String table = "test1";

        dropTable(table);
        createTable(table);

        test("/mysql/yml/test-insert-direct-after-load.yml");

        List<List<Object>> rows = select(table);
        assertEquals(3, rows.size());
        Iterator<List<Object>> i1 = rows.iterator();
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A001", i2.next());
            assertEquals(9, i2.next());
            assertEquals("x", i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A002", i2.next());
            assertEquals(0, i2.next());
            assertEquals("b", i2.next());
        }
        {
            Iterator<Object> i2 = i1.next().iterator();
            assertEquals("A003", i2.next());
            assertEquals(9, i2.next());
            assertEquals("x", i2.next());
        }
    }

    private void createTable(String table) throws SQLException
    {
        String sql = String.format("create table %s ("
                + "id              char(4),"
                + "int_item        int,"
                + "varchar_item    varchar(8),"
                + "primary key (id))", table);
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

    @Override
    protected Connection connect() throws SQLException
    {
        return DriverManager.getConnection(String.format(ENGLISH, "jdbc:mysql://%s:%d/%s", getHost(), getPort(), getDatabase()),
                getUser(), getPassword());
    }

}
