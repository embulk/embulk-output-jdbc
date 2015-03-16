package org.embulk.output.oracle;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.embulk.output.OracleOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.junit.BeforeClass;
import org.junit.Test;

public class OracleOutputPluginTest
{

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
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("You should put Oracle JDBC driver on 'driver' directory.");
        }

        try (Connection connection = connect()) {
            // NOP
        } catch (SQLException e) {
            // NOP
        }
    }


    @Test
    public void testInsert() throws Exception {
        //execute("run", "/yml/test-insert.yml");
        executeSQL(dropTable, true);
        executeSQL(createTable);

        run("/yml/test-insert.yml");

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
            return DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "EMBULK_ORACLE_TEST", "embulk_oracle_test");

        } catch (SQLException e) {
            throw new RuntimeException("You should prepare a schema on Oracle (service = 'XE', schema = 'EMBULK_ORACLE_TEST', password = 'embulk_oracle_test').");
            // for example
            //   CREATE USER EMBULK_ORACLE_TEST IDENTIFIED BY "embulk_oracle_test";
            //   GRANT DBA TO EMBULK_ORACLE_TEST;
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
