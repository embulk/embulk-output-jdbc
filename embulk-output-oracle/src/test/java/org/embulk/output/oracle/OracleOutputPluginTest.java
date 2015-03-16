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

public class OracleOutputPluginTest extends EmbulkPluginTester
{

    public OracleOutputPluginTest()
    {
        super(OutputPlugin.class, "oracle", OracleOutputPlugin.class);

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

    public void executeSQL(String sql) throws SQLException
    {
        executeSQL(sql, false);
    }

    public void executeSQL(String sql, boolean ignoreError) throws SQLException
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

    private Connection connect()
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


    @Override
    public void run(String ymlName) throws Exception
    {
        super.run(convertYml(ymlName));
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
