package org.embulk.output.sqlserver;

import static java.util.Locale.ENGLISH;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;

public class SQLServerTests
{
    public static ConfigSource baseConfig()
    {
        return EmbulkTests.config("EMBULK_OUTPUT_SQLSERVER_TEST_CONFIG");
    }

    public static Connection connect() throws SQLException
    {
        ConfigSource config = baseConfig();

        String user = config.get(String.class, "user");
        String password = config.get(String.class, "password");
        String host = config.get(String.class, "host");
        Integer port = config.get(Integer.class, "port");
        String database = config.get(String.class, "database");

        String url = String.format("jdbc:sqlserver://%s:%d;databaseName=%s", host, port, database);

        return DriverManager.getConnection(url, user, password);
    }

    public static void execute(String sql, String... options)
    {
        ConfigSource config = baseConfig();

        List<String> args = new ArrayList<>(Arrays.asList("sqlcmd",
                "-U",
                config.get(String.class, "user"),
                "-P",
                config.get(String.class, "password"),
                "-H",
                config.get(String.class, "host"),
                "-d",
                config.get(String.class, "database"),
                "-Q",
                sql
                ));

        for (String option : options) {
            args.add((String) option);
        }

        ProcessBuilder pb = new ProcessBuilder(Collections.unmodifiableList(args));
        pb.redirectErrorStream(true);
        int code;
        try {
            Process process = pb.start();
            InputStream inputStream = process.getInputStream();
            byte[] buffer = new byte[8192];
            int readSize;
            while ((readSize = inputStream.read(buffer)) != -1) {
                System.out.write(buffer, 0, readSize);
            }
            code = process.waitFor();
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        if (code != 0) {
            throw new RuntimeException(String.format(ENGLISH,
                    "Command finished with non-zero exit code. Exit code is %d.", code));
        }
    }

    public static String selectRecords(TestingEmbulk embulk, String tableName) throws IOException
    {
        return executeQuery(embulk, "SELECT * FROM " + tableName);
    }

    public static String executeQuery(TestingEmbulk embulk, String query) throws IOException
    {
        Path temp = embulk.createTempFile("txt");
        Files.delete(temp);

        // should not use UTF8 because of BOM
        execute("SET NOCOUNT ON; " + query, "-h", "-1", "-s", ",", "-W", "-f", "932", "-o", temp.toString());

        List<String> lines = Files.readAllLines(temp, Charset.forName("MS932"));
        Collections.sort(lines);
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line);
            sb.append("\n");
        }
        return sb.toString();
    }
}
