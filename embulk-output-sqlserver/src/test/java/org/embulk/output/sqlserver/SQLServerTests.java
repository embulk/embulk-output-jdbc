package org.embulk.output.sqlserver;

import static java.util.Locale.ENGLISH;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;

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

        final ArrayList<String> args = new ArrayList<>();

        final String sqlcmdCommand = System.getenv("EMBULK_OUTPUT_SQLSERVER_TEST_SQLCMD_COMMAND");
        if (sqlcmdCommand == null || sqlcmdCommand.isEmpty()) {
            args.add("sqlcmd");
        } else {
            args.addAll(Arrays.asList(sqlcmdCommand.split(" ")));
        }

        args.add("-U");
        args.add(config.get(String.class, "user"));
        args.add("-P");
        args.add(config.get(String.class, "password"));
        args.add("-S");
        args.add(config.get(String.class, "host"));
        args.add("-d");
        args.add(config.get(String.class, "database"));
        args.add("-Q");
        args.add(sql);
        for (String option : options) {
            args.add(option);
        }

        ProcessBuilder pb = new ProcessBuilder(args);
        pb.redirectErrorStream(true);
        int code;
        try {
            Process process = pb.start();
            ByteStreams.copy(process.getInputStream(), System.out);
            code = process.waitFor();
        } catch (IOException | InterruptedException ex) {
            throw Throwables.propagate(ex);
        }
        if (code != 0) {
            throw new RuntimeException(String.format(ENGLISH,
                    "Command finished with non-zero exit code. Exit code is %d.", code));
        }
    }

    public static String selectRecords(TestingEmbulk embulk, String tableName) throws Exception
    {
        return executeQuery(embulk, "SELECT * FROM " + tableName);
    }

    public static String executeQuery(TestingEmbulk embulk, String query) throws Exception
    {
        // cannot use TestingEmbulk.createTempFile because sqlcmd cannot read files whose names including ' '.
        FileSystem fs = FileSystems.getDefault();
        File tempDir = new File(SQLServerTests.class.getResource("/org/embulk/output/sqlserver/test").toURI());

        //Path temp = embulk.createTempFile("txt");
        Path temp = fs.getPath(new File(tempDir, "temp.txt").getAbsolutePath());
        Files.deleteIfExists(temp);

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
