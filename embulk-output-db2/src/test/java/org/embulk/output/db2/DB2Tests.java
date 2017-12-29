package org.embulk.output.db2;

import static java.util.Locale.ENGLISH;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;

public class DB2Tests
{
    public static ConfigSource baseConfig()
    {
        return EmbulkTests.config("EMBULK_OUTPUT_DB2_TEST_CONFIG");
    }

    public static void execute(String sqlName) throws Exception
    {
        // DB2Tests.excute takes a resource name of SQL file, doesn't take a SQL sentence as other XXXTests do.
        // Because TestingEmbulk.createTempFile might create a file whose name contains ' ' and DB2 clpplus cannot read such a file.
        // But if root directory name of embulk-input-db2 contains ' ', tests will fail for the same reason.
        URL sqlUrl = DB2Tests.class.getResource("/" + sqlName);
        execute(new File(sqlUrl.toURI()));
    }

    private static void execute(File sqlFile) throws Exception
    {
        ConfigSource config = baseConfig();
        String host = config.get(String.class, "host");
        String port = config.get(String.class, "port", "50000");
        String user = config.get(String.class, "user");
        String password = config.get(String.class, "password");
        String database = config.get(String.class, "database");

        boolean isWindows = File.separatorChar == '\\';
        ProcessBuilder pb = new ProcessBuilder(
                "clpplus." + (isWindows ? "bat" : "sh"),
                user + "/" + password + "@" + host + ":" + port + "/" + database,
                "@" + sqlFile.getAbsolutePath());
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
        Charset charset = Charset.forName("UTF8");

        // cannot use TestingEmbulk.createTempFile because DB2 clpplus cannot read files whose names including ' '.
        FileSystem fs = FileSystems.getDefault();
        File tempDir = new File(DB2Tests.class.getResource("/org/embulk/output/db2/test").toURI());

        //Path temp = embulk.createTempFile("txt");
        Path temp = fs.getPath(new File(tempDir, "temp.txt").getAbsolutePath());
        Files.deleteIfExists(temp);

        //Path sql = embulk.createTempFile("sql");
        Path sql = fs.getPath(new File(tempDir, "temp.sql").getAbsolutePath());
        Files.write(sql,
                Arrays.asList(
                        "SET LINESIZE 1000;",
                        "SPOOL " + temp.toString() + ";",
                        "SELECT * FROM " + tableName + " ORDER BY ID;",
                        "EXIT;"),
                charset);

        execute(sql.toFile());

        List<String> lines = Files.readAllLines(temp, charset);
        Collections.sort(lines);
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            sb.append(line);
            sb.append("\n");
        }
        return sb.toString();
    }
}
