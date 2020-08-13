package org.embulk.output.postgresql;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import jnr.ffi.Platform;
import jnr.ffi.Platform.OS;
import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Locale.ENGLISH;
import static org.embulk.test.EmbulkTests.readSortedFile;

public class PostgreSQLTests
{
    public static ConfigSource baseConfig()
    {
        return EmbulkTests.config("EMBULK_OUTPUT_POSTGRESQL_TEST_CONFIG");
    }

    public static void execute(String sql)
    {
        ConfigSource config = baseConfig();
        ProcessBuilder pb = new ProcessBuilder(
                "psql", "-w",
                "--set", "ON_ERROR_STOP=1",
                "--host", config.get(String.class, "host"),
                "--username", config.get(String.class, "user"),
                "--dbname", config.get(String.class, "database"),
                "-c", convert(sql));
        pb.environment().put("PGPASSWORD", config.get(String.class, "password"));
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

    private static String convert(String sql)
    {
        if (Platform.getNativePlatform().getOS().equals(OS.WINDOWS)) {
            // '"' should be '\"' and '\' should be '\\' in Windows
            return sql.replace("\\\\", "\\").replace("\\", "\\\\").replace("\"", "\\\"");
        }
        return sql;
    }

    public static String selectRecords(TestingEmbulk embulk, String tableName) throws IOException
    {
        Path temp = embulk.createTempFile("txt");
        Files.delete(temp);
        execute("\\copy " + tableName + " to '" + temp.toString().replace("\\", "\\\\") + "' delimiter ','");
        return readSortedFile(temp);
    }
}
