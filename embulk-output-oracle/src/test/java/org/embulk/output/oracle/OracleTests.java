package org.embulk.output.oracle;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;

import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Locale.ENGLISH;

public class OracleTests
{
    public static ConfigSource baseConfig()
    {
        return EmbulkTests.config("EMBULK_OUTPUT_ORACLE_TEST_CONFIG");
    }

    public static void execute(TestingEmbulk embulk, String sql) throws IOException
    {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("You should put 'ojdbc7.jar' in 'embulk-output-oracle/driver' directory in order to test.");
        }

        Path sqlFile = embulk.createTempFile("sql");
        Files.write(sqlFile, Arrays.asList(sql), Charset.forName("UTF8"));

        ConfigSource config = baseConfig();
        String host = config.get(String.class, "host");
        String port = config.get(String.class, "port", "1521");
        String user = config.get(String.class, "user");
        String password = config.get(String.class, "password");
        String database = config.get(String.class, "database");

        ProcessBuilder pb = new ProcessBuilder(
                "sql",
                user + "/" + password + "@" + host + ":" + port + "/" + database,
                "@" + sqlFile.toFile().getAbsolutePath());
        pb.environment().put("JAVA_TOOL_OPTIONS", "-Dfile.encoding=UTF8");
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

    public static String selectRecords(TestingEmbulk embulk, String tableName, Path inputPath) throws IOException
    {
        Path temp = embulk.createTempFile("txt");
        Files.delete(temp);

        StringBuilder sql = new StringBuilder();
        sql.append("SPOOL " + temp.toString());
        sql.append(System.lineSeparator());
        sql.append("SET HEADING OFF");
        sql.append(System.lineSeparator());
        sql.append("SET PAGESIZE 0");
        sql.append(System.lineSeparator());
        sql.append("SET LINESIZE 1000");
        sql.append(System.lineSeparator());
        sql.append("SET COLSEP ','");
        sql.append(System.lineSeparator());
        sql.append("SET TRIMSPOOL ON");
        sql.append(System.lineSeparator());
        sql.append("SELECT * FROM " + tableName + ";");
        sql.append(System.lineSeparator());
        sql.append("EXIT;");
        execute(embulk, sql.toString());

        return EmbulkTests.readSortedFile(temp);
/*
        List<String> lines = Files.readAllLines(temp, Charset.forName("MS932"));
        Collections.sort(lines);
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line);
            sb.append("\n");
        }
        return sb.toString();*/
    }
}
