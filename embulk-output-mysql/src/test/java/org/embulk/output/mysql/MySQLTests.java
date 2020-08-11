package org.embulk.output.mysql;

import static java.util.Locale.ENGLISH;
import static org.embulk.test.EmbulkTests.readSortedFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;

public class MySQLTests
{
    public static ConfigSource baseConfig()
    {
        return EmbulkTests.config("EMBULK_OUTPUT_MYSQL_TEST_CONFIG");
    }

    public static Connection connect() throws SQLException
    {
        ConfigSource config = baseConfig();

        String url = String.format("jdbc:mysql://%s:%s/%s",
                config.get(String.class, "host"),
                config.get(String.class, "port", "3306"),
                config.get(String.class, "database"));
        return DriverManager.getConnection(url, config.get(String.class, "user"), config.get(String.class, "password"));
    }

    public static void execute(String sql)
    {
        ConfigSource config = baseConfig();

        ImmutableList.Builder<String> args = ImmutableList.builder();
        args.add("mysql")
                .add("-u" + config.get(String.class, "user"));
        if (StringUtils.isNotBlank(config.get(String.class, "password"))) {
            args.add("-p" + config.get(String.class, "password"));
        }
        args
                .add("-h" + config.get(String.class, "host"))
                .add("-P" + config.get(String.class, "port", "3306"))
                .add("-D" + config.get(String.class, "database"))
                .add("-e")
                .add(sql);

        ProcessBuilder pb = new ProcessBuilder(args.build());
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

    public static String selectRecords(TestingEmbulk embulk, String tableName) throws IOException
    {
        Path temp = embulk.createTempFile("txt");
        Files.delete(temp);
        // test user needs FILE privilege
        execute("select * from " + tableName + " into outfile '" + temp.toString().replace("\\", "\\\\") + "' fields terminated by ','");
        return readSortedFile(temp);
    }
}
