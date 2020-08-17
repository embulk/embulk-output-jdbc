package org.embulk.output.db2;

import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static org.embulk.test.EmbulkTests.readResource;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
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
        executeSQL(readResource(sqlName));
//        URL sqlUrl = DB2Tests.class.getResource("/" + sqlName);
//        execute(new File(sqlUrl.toURI()));

    }

    private static void execute(File sqlFile) throws Exception
    {
        Class.forName("com.ibm.db2.jcc.DB2Driver");
        ConfigSource config = baseConfig();
        String host = config.get(String.class, "host");
        String port = config.get(String.class, "port", "50000");
        String user = config.get(String.class, "user");
        String password = config.get(String.class, "password");
        String database = config.get(String.class, "database");

        final ArrayList<String> commandLine = new ArrayList<>();

        // JFYI: We may use the "db2" command instead of "clpplus".
        // https://publib.boulder.ibm.com/tividd/td/ITCM/GC23-4702-01/ja_JA/HTML/CM_PI107.htm
        final String clpplusCommand = "/Users/vietnguyen/Downloads/dsdriver/clpplus/bin/clpplus";
        if (clpplusCommand == null || clpplusCommand.isEmpty()) {
            commandLine.add("clpplus." + (File.separatorChar == '\\' ? "bat" : "sh"));
        } else {
            commandLine.addAll(Arrays.asList(clpplusCommand.split(" ")));
        }
        commandLine.add(user + "/" + password + "@" + host + ":" + port + "/" + database);
        commandLine.add("@" + new File(sqlFile.toURI()).getAbsolutePath());
        System.out.println(commandLine);
        final ProcessBuilder pb = new ProcessBuilder(commandLine);

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
        return executeQuery("SELECT * FROM " + tableName + " ORDER BY ID");
//        Charset charset = Charset.forName("UTF8");
//
//        // cannot use TestingEmbulk.createTempFile because DB2 clpplus cannot read files whose names including ' '.
//        FileSystem fs = FileSystems.getDefault();
//        File tempDir = new File(DB2Tests.class.getResource("/org/embulk/output/db2/test").toURI());
//
//        //Path temp = embulk.createTempFile("txt");
//        Path temp = fs.getPath(new File(tempDir, "temp.txt").getAbsolutePath());
//        Files.deleteIfExists(temp);
//
//        //Path sql = embulk.createTempFile("sql");
//        Path sql = fs.getPath(new File(tempDir, "temp.sql").getAbsolutePath());
//        Files.write(sql,
//                Arrays.asList(
//                        "SET LINESIZE 1000;",
//                        "SPOOL " + temp.toString() + ";",
//                        "SELECT * FROM " + tableName + " ORDER BY ID;",
//                        "EXIT;"),
//                charset);
//
//        execute(sql.toFile());
//
//        List<String> lines = Files.readAllLines(temp, charset);
//        Collections.sort(lines);
//        StringBuilder sb = new StringBuilder();
//        for (String line : lines) {
//            line = line.trim();
//            if (line.isEmpty()) {
//                continue;
//            }
//            sb.append(line);
//            sb.append("\n");
//        }
//        return sb.toString();
    }

    public static Connection connect() throws SQLException
    {
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        ConfigSource config = baseConfig();

        String url = String.format("jdbc:db2://%s:%s/%s",
                config.get(String.class, "host"),
                config.get(String.class, "port", "3306"),
                config.get(String.class, "database"));
        return DriverManager.getConnection(url, config.get(String.class, "user"), config.get(String.class, "password"));
    }

    public static String executeQuery(String sql)
    {
        StringBuilder result = new StringBuilder();
        try (Connection conn = connect();
             PreparedStatement preparedStatement = conn.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery();
        ) {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                result.append(metaData.getColumnName(i)).append(" ");
            }
            result.setLength(result.length() - 1);
            result.append(System.lineSeparator());
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    result.append(resultSet.getString(i)).append(" ");
                }
                result.setLength(result.length() - 1);
                result.append(System.lineSeparator());
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result.toString();
    }

    /**
     * execute sql content file with ; delimiter
     *
     * @param sqlContent
     */
    public static void executeSQL(String sqlContent)
    {
        try (Connection conn = connect();) {
            for (String s : sqlContent.split(";")) {
                if (StringUtils.isBlank(s)) {
                    continue;
                }
                try (PreparedStatement preparedStatement = conn.prepareStatement(s);) {
                    preparedStatement.execute();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static String selectRecords(String tableName, List<String> columnList)
    {
        final String cols = columnList.stream().collect((joining(",")));
        return executeQuery(String.format("SELECT concat(CONCAT_WS(',', %s)) from %s", cols, tableName));
    }
}
