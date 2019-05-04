package org.embulk.output.mysql;

import static org.embulk.output.mysql.MySQLTests.execute;
import static org.embulk.output.mysql.MySQLTests.selectRecords;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.output.MySQLOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.io.Resources;

public class RetryTest
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/output/mysql/test/expect/retry/";

    private static ConfigSource loadYamlResource(TestingEmbulk embulk, String fileName)
    {
        return embulk.loadYamlResource(BASIC_RESOURCE_PATH + fileName);
    }

    private static String readResource(String fileName)
    {
        return EmbulkTests.readResource(BASIC_RESOURCE_PATH + fileName);
    }

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(OutputPlugin.class, "mysql", MySQLOutputPlugin.class)
            .build();

    private ConfigSource baseConfig;

    @Before
    public void setup()
    {
        baseConfig = MySQLTests.baseConfig();
        execute(readResource("setup.sql")); // setup rows
    }

    @Test
    public void testSucceeded() throws Exception
    {
        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test1.yml")), in1);
        assertThat(selectRecords(embulk, "test1"), is(readResource("test1_expected.csv")));
    }

    @Test
    public void testRetry() throws Exception
    {
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    try (Connection conn = MySQLTests.connect()) {
                        conn.setAutoCommit(false);
                        try (Statement statement = conn.createStatement()) {
                            // make the transaction larger so that embulk-output-mysql transaction will be rolled back at a deadlock.
                            for (int i = 100; i < 110; i++) {
                                statement.execute("insert into test1 values('B" + i + "', 0)");
                            }

                            statement.execute("insert into test1 values('A003', 0)");
                            Thread.sleep(3000);
                            // deadlock will occur
                            statement.execute("insert into test1 values('A002', 0)");
                            conn.rollback();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test1.yml")), in1);
        assertThat(selectRecords(embulk, "test1"), is(readResource("test1_expected.csv")));
    }

    // will be flushed multiple times
    @Test
    public void testRetryLarge() throws Exception
    {
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    try (Connection conn = MySQLTests.connect()) {
                        conn.setAutoCommit(false);
                        try (Statement statement = conn.createStatement()) {
                            // make the transaction larger so that embulk-output-mysql transaction will be rolled back at a deadlock.
                            for (int i = 100; i < 200; i++) {
                                statement.execute("insert into test1 values('B" + i + "', 0)");
                            }

                            statement.execute("insert into test1 values('A170', 0)");
                            Thread.sleep(3000);
                            // deadlock will occur
                            statement.execute("insert into test1 values('A160', 0)");
                            conn.rollback();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();

        Path in1 = toPath("test1_large.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test1_large.yml")), in1);
        assertThat(selectRecords(embulk, "test1"), is(readResource("test1_large_expected.csv")));
    }

    private Path toPath(String fileName) throws URISyntaxException
    {
        URL url = Resources.getResource(BASIC_RESOURCE_PATH + fileName);
        return FileSystems.getDefault().getPath(new File(url.toURI()).getAbsolutePath());
    }

}
