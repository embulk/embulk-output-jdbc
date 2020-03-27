package org.embulk.output.mysql;

import static org.embulk.output.mysql.MySQLTests.execute;
import static org.embulk.output.mysql.MySQLTests.selectRecords;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
    private static class Lock
    {
        public boolean enabled = true;
    }

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
    public void testRetry_FlushedMultipleTimes() throws Exception
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

        Path in1 = toPath("test1_flushed_multiple_times.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test1_flushed_multiple_times.yml")), in1);
        assertThat(selectRecords(embulk, "test1"), is(readResource("test1_flushed_multiple_times_expected.csv")));
    }

    // records will be partially committed
    @Test
    public void testRetry_Large() throws Exception
    {
        // canonicalize path because LocalFileInputPlugin can't read Windows short file name.
        Path in1 = embulk.createTempFile("csv").toRealPath();
        StringBuilder expected1 = new StringBuilder();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(in1.toFile()))) {
            writer.write("id:string,num:long");
            writer.newLine();

            for (int i = 1000000; i < 1250000; i++) {
                writer.write("A" + i + "," + i);
                writer.newLine();

                expected1.append("A" + i + "," + i + "\n");
            }
        }

        final Lock lock = new Lock();

        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    try (Connection conn = MySQLTests.connect()) {
                        conn.setAutoCommit(false);
                        try (Statement statement = conn.createStatement()) {
                            // make the transaction larger so that embulk-output-mysql transaction will be rolled back at a deadlock.
                            for (int i = 1000000; i < 1260000; i++) {
                                statement.execute("insert into test1 values('B" + i + "', 0)");
                            }
                            System.out.println("# Inserted many records.");

                            synchronized (lock) {
                                lock.notify();
                                lock.enabled = false;
                            }

                            System.out.println("# Insert 'A1249000'.");
                            statement.execute("insert into test1 values('A1249010', 0)");
                            Thread.sleep(5000);
                            // deadlock will occur
                            System.out.println("# Insert 'A1249010'.");
                            statement.execute("insert into test1 values('A1249000', 0)");
                            conn.rollback();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();

        synchronized (lock) {
            if (lock.enabled) {
                lock.wait();
            }
        }

        //Path in1 = toPath("test1_flushed_multiple_times.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test1.yml")), in1);
        assertThat(selectRecords(embulk, "test1"), is(expected1.toString()));
    }

    @Test
    public void testRetry_MultipleTimes() throws Exception
    {
        Thread thread1 = new Thread() {
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
        thread1.start();

        Thread thread2 = new Thread() {
            @Override
            public void run() {
                try {
                    try (Connection conn = MySQLTests.connect()) {
                        conn.setAutoCommit(false);
                        try (Statement statement = conn.createStatement()) {
                            // make the transaction larger so that embulk-output-mysql transaction will be rolled back at a deadlock.
                            for (int i = 100; i < 110; i++) {
                                statement.execute("insert into test1 values('C" + i + "', 0)");
                            }

                            statement.execute("insert into test1 values('A004', 0)");
                            Thread.sleep(6000);
                            // deadlock will occur
                            statement.execute("insert into test1 values('A001', 0)");
                            conn.rollback();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        thread2.start();

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test1.yml")), in1);
        assertThat(selectRecords(embulk, "test1"), is(readResource("test1_expected.csv")));
    }

    // records will be partially committed
    @Test
    public void testRetry_Large_MultipleTimes() throws Exception
    {
        // canonicalize path because LocalFileInputPlugin can't read Windows short file name.
        Path in1 = embulk.createTempFile("csv").toRealPath();
        StringBuilder expected1 = new StringBuilder();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(in1.toFile()))) {
            writer.write("id:string,num:long");
            writer.newLine();

            for (int i = 1000000; i < 1250000; i++) {
                writer.write("A" + i + "," + i);
                writer.newLine();

                expected1.append("A" + i + "," + i + "\n");
            }
        }

        final Lock lock1 = new Lock();
        final Lock lock2 = new Lock();

        Thread thread1 = new Thread() {
            @Override
            public void run() {
                try {
                    try (Connection conn = MySQLTests.connect()) {
                        conn.setAutoCommit(false);
                        try (Statement statement = conn.createStatement()) {
                            // make the transaction larger so that embulk-output-mysql transaction will be rolled back at a deadlock.
                            for (int i = 1000000; i < 1260000; i++) {
                                statement.execute("insert into test1 values('B" + i + "', 0)");
                            }
                            System.out.println("# Inserted many records (1).");

                            synchronized (lock1) {
                                lock1.notify();
                                lock1.enabled = false;
                            }

                            System.out.println("# Insert 'A1249010'.");
                            statement.execute("insert into test1 values('A1249010', 0)");
                            Thread.sleep(5000);
                            // deadlock will occur
                            System.out.println("# Insert 'A1249000'.");
                            statement.execute("insert into test1 values('A1249000', 0)");
                            conn.rollback();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        thread1.start();

        Thread thread2 = new Thread() {
            @Override
            public void run() {
                try {
                    try (Connection conn = MySQLTests.connect()) {
                        conn.setAutoCommit(false);
                        try (Statement statement = conn.createStatement()) {
                            // make the transaction larger so that embulk-output-mysql transaction will be rolled back at a deadlock.
                            for (int i = 1000000; i < 1260000; i++) {
                                statement.execute("insert into test1 values('C" + i + "', 0)");
                            }
                            System.out.println("# Inserted many records (2).");

                            synchronized (lock2) {
                                lock2.notify();
                                lock2.enabled = false;
                            }

                            System.out.println("# Insert 'A1249030'.");
                            statement.execute("insert into test1 values('A1249030', 0)");
                            Thread.sleep(20000);
                            // deadlock will occur
                            System.out.println("# Insert 'A1249020'.");
                            statement.execute("insert into test1 values('A1249020', 0)");
                            conn.rollback();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        thread2.start();

        synchronized (lock1) {
            if (lock1.enabled) {
                lock1.wait();
            }
        }
        synchronized (lock2) {
            if (lock2.enabled) {
                lock2.wait();
            }
        }

        //Path in1 = toPath("test1_flushed_multiple_times.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test1.yml")), in1);
        assertThat(selectRecords(embulk, "test1"), is(expected1.toString()));
    }

    private Path toPath(String fileName) throws URISyntaxException
    {
        URL url = Resources.getResource(BASIC_RESOURCE_PATH + fileName);
        return FileSystems.getDefault().getPath(new File(url.toURI()).getAbsolutePath());
    }

}
