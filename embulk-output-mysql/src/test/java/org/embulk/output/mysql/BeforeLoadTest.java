package org.embulk.output.mysql;

import com.google.common.io.Resources;
import org.embulk.config.ConfigSource;
import org.embulk.output.MySQLOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;

import static org.embulk.output.mysql.MySQLTests.execute;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class BeforeLoadTest
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/output/mysql/test/expect/before_load/";

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
    public void testInsertBeforeLoad() throws Exception
    {
        execute("insert into test1 values('B001', 0, 'z')");
        execute("insert into test1 values('B002', 9, 'z')");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_before_load.yml")), in1);
        assertThat(selectRecords(), is(readResource("test_insert_before_load_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testInsertDirectBeforeLoad() throws Exception
    {
        execute("insert into test1 values('B001', 0, 'z')");
        execute("insert into test1 values('B002', 9, 'z')");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_direct_before_load.yml")), in1);
        assertThat(selectRecords(), is(readResource("test_insert_before_load_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testTruncateInsertBeforeLoad() throws Exception
    {
        execute("insert into test1 values('B001', 0, 'z')");
        execute("insert into test1 values('B002', 9, 'z')");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_truncate_insert_before_load.yml")), in1);
        assertThat(selectRecords(), is(readResource("test_truncate_insert_before_load_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testMergeBeforeLoad() throws Exception
    {
        execute("insert into test1 values('A002', 1, 'y')");
        execute("insert into test1 values('A003', 1, 'y')");
        execute("insert into test1 values('B001', 0, 'z')");
        execute("insert into test1 values('B002', 9, 'z')");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_merge_before_load.yml")), in1);
        assertThat(selectRecords(), is(readResource("test_merge_before_load_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testMergeDirectBeforeLoad() throws Exception
    {
        execute("insert into test1 values('A002', 1, 'y')");
        execute("insert into test1 values('A003', 1, 'y')");
        execute("insert into test1 values('B001', 0, 'z')");
        execute("insert into test1 values('B002', 9, 'z')");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_merge_direct_before_load.yml")), in1);
        assertThat(selectRecords(), is(readResource("test_merge_before_load_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    private Path toPath(String fileName) throws URISyntaxException
    {
        URL url = Resources.getResource(BASIC_RESOURCE_PATH + fileName);
        return FileSystems.getDefault().getPath(new File(url.toURI()).getAbsolutePath());
    }

    private String selectRecords()
    {
        return MySQLTests.selectRecords("test1", Arrays.asList("id", "int_item", "varchar_item"));
    }
}
