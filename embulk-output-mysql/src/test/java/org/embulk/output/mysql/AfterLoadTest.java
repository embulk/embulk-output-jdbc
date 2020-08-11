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

public class AfterLoadTest
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/output/mysql/test/expect/after_load/";

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
    public void testInsertAfterLoad() throws Exception
    {
        execute("insert into test1 values('B001', 0, 'z')");
        execute("insert into test1 values('B002', 9, 'z')");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_after_load.yml")), in1);
        assertThat(selectRecords(embulk, "test1"), is(readResource("test_insert_after_load_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testInsertDirectAfterLoad() throws Exception
    {
        execute("insert into test1 values('B001', 0, 'z')");
        execute("insert into test1 values('B002', 9, 'z')");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_direct_after_load.yml")), in1);
        assertThat(selectRecords(embulk, "test1"), is(readResource("test_insert_after_load_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testTruncateInsertAfterLoad() throws Exception
    {
        execute("insert into test1 values('B001', 0, 'z')");
        execute("insert into test1 values('B002', 9, 'z')");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_replace_after_load.yml")), in1);
        assertThat(selectRecords(embulk, "test1"), is(readResource("test_replace_after_load_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testReplaceAfterLoad() throws Exception
    {
        execute("insert into test1 values('B001', 0, 'z')");
        execute("insert into test1 values('B002', 9, 'z')");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_truncate_insert_after_load.yml")), in1);
        assertThat(selectRecords(embulk, "test1"), is(readResource("test_truncate_insert_after_load_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testMergeAfterLoad() throws Exception
    {
        execute("insert into test1 values('A002', 1, 'y')");
        execute("insert into test1 values('A003', 1, 'y')");
        execute("insert into test1 values('B001', 0, 'z')");
        execute("insert into test1 values('B002', 9, 'z')");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_merge_after_load.yml")), in1);
        assertThat(selectRecords(embulk, "test1"), is(readResource("test_merge_after_load_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testMergeDirectAfterLoad() throws Exception
    {
        execute("insert into test1 values('A002', 1, 'y')");
        execute("insert into test1 values('A003', 1, 'y')");
        execute("insert into test1 values('B001', 0, 'z')");
        execute("insert into test1 values('B002', 9, 'z')");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_merge_direct_after_load.yml")), in1);
        assertThat(selectRecords(embulk, "test1"), is(readResource("test_merge_after_load_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    private Path toPath(String fileName) throws URISyntaxException
    {
        URL url = Resources.getResource(BASIC_RESOURCE_PATH + fileName);
        return FileSystems.getDefault().getPath(new File(url.toURI()).getAbsolutePath());
    }
}
