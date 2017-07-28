package org.embulk.output.sqlserver;

import static org.embulk.output.sqlserver.SQLServerTests.execute;
import static org.embulk.output.sqlserver.SQLServerTests.selectRecords;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.output.SQLServerOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.io.Resources;

public class BasicTest
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/output/sqlserver/test/expect/basic/";

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
            .registerPlugin(OutputPlugin.class, "sqlserver", SQLServerOutputPlugin.class)
            .build();

    private ConfigSource baseConfig;

    @Before
    public void setup()
    {
        baseConfig = SQLServerTests.baseConfig();
        execute(readResource("setup.sql")); // setup rows
    }

    @Test
    public void testInsert() throws Exception
    {
        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1", in1), is(readResource("test_insert_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testInsertCreate() throws Exception
    {
        execute("DROP TABLE TEST1");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1", in1), is(readResource("test_insert_create_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testInsertDirect() throws Exception
    {
        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_direct.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1", in1), is(readResource("test_insert_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testInsertDirectCreate() throws Exception
    {
        execute("DROP TABLE TEST1");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_direct.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1", in1), is(readResource("test_insert_create_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testTruncateInsert() throws Exception
    {
        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_truncate_insert.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1", in1), is(readResource("test_truncate_insert_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testReplace() throws Exception
    {
        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_replace.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1", in1), is(readResource("test_insert_create_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testReplaceCreate() throws Exception
    {
        execute("DROP TABLE TEST1");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_replace.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1", in1), is(readResource("test_insert_create_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testReplaceLongName() throws Exception
    {
        String tableName = "TEST___Ａ１２３４５６７８９Ｂ１２３４５６７８９Ｃ１２３４５６７８９Ｄ１２３４５６７８９Ｅ１２３４５６７８９Ｆ１２３４５６７８９Ｇ１２３４５６７８９Ｈ１２３４５６７８９Ｉ１２３４５６７８９Ｊ１２３４５６７８９Ｋ１２３４５６７８９Ｌ１２３４５６７８９";
        assertThat(tableName.length(), is(127));

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_replace_longname.yml")), in1);
        assertThat(selectRecords(embulk, tableName, in1), is(readResource("test_insert_create_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }


    private Path toPath(String fileName) throws URISyntaxException
    {
        URL url = Resources.getResource(BASIC_RESOURCE_PATH + fileName);
        return FileSystems.getDefault().getPath(new File(url.toURI()).getAbsolutePath());
    }

}
