package org.embulk.output.sqlserver;

import static org.embulk.output.sqlserver.SQLServerTests.execute;
import static org.embulk.output.sqlserver.SQLServerTests.executeQuery;
import static org.embulk.output.sqlserver.SQLServerTests.selectRecords;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

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
        assertThat(selectRecords(embulk, "TEST1"), is(readResource("test_insert_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testInsertCreate() throws Exception
    {
        execute("DROP TABLE TEST1");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1"), is(readResource("test_insert_create_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testInsertDirect() throws Exception
    {
        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_direct.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1"), is(readResource("test_insert_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testInsertDirectCreate() throws Exception
    {
        execute("DROP TABLE TEST1");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_direct.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1"), is(readResource("test_insert_create_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testTruncateInsert() throws Exception
    {
        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_truncate_insert.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1"), is(readResource("test_truncate_insert_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testReplace() throws Exception
    {
        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_replace.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1"), is(readResource("test_insert_create_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testReplaceCreate() throws Exception
    {
        execute("DROP TABLE TEST1");

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_replace.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1"), is(readResource("test_insert_create_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testReplaceLongName() throws Exception
    {
        String tableName = "TEST___Ａ１２３４５６７８９Ｂ１２３４５６７８９Ｃ１２３４５６７８９Ｄ１２３４５６７８９Ｅ１２３４５６７８９Ｆ１２３４５６７８９Ｇ１２３４５６７８９Ｈ１２３４５６７８９Ｉ１２３４５６７８９Ｊ１２３４５６７８９Ｋ１２３４５６７８９Ｌ１２３４５６７８９";
        assertThat(tableName.length(), is(127));

        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_replace_longname.yml")), in1);
        assertThat(selectRecords(embulk, tableName), is(readResource("test_insert_create_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testMerge() throws Exception
    {
        Path in1 = toPath("test_merge.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_merge.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_MERGE1"), is(readResource("test_merge_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testMergeWithKeys() throws Exception
    {
        Path in1 = toPath("test_merge.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_merge_keys.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_MERGE2"), is(readResource("test_merge_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testMergeWithRule() throws Exception
    {
        Path in1 = toPath("test_merge.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_merge_rule.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_MERGE1"), is(readResource("test_merge_rule_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testNativeInsertDirect() throws Exception
    {
        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_native_insert_direct.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1"), is(readResource("test_insert_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testStringToTimestamp() throws Exception
    {
        Path in1 = toPath("test_string_timestamp.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_string_timestamp.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1"), is(readResource("test_string_timestamp_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testMax() throws Exception
    {
        // canonicalize path because LocalFileInputPlugin can't read Windows short file name.
        Path in1 = embulk.createTempFile("csv").toRealPath();
        String line3 = "2," + createString('A', 8000) + "," + createString('a', 10000) + "," + createString('あ', 4000) + "," + createString('ア', 10000);
        Files.write(in1, Arrays.asList("ID:long,C1:string,C2:string,C3:string,C4:string", "1,,,,", line3));
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_max.yml")), in1);
        assertThat(executeQuery(embulk, "SELECT ID, LEN(C1), LEN(C2), LEN(C3), LEN(C4) FROM TEST_MAX"), is("1,NULL,NULL,NULL,NULL\n2,8000,10000,4000,10000\n"));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    private static String createString(char c, int n)
    {
        char[] chars = new char[n];
        Arrays.fill(chars, c);
        return new String(chars);
    }

    @Test
    public void testJtds() throws Exception
    {
        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_jtds.yml")), in1);
        assertThat(selectRecords(embulk, "TEST1"), is(readResource("test_insert_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    private Path toPath(String fileName) throws URISyntaxException
    {
        URL url = Resources.getResource(BASIC_RESOURCE_PATH + fileName);
        return FileSystems.getDefault().getPath(new File(url.toURI()).getAbsolutePath());
    }

}
