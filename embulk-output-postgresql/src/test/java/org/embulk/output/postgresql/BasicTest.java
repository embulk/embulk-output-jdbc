package org.embulk.output.postgresql;

import static org.embulk.output.postgresql.PostgreSQLTests.execute;
import static org.embulk.output.postgresql.PostgreSQLTests.selectRecords;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.PostgreSQLOutputPlugin;
import org.embulk.output.file.LocalFileOutputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.io.Resources;

public class BasicTest
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/output/postgresql/test/expect/basic/";

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
            .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
            .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
            .registerPlugin(FormatterPlugin.class, "csv", CsvFormatterPlugin.class)
            .registerPlugin(FileOutputPlugin.class, "file", LocalFileOutputPlugin.class)
            .registerPlugin(OutputPlugin.class, "postgresql", PostgreSQLOutputPlugin.class)
            .build();

    private ConfigSource baseConfig;

    @Before
    public void setup() throws IOException
    {
        baseConfig = PostgreSQLTests.baseConfig();
        execute(readResource("setup.sql")); // setup rows

        if (System.getProperty("path.separator").equals(";")) {
            // for Windows (because encoding will be set to UTF8 when executed by Eclipse)
            System.setProperty("file.encoding", "MS932");
        }
    }

    @Test
    public void testNumber() throws Exception
    {
        Path in1 = toPath("test_number.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_number.yml")), in1);
        assertThat(selectRecords(embulk, "test_number"), is(readResource("test_number_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testString() throws Exception
    {
        Path in1 = toPath("test_string.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_string.yml")), in1);
        assertThat(selectRecords(embulk, "test_string"), is(readResource("test_string_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testTimestamp() throws Exception
    {
        Path in1 = toPath("test_timestamp.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_timestamp.yml")), in1);
        assertThat(selectRecords(embulk, "test_timestamp"), is(readResource("test_timestamp_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testJson() throws Exception
    {
        Path in1 = toPath("test_json.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_json.yml")), in1);
        assertThat(selectRecords(embulk, "test_json"), is(readResource("test_json_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testMerge() throws Exception
    {
        Path in1 = toPath("test_merge.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_merge.yml")), in1);
        assertThat(selectRecords(embulk, "test_merge"), is(readResource("test_merge_expected.csv")));
    }

    @Test
    public void testMergeWithKeys() throws Exception
    {
        Path in1 = toPath("test_merge_keys.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_merge_keys.yml")), in1);
        assertThat(selectRecords(embulk, "test_merge"), is(readResource("test_merge_keys_expected.csv")));
    }

    @Test
    public void testMergeRule() throws Exception
    {
        Path in1 = toPath("test_merge.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_merge_rule.yml")), in1);
        assertThat(selectRecords(embulk, "test_merge"), is(readResource("test_merge_rule_expected.csv")));
    }

    @Test
    public void testReplace() throws Exception
    {
        Path in1 = toPath("test_string.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_replace.yml")), in1);
        assertThat(selectRecords(embulk, "test_string"), is(readResource("test_replace_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }


    private Path toPath(String fileName) throws URISyntaxException
    {
        URL url = Resources.getResource(BASIC_RESOURCE_PATH + fileName);
        return FileSystems.getDefault().getPath(new File(url.toURI()).getAbsolutePath());
    }

    @Test
    public void testRoleName() throws Exception
    {
        Path in1 = toPath("test_string.csv");
        ConfigSource config = baseConfig.merge(loadYamlResource(embulk, "test_string.yml"));
        config.set("role_name", baseConfig.get(String.class, "user"));
        TestingEmbulk.RunResult result1 = embulk.runOutput(config, in1);
        assertThat(selectRecords(embulk, "test_string"), is(readResource("test_string_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

}
