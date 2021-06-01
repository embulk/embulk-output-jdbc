package org.embulk.output.db2;

import static org.embulk.output.db2.DB2Tests.execute;
import static org.embulk.output.db2.DB2Tests.selectRecords;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.embulk.config.ConfigSource;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.DB2OutputPlugin;
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
    private static final String BASIC_RESOURCE_PATH = "org/embulk/output/db2/test/expect/basic/";

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
            .registerPlugin(OutputPlugin.class, "db2", DB2OutputPlugin.class)
            .build();

    private ConfigSource baseConfig;

    @Before
    public void setup() throws Exception
    {
        baseConfig = DB2Tests.baseConfig();
        execute(BASIC_RESOURCE_PATH + "setup.sql"); // setup rows
    }

    @Test
    public void testInsertChar() throws Exception
    {
        Path in1 = toPath("test_char.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_char.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_CHAR"), is(readResource("test_insert_char_expected.txt")));
    }

    @Test
    public void testInsertDirectChar() throws Exception
    {
        Path in1 = toPath("test_char.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_direct_char.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_CHAR"), is(readResource("test_insert_char_expected.txt")));
    }

    @Test
    public void testInsertCreateChar() throws Exception
    {
        Path in1 = toPath("test_char.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_create_char.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_CHAR2"), is(readResource("test_create_char_expected.txt")));
    }

    @Test
    public void testReplace() throws Exception
    {
        Path in1 = toPath("test_char.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_replace.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_CHAR"), is(readResource("test_create_char_expected.txt")));
    }

    @Test
    public void testReplaceCreate() throws Exception
    {
        Path in1 = toPath("test_char.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_replace_create.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_CHAR2"), is(readResource("test_create_char_expected.txt")));
    }

    @Test
    public void testReplaceLongName() throws Exception
    {
        // table name has 128 characters
        Path in1 = toPath("test_char.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_replace_long_name.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_CHAR_1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678"), is(readResource("test_create_char_expected.txt")));
    }

    @Test
    public void testInsertNumber() throws Exception
    {
        Path in1 = toPath("test_number.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_number.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_NUMBER"), is(readResource("test_insert_number_expected.txt")));
    }

    @Test
    public void testInsertDirectNumber() throws Exception
    {
        Path in1 = toPath("test_number.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_direct_number.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_NUMBER"), is(readResource("test_insert_number_expected.txt")));
    }

    @Test
    public void testInsertCreateNumber() throws Exception
    {
        Path in1 = toPath("test_number.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_create_number.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_NUMBER2"), is(readResource("test_create_number_expected.txt")));
    }

    @Test
    public void testTruncateInsert() throws Exception
    {
        Path in1 = toPath("test_number.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_truncate_insert.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_NUMBER"), is(readResource("test_truncate_insert_expected.txt")));
    }

    @Test
    public void testInsertDateTime() throws Exception
    {
        Path in1 = toPath("test_datetime.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_datetime.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_DATETIME"), is(readResource("test_insert_datetime_expected.txt")));
    }

    @Test
    public void testInsertDirectDateTime() throws Exception
    {
        Path in1 = toPath("test_datetime.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_direct_datetime.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_DATETIME"), is(readResource("test_insert_direct_datetime_expected.txt")));
    }

    @Test
    public void testInsertCreateDateTime() throws Exception
    {
        Path in1 = toPath("test_datetime.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_insert_create_datetime.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_DATETIME2"), is(readResource("test_create_datetime_expected.txt")));
    }


    private Path toPath(String fileName) throws URISyntaxException
    {
        URL url = Resources.getResource(BASIC_RESOURCE_PATH + fileName);
        return FileSystems.getDefault().getPath(new File(url.toURI()).getAbsolutePath());
    }
}
