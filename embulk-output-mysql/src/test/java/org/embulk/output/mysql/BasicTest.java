package org.embulk.output.mysql;

import static org.embulk.output.mysql.MySQLTests.execute;
import static org.embulk.output.mysql.MySQLTests.selectRecords;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;

import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigException;
import org.embulk.exec.PartialExecutionException;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.MySQLOutputPlugin;
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
    private static final String BASIC_RESOURCE_PATH = "org/embulk/output/mysql/test/expect/basic/";

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
    public void testReplace() throws Exception
    {
        Path in1 = toPath("test1.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_replace.yml")), in1);
        // Multiple timestamp columns will cause the error "Invalid default value for <second timestamp column>".
        assertThat(selectRecords("test1", Arrays.asList("id", "num", "str", "varstr", "dttm3")), is(readResource("test_replace_expected.csv")));
    }

    @Test
    public void testMerge() throws Exception
    {
        Path in1 = toPath("test_merge.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_merge.yml")), in1);
        assertThat(selectRecords("test_merge", Arrays.asList("id", "value1", "value2")), is(readResource("test_merge_expected.csv")));
    }

    @Test
    public void testInvalidTimeZone() throws Exception
    {
        Path in1 = toPath("test1.csv");
        try {
            embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_invalid_time_zone.yml")), in1);
        } catch (final PartialExecutionException ex) {
            Throwable cause = ex.getCause();
            while (cause != null) {
                if (cause.getMessage() != null
                            && cause.getMessage().contains("\"Somewhere/Some_City\" is not recognized as a timezone name.")) {
                    return;
                }
                cause = cause.getCause();
            }
        }
        fail("It did not throw an expected Exception.");
    }

    private Path toPath(String fileName) throws URISyntaxException
    {
        URL url = Resources.getResource(BASIC_RESOURCE_PATH + fileName);
        return FileSystems.getDefault().getPath(new File(url.toURI()).getAbsolutePath());
    }
}
