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

public class NativeTest
{
    private static final String BASIC_RESOURCE_PATH = "org/embulk/output/sqlserver/test/expect/native/";

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
    public void testTinyIntNull() throws Exception
    {
        Path in1 = toPath("test_integer_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_tinyint_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_TINYINT"), is(readResource("test_integer_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testSmallIntNull() throws Exception
    {
        Path in1 = toPath("test_integer_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_smallint_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_SMALLINT"), is(readResource("test_integer_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testIntNull() throws Exception
    {
        Path in1 = toPath("test_integer_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_int_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_INT"), is(readResource("test_integer_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testBigIntNull() throws Exception
    {
        Path in1 = toPath("test_integer_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_bigint_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_BIGINT"), is(readResource("test_integer_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    private Path toPath(String fileName) throws URISyntaxException
    {
        URL url = Resources.getResource(BASIC_RESOURCE_PATH + fileName);
        return FileSystems.getDefault().getPath(new File(url.toURI()).getAbsolutePath());
    }

}