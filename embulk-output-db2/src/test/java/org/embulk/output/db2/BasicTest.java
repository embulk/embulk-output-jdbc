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
import org.embulk.output.DB2OutputPlugin;
import org.embulk.spi.OutputPlugin;
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


    private Path toPath(String fileName) throws URISyntaxException
    {
        URL url = Resources.getResource(BASIC_RESOURCE_PATH + fileName);
        return FileSystems.getDefault().getPath(new File(url.toURI()).getAbsolutePath());
    }
}
