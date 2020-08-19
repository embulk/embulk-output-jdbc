package org.embulk.output.mysql;

import static org.embulk.output.mysql.MySQLTests.execute;
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
import org.embulk.output.MySQLOutputPlugin;
import org.embulk.spi.OutputPlugin;
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
    public void testInvalidTimeZone() throws Exception
    {
        Path in1 = toPath("test1.csv");
        try {
            embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_invalid_time_zone.yml")), in1);
        } catch (final PartialExecutionException ex) {
            final Throwable cause = ex.getCause();
            assertThat(cause, instanceOf(ConfigException.class));
            assertThat(cause.getMessage(), is("Time zone 'Somewhere/Some_City' is not recognised."));
            return;
        }
        fail();
    }

    private Path toPath(String fileName) throws URISyntaxException
    {
        URL url = Resources.getResource(BASIC_RESOURCE_PATH + fileName);
        return FileSystems.getDefault().getPath(new File(url.toURI()).getAbsolutePath());
    }
}
