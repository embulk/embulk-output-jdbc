package org.embulk.output.sqlserver;

import static org.embulk.output.sqlserver.SQLServerTests.execute;
import static org.embulk.output.sqlserver.SQLServerTests.selectRecords;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.embulk.config.ConfigSource;
import org.embulk.output.SQLServerOutputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class NativeTest
{
    private static final String BASIC_RESOURCE_PATH = "/org/embulk/output/sqlserver/test/expect/native/";

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

    @Test
    public void testDecimalNull() throws Exception
    {
        Path in1 = toPath("test_decimal_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_decimal_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_DECIMAL"), is(readResource("test_decimal_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testNumericNull() throws Exception
    {
        Path in1 = toPath("test_decimal_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_numeric_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_NUMERIC"), is(readResource("test_decimal_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testMoneyNull() throws Exception
    {
        Path in1 = toPath("test_decimal_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_money_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_MONEY"), is(readResource("test_decimal_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testSmallMoneyNull() throws Exception
    {
        Path in1 = toPath("test_decimal_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_smallmoney_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_SMALLMONEY"), is(readResource("test_decimal_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testBitNull() throws Exception
    {
        Path in1 = toPath("test_bit_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_bit_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_BIT"), is(readResource("test_bit_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testRealNull() throws Exception
    {
        Path in1 = toPath("test_float_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_real_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_REAL"), is(readResource("test_float_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testFloatNull() throws Exception
    {
        Path in1 = toPath("test_float_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_float_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_FLOAT"), is(readResource("test_float_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testCharNull() throws Exception
    {
        Path in1 = toPath("test_char_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_char_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_CHAR"), is(readResource("test_char_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testVarCharNull() throws Exception
    {
        Path in1 = toPath("test_varchar_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_varchar_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_VARCHAR"), is(readResource("test_varchar_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testTextNull() throws Exception
    {
        Path in1 = toPath("test_varchar_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_text_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_TEXT"), is(readResource("test_varchar_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testNCharNull() throws Exception
    {
        Path in1 = toPath("test_char_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_nchar_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_NCHAR"), is(readResource("test_char_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testNVarCharNull() throws Exception
    {
        Path in1 = toPath("test_varchar_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_nvarchar_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_NVARCHAR"), is(readResource("test_varchar_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testNTextNull() throws Exception
    {
        Path in1 = toPath("test_varchar_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_ntext_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_NTEXT"), is(readResource("test_varchar_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testDateNull() throws Exception
    {
        Path in1 = toPath("test_timestamp_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_date_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_DATE"), is(readResource("test_date_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testDateTimeNull() throws Exception
    {
        Path in1 = toPath("test_timestamp_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_datetime_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_DATETIME"), is(readResource("test_datetime_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testDateTime2Null() throws Exception
    {
        Path in1 = toPath("test_timestamp_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_datetime2_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_DATETIME2"), is(readResource("test_datetime2_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testSmallDateTimeNull() throws Exception
    {
        Path in1 = toPath("test_timestamp_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_smalldatetime_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_SMALLDATETIME"), is(readResource("test_smalldatetime_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testTimeNull() throws Exception
    {
        Path in1 = toPath("test_timestamp_null.csv");
        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_time_null.yml")), in1);
        assertThat(selectRecords(embulk, "TEST_TIME"), is(readResource("test_time_null_expected.csv")));
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));
    }

    @Test
    public void testHuge() throws Exception
    {
        Path in1 = toPath("test_huge.csv");

        // create input data dynamically because it is huge.

        List<String[]> recordsList = new ArrayList<>();
        recordsList.add(new String[]{"a", "X", "あ"});
        recordsList.add(new String[]{"b", createString(9000), createString(10000) + "い"});
        recordsList.add(new String[]{"c", createString(20000), createString(30000) + "う"});
        List<String[]> records = Collections.unmodifiableList(recordsList);

        //ImmutableList.Builder<String> linesBuilder = ImmutableList.builder();
        List<String> lines = new ArrayList<>();
        lines.add("ITEM1:long,ITEM2:string,ITEM3:string,ITEM4:string");
        for (int i = 0; i < records.size(); i++) {
            String[] record = records.get(i);
            lines.add((i + 1) + "," + record[0] + "," + record[1] + "," + record[2]);
        }

        Charset charset = Charset.forName("UTF8");
        Files.write(in1, Collections.unmodifiableList(lines), charset);

        TestingEmbulk.RunResult result1 = embulk.runOutput(baseConfig.merge(loadYamlResource(embulk, "test_huge.yml")), in1);
        //assertThat(result1.getConfigDiff(), is((ConfigDiff) loadYamlResource(embulk, "test_expected.diff")));

        try (Connection conn = SQLServerTests.connect()) {
            try (Statement statement = conn.createStatement()) {
                try (ResultSet rs = statement.executeQuery("SELECT * FROM TEST_HUGE ORDER BY ITEM1")) {
                    int recordIndex = 0;
                    while (rs.next()) {
                        assertThat(rs.getInt(1), is(recordIndex + 1));
                        assertThat(rs.getString(2), is(records.get(recordIndex)[0]));
                        assertThat(rs.getString(3), is(records.get(recordIndex)[1]));
                        assertThat(rs.getString(4), is(records.get(recordIndex)[2]));
                        recordIndex++;
                    }

                    assertThat(recordIndex, is(records.size()));
                }
            }
        }
    }

    private String createString(int size)
    {
        char[] chars = new char[size];
        for (int i = 0; i < size; i++) {
            chars[i] = (char)('A' + i % 26);
        }
        return new String(chars);
    }

    private Path toPath(String fileName) throws URISyntaxException
    {
        URL url = EmbulkTests.class.getResource(BASIC_RESOURCE_PATH + fileName);
        return FileSystems.getDefault().getPath(new File(url.toURI()).getAbsolutePath());
    }

}
