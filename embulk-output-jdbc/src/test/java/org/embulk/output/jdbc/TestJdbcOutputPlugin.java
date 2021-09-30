package org.embulk.output.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import org.embulk.config.ConfigException;
import org.junit.Test;

public class TestJdbcOutputPlugin
{
    @Test
    public void testIntermediateTableNameTruncated()
    {
        final long timeDummy = 1632976920261L;
        assertIntermediateTableNameTruncatedWithCharacters("footest_0000017c35025ec5_embulk", "footest", timeDummy, 32);
        assertIntermediateTableNameTruncatedWithCharacters("footest_0000017c35025ec5_embulk", "footest", timeDummy, 31);
        assertIntermediateTableNameTruncatedWithCharacters("footest_000017c35025ec5_embulk", "footest", timeDummy, 30);
        assertIntermediateTableNameTruncatedWithCharacters("footest_17c35025ec5_embulk", "footest", timeDummy, 26);
        assertIntermediateTableNameTruncatedWithCharacters("footest_35025ec5_embulk", "footest", timeDummy, 23);
        assertIntermediateTableNameTruncatedWithCharacters("footes_35025ec5_embulk", "footest", timeDummy, 22);
        assertIntermediateTableNameTruncatedWithCharacters("foot_35025ec5_embulk", "footest", timeDummy, 20);
        assertIntermediateTableNameTruncatedWithCharacters("f_35025ec5_embulk", "footest", timeDummy, 17);

        try {
            AbstractJdbcOutputPlugin.LengthSemantics.CHARACTERS.buildIntermediateTableNameTruncated("footest", timeDummy, 16);
            fail();
        } catch (final ConfigException ignored) {
            // Pass-through.
        }
    }

    @Test
    public void testCalculateSuffixLength()
    {
        {
            try {
                AbstractJdbcOutputPlugin.calculateSuffixLength(-1);
            } catch (java.lang.AssertionError e) {
                assert(true);
            }
        }
        {
            try {
                AbstractJdbcOutputPlugin.calculateSuffixLength(0);
            } catch (java.lang.AssertionError e) {
                assert(true);
            }
        }
        {
            int i = AbstractJdbcOutputPlugin.calculateSuffixLength(1);
            assertEquals(3, i);
        }
        {
            int i = AbstractJdbcOutputPlugin.calculateSuffixLength(10);
            assertEquals(3, i);
        }
        {
            int i = AbstractJdbcOutputPlugin.calculateSuffixLength(11);
            assertEquals(3, i);
        }
        {
            int i = AbstractJdbcOutputPlugin.calculateSuffixLength(100);
            assertEquals(3, i);
        }
        {
            int i = AbstractJdbcOutputPlugin.calculateSuffixLength(101);
            assertEquals(3, i);
        }
        {
            int i = AbstractJdbcOutputPlugin.calculateSuffixLength(1000);
            assertEquals(3, i);
        }
        {
            int i = AbstractJdbcOutputPlugin.calculateSuffixLength(1001);
            assertEquals(4, i);
        }
        {
            int i = AbstractJdbcOutputPlugin.calculateSuffixLength(10000);
            assertEquals(4, i);
        }
        {
            int i = AbstractJdbcOutputPlugin.calculateSuffixLength(10001);
            assertEquals(5, i);
        }
    }

    private static void assertIntermediateTableNameTruncatedWithCharacters(
            final String expected, final String baseName, final long timeMillis, final int maxLength)
    {
        assertEquals(
                expected,
                AbstractJdbcOutputPlugin.LengthSemantics.CHARACTERS.buildIntermediateTableNameTruncated(
                    baseName, StandardCharsets.UTF_8, timeMillis, maxLength));
    }
}
