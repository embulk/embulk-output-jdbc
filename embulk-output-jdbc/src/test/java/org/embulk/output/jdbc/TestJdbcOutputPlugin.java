package org.embulk.output.jdbc;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestJdbcOutputPlugin
{
    @Test
    public void testBuildFormattedTaskIndex()
    {
        {
            try {
                AbstractJdbcOutputPlugin.buildFormattedTaskIndex(-1, 5);
            } catch (Exception e) {
                assertEquals(java.util.IllegalFormatFlagsException.class, e.getClass());
            }
        }
        {
            try {
                AbstractJdbcOutputPlugin.buildFormattedTaskIndex(0, 5);
            } catch (Exception e) {
                assertEquals(java.util.DuplicateFormatFlagsException.class, e.getClass());
            }
        }
        {
            String s = AbstractJdbcOutputPlugin.buildFormattedTaskIndex(1, 5);
            assertEquals("5", s);
        }
        {
            String s = AbstractJdbcOutputPlugin.buildFormattedTaskIndex(2, 5);
            assertEquals("05", s);
        }
        {
            String s = AbstractJdbcOutputPlugin.buildFormattedTaskIndex(10, 5);
            assertEquals("0000000005", s);
        }
    }
}
