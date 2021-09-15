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
                AbstractJdbcOutputPlugin.buildFormattedTaskIndexSuffix(-1, 5);
            } catch (java.lang.AssertionError e) {
                assert(true);
            }
        }
        {
            try {
                AbstractJdbcOutputPlugin.buildFormattedTaskIndexSuffix(0, 5);
            } catch (java.lang.AssertionError e) {
                assert(true);
            }
        }
        {
            String s = AbstractJdbcOutputPlugin.buildFormattedTaskIndexSuffix(1, 5);
            assertEquals("5", s);
        }
        {
            String s = AbstractJdbcOutputPlugin.buildFormattedTaskIndexSuffix(10, 5);
            assertEquals("5", s);
        }
        {
            String s = AbstractJdbcOutputPlugin.buildFormattedTaskIndexSuffix(11, 5);
            assertEquals("05", s);
        }
        {
            String s = AbstractJdbcOutputPlugin.buildFormattedTaskIndexSuffix(100, 5);
            assertEquals("05", s);
        }
        {
            String s = AbstractJdbcOutputPlugin.buildFormattedTaskIndexSuffix(101, 5);
            assertEquals("005", s);
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
            assertEquals(1, i);
        }
        {
            int i = AbstractJdbcOutputPlugin.calculateSuffixLength(10);
            assertEquals(1, i);
        }
        {
            int i = AbstractJdbcOutputPlugin.calculateSuffixLength(11);
            assertEquals(2, i);
        }
    }
}
