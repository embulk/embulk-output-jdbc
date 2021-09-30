package org.embulk.output.jdbc;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestJdbcOutputPlugin
{
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
}
