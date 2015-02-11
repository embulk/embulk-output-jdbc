package org.embulk.output.jdbc.setter;

import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class SkipColumnSetter
        extends ColumnSetter
{
    public SkipColumnSetter(BatchInsert batch, PageReader pageReader)
    {
        super(batch, pageReader, null);
    }

    protected void booleanValue(boolean v)
    {
    }

    protected void longValue(long v)
    {
    }

    protected void doubleValue(double v)
    {
    }

    protected void stringValue(String v)
    {
    }

    protected void timestampValue(Timestamp v)
    {
    }
}
