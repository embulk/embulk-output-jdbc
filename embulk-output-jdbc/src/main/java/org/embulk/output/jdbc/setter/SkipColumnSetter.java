package org.embulk.output.jdbc.setter;

import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.BatchInsert;

public class SkipColumnSetter
        extends ColumnSetter
{
    public SkipColumnSetter(BatchInsert batch, PageReader pageReader)
    {
        super(batch, pageReader, null);
    }

    @Override
    protected void booleanValue(boolean v)
    {
    }

    @Override
    protected void longValue(long v)
    {
    }

    @Override
    protected void doubleValue(double v)
    {
    }

    @Override
    protected void stringValue(String v)
    {
    }

    @Override
    protected void timestampValue(Timestamp v)
    {
    }

    @Override
    protected void nullValue()
    {
    }
}
