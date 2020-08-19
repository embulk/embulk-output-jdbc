package org.embulk.output.jdbc.setter;

import java.time.Instant;
import org.embulk.output.jdbc.BatchInsert;
import org.msgpack.value.Value;

public class SkipColumnSetter
        extends ColumnSetter
{
    public SkipColumnSetter(BatchInsert batch)
    {
        super(batch, null, null);
    }

    @Override
    public void booleanValue(boolean v)
    {
    }

    @Override
    public void longValue(long v)
    {
    }

    @Override
    public void doubleValue(double v)
    {
    }

    @Override
    public void stringValue(String v)
    {
    }

    @Override
    public void timestampValue(final Instant v)
    {
    }

    @Override
    public void nullValue()
    {
    }

    @Override
    public void jsonValue(Value v)
    {
    }
}
