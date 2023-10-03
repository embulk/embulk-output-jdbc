package org.embulk.output.jdbc;

import java.time.Instant;
import org.embulk.spi.Column;
import org.msgpack.value.Value;

public class MemoryRecord implements Record
{
    private final Object[] values;

    public MemoryRecord(int columnCount)
    {
        values = new Object[columnCount];
    }


    public boolean isNull(Column column)
    {
        return getValue(column) == null;
    }

    public boolean getBoolean(Column column)
    {
        return (Boolean)getValue(column);
    }

    public long getLong(Column column)
    {
        return (Long)getValue(column);
    }

    public double getDouble(Column column)
    {
        return (Double)getValue(column);
    }

    public String getString(Column column)
    {
        return (String)getValue(column);
    }

    public Instant getTimestamp(Column column)
    {
        return (Instant)getValue(column);
    }

    public Value getJson(Column column)
    {
        return (Value)getValue(column);
    }

    private Object getValue(Column column)
    {
        return values[column.getIndex()];
    }

    public void setValue(Column column, Object value)
    {
        values[column.getIndex()] = value;
    }
}
