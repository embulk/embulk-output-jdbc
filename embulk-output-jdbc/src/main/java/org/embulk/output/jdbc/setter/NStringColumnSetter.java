package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;

import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

public class NStringColumnSetter
        extends ColumnSetter
{
    private final TimestampFormatter timestampFormatter;

    public NStringColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue,
            TimestampFormatter timestampFormatter)
    {
        super(batch, column, defaultValue);
        this.timestampFormatter = timestampFormatter;
    }

    @Override
    public void nullValue() throws IOException, SQLException
    {
        defaultValue.setNString();
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setNString(Boolean.toString(v));
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        batch.setNString(Long.toString(v));
    }

    @Override
    public void doubleValue(double v) throws IOException, SQLException
    {
        batch.setNString(Double.toString(v));
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        batch.setNString(v);
    }

    @Override
    public void timestampValue(final Instant v) throws IOException, SQLException
    {
        batch.setNString(timestampFormatter.format(v));
    }

    @Override
    public void jsonValue(Value v) throws IOException, SQLException
    {
        batch.setNString(v.toJson());
    }
}
