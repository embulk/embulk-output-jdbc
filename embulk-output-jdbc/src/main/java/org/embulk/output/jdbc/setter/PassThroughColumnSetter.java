package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class PassThroughColumnSetter
        extends ColumnSetter
{
    private final TimestampFormatter timestampFormatter;

    public PassThroughColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue,
            TimestampFormatter timestampFormatter)
    {
        super(batch, column, defaultValue);
        this.timestampFormatter = timestampFormatter;
    }

    @Override
    public void nullValue() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setBoolean(v);
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        batch.setLong(v);
    }

    @Override
    public void doubleValue(double v) throws IOException, SQLException
    {
        batch.setDouble(v);
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        batch.setString(v);
    }

    @Override
    public void timestampValue(Timestamp v) throws IOException, SQLException
    {
        java.sql.Timestamp t = new java.sql.Timestamp(v.toEpochMilli());
        t.setNanos(v.getNano());
        batch.setSqlTimestamp(t, getSqlType());
    }
}
