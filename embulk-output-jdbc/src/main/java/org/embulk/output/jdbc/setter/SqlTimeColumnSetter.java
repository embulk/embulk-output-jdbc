package org.embulk.output.jdbc.setter;

import java.util.Calendar;
import java.io.IOException;
import java.sql.SQLException;

import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;
import org.msgpack.value.Value;

public class SqlTimeColumnSetter
        extends ColumnSetter
{
    protected final Calendar calendar;

    public SqlTimeColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue,
            Calendar calendar)
    {
        super(batch, column, defaultValue);
        this.calendar = calendar;
    }

    @Override
    public void nullValue() throws IOException, SQLException
    {
        defaultValue.setSqlTime();
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        defaultValue.setSqlTime();
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        defaultValue.setSqlTime();
    }

    @Override
    public void doubleValue(double v) throws IOException, SQLException
    {
        defaultValue.setSqlTime();
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        defaultValue.setSqlTime();
    }

    @Override
    public void timestampValue(Timestamp v) throws IOException, SQLException
    {
        batch.setSqlTime(v, calendar);
    }

    @Override
    public void jsonValue(Value v) throws IOException, SQLException
    {
        defaultValue.setSqlTime();
    }
}
