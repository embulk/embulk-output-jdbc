package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;

import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;
import org.msgpack.value.Value;

public class SqlDateColumnSetter
        extends ColumnSetter
{
    protected final ZoneId zone;

    public SqlDateColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue,
            ZoneId zone)
    {
        super(batch, column, defaultValue);
        this.zone = zone;
    }

    @Override
    public void nullValue() throws IOException, SQLException
    {
        defaultValue.setSqlDate();
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        defaultValue.setSqlDate();
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        defaultValue.setSqlDate();
    }

    @Override
    public void doubleValue(double v) throws IOException, SQLException
    {
        defaultValue.setSqlDate();
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        defaultValue.setSqlDate();
    }

    @Override
    public void timestampValue(final Instant v) throws IOException, SQLException
    {
        batch.setSqlDate(v, this.zone);
    }

    @Override
    public void jsonValue(Value v) throws IOException, SQLException
    {
        defaultValue.setSqlDate();
    }
}
