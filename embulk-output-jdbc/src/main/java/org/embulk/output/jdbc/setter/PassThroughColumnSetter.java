package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;

import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;
import org.msgpack.value.Value;

public class PassThroughColumnSetter
        extends ColumnSetter
{
    private final ZoneId zone;

    public PassThroughColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue,
            ZoneId zone)
    {
        super(batch, column, defaultValue);
        this.zone = zone;
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
    public void timestampValue(final Instant v) throws IOException, SQLException
    {
        batch.setSqlTimestamp(v, this.zone);
    }

    @Override
    public void jsonValue(Value v) throws IOException, SQLException
    {
        batch.setString(v.toJson());
    }
}
