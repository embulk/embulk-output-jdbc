package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class SqlTimestampColumnSetter
        extends ColumnSetter
{
    public SqlTimestampColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue)
    {
        super(batch, column, defaultValue);
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        defaultValue.setSqlTimestamp();
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        defaultValue.setSqlTimestamp();
    }

    @Override
    public void doubleValue(double v) throws IOException, SQLException
    {
        defaultValue.setSqlTimestamp();
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        defaultValue.setSqlTimestamp();
    }

    @Override
    public void timestampValue(Timestamp v) throws IOException, SQLException
    {
        java.sql.Timestamp t = new java.sql.Timestamp(v.toEpochMilli());
        t.setNanos(v.getNano());
        batch.setSqlTimestamp(t, getSqlType());
    }
}
