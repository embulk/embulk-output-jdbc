package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import com.google.common.collect.ImmutableSet;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class BooleanColumnSetter
        extends ColumnSetter
{
    public BooleanColumnSetter(BatchInsert batch, JdbcColumn column)
    {
        super(batch, column);
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setBoolean(v);
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        batch.setBoolean(v > 0);
    }

    @Override
    public void doubleValue(double v) throws IOException, SQLException
    {
        batch.setBoolean(v > 0.0);
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        nullValue();
    }

    @Override
    public void timestampValue(Timestamp v) throws IOException, SQLException
    {
        nullValue();
    }
}
