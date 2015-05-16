package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class NullColumnSetter
        extends ColumnSetter
{
    public NullColumnSetter(BatchInsert batch, JdbcColumn column)
    {
        super(batch, column);
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        nullValue();
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        nullValue();
    }

    @Override
    public void doubleValue(double v) throws IOException, SQLException
    {
        nullValue();
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
