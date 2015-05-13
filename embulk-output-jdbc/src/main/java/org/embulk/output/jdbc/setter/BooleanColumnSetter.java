package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import com.google.common.collect.ImmutableSet;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class BooleanColumnSetter
        extends ColumnSetter
{
    public BooleanColumnSetter(BatchInsert batch, PageReader pageReader,
            JdbcColumn column)
    {
        super(batch, pageReader, column);
    }

    @Override
    protected void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setBoolean(v);
    }

    @Override
    protected void longValue(long v) throws IOException, SQLException
    {
        batch.setBoolean(v > 0);
    }

    @Override
    protected void doubleValue(double v) throws IOException, SQLException
    {
        batch.setBoolean(v > 0.0);
    }

    @Override
    protected void stringValue(String v) throws IOException, SQLException
    {
        nullValue();
    }

    @Override
    protected void timestampValue(Timestamp v) throws IOException, SQLException
    {
        nullValue();
    }
}
