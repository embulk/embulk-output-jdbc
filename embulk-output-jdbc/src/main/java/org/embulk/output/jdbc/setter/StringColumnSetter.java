package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class StringColumnSetter
        extends ColumnSetter
{
    private final TimestampFormatter timestampFormatter;

    public StringColumnSetter(BatchInsert batch, PageReader pageReader,
            JdbcColumn column, TimestampFormatter timestampFormatter)
    {
        super(batch, pageReader, column);
        this.timestampFormatter = timestampFormatter;
    }

    @Override
    protected void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setString(Boolean.toString(v));
    }

    @Override
    protected void longValue(long v) throws IOException, SQLException
    {
        batch.setString(Long.toString(v));
    }

    @Override
    protected void doubleValue(double v) throws IOException, SQLException
    {
        batch.setString(Double.toString(v));
    }

    @Override
    protected void stringValue(String v) throws IOException, SQLException
    {
        batch.setString(v);
    }

    @Override
    protected void timestampValue(Timestamp v) throws IOException, SQLException
    {
        batch.setString(timestampFormatter.format(v));
    }
}
