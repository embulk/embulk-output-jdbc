package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.batch.BatchInsert;

public class NullColumnSetter
        extends ColumnSetter
{
    public NullColumnSetter(BatchInsert batch, PageReader pageReader,
            JdbcColumn column)
    {
        super(batch, pageReader, column);
    }

    protected void booleanValue(boolean v) throws IOException, SQLException
    {
        nullValue();
    }

    protected void longValue(long v) throws IOException, SQLException
    {
        nullValue();
    }

    protected void doubleValue(double v) throws IOException, SQLException
    {
        nullValue();
    }

    protected void stringValue(String v) throws IOException, SQLException
    {
        nullValue();
    }

    protected void timestampValue(Timestamp v) throws IOException, SQLException
    {
        nullValue();
    }
}
