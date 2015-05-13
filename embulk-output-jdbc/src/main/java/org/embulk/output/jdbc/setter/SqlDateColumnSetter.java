package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Date;
import org.joda.time.DateTimeZone;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class SqlDateColumnSetter
        extends ColumnSetter
{
    private final DateTimeZone timeZone;

    public SqlDateColumnSetter(BatchInsert batch, PageReader pageReader,
            JdbcColumn column, DateTimeZone timeZone)
    {
        super(batch, pageReader, column);
        this.timeZone = timeZone;
    }

    @Override
    protected void booleanValue(boolean v) throws IOException, SQLException
    {
        nullValue();
    }

    @Override
    protected void longValue(long v) throws IOException, SQLException
    {
        nullValue();
    }

    @Override
    protected void doubleValue(double v) throws IOException, SQLException
    {
        nullValue();
    }

    @Override
    protected void stringValue(String v) throws IOException, SQLException
    {
        nullValue();
    }

    @Override
    protected void timestampValue(Timestamp v) throws IOException, SQLException
    {
        // JavaDoc of java.sql.Time says:
        // >> To conform with the definition of SQL DATE, the millisecond values wrapped by a java.sql.Date instance must be 'normalized' by setting the hours, minutes, seconds, and milliseconds to zero in the particular time zone with which the instance is associated.
        long normalized = timeZone.convertUTCToLocal(v.toEpochMilli());
        Date d = new Date(normalized);
        batch.setSqlDate(d, getSqlType());
    }
}
