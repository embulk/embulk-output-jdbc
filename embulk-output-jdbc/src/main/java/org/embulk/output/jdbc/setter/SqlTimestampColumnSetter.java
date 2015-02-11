package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.math.RoundingMode;
import com.google.common.math.DoubleMath;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class SqlTimestampColumnSetter
        extends ColumnSetter
{
    public SqlTimestampColumnSetter(BatchInsert batch, PageReader pageReader,
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
        // TODO parse time?
        nullValue();
    }

    protected void timestampValue(Timestamp v) throws IOException, SQLException
    {
        java.sql.Timestamp t = new java.sql.Timestamp(v.toEpochMilli());
        t.setNanos(v.getNano());
        batch.setSqlTimestamp(t, getSqlType());
    }
}
