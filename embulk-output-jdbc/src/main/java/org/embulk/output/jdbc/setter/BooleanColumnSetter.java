package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import com.google.common.collect.ImmutableSet;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.batch.BatchInsert;

public class BooleanColumnSetter
        extends ColumnSetter
{
    private static final ImmutableSet<String> trueStrings =
        ImmutableSet.<String>of(
                "true", "True", "TRUE",
                "yes", "Yes", "YES",
                "y", "Y",
                "on", "On", "ON",
                "1");

    public BooleanColumnSetter(BatchInsert batch, PageReader pageReader,
            JdbcColumn column)
    {
        super(batch, pageReader, column);
    }

    protected void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setBoolean(v);
    }

    protected void longValue(long v) throws IOException, SQLException
    {
        batch.setBoolean(v > 0);
    }

    protected void doubleValue(double v) throws IOException, SQLException
    {
        batch.setBoolean(v > 0.0);
    }

    protected void stringValue(String v) throws IOException, SQLException
    {
        batch.setBoolean(trueStrings.contains(v));
    }

    protected void timestampValue(Timestamp v) throws IOException, SQLException
    {
        nullValue();
    }
}
