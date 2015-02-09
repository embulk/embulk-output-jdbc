package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.batch.BatchInsert;

public class DoubleColumnSetter
        extends ColumnSetter
{
    public DoubleColumnSetter(BatchInsert batch, PageReader pageReader,
            JdbcColumn column)
    {
        super(batch, pageReader, column);
    }

    protected void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setDouble(v ? 1.0 : 0.0);
    }

    protected void longValue(long v) throws IOException, SQLException
    {
        batch.setDouble((double) v);
    }

    protected void doubleValue(double v) throws IOException, SQLException
    {
        batch.setDouble(v);
    }

    protected void stringValue(String v) throws IOException, SQLException
    {
        double dv;
        try {
            dv = Double.parseDouble(v);
        } catch (NumberFormatException e) {
            nullValue();
            return;
        }
        batch.setDouble(dv);
    }

    protected void timestampValue(Timestamp v) throws IOException, SQLException
    {
        nullValue();
    }
}
