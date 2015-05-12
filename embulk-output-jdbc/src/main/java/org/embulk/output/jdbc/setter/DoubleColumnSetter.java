package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class DoubleColumnSetter
        extends ColumnSetter
{
    public DoubleColumnSetter(BatchInsert batch, PageReader pageReader,
            JdbcColumn column)
    {
        super(batch, pageReader, column);
    }

    @Override
    protected void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setDouble(v ? 1.0 : 0.0);
    }

    @Override
    protected void longValue(long v) throws IOException, SQLException
    {
        batch.setDouble((double) v);
    }

    @Override
    protected void doubleValue(double v) throws IOException, SQLException
    {
        batch.setDouble(v);
    }

    @Override
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

    @Override
    protected void timestampValue(Timestamp v) throws IOException, SQLException
    {
        nullValue();
    }
}
