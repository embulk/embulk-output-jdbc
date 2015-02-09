package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.math.RoundingMode;
import com.google.common.math.DoubleMath;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.batch.BatchInsert;

public class LongColumnSetter
        extends ColumnSetter
{
    public LongColumnSetter(BatchInsert batch, PageReader pageReader,
            JdbcColumn column)
    {
        super(batch, pageReader, column);
    }

    protected void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setLong(v ? 1L : 0L);
    }

    protected void longValue(long v) throws IOException, SQLException
    {
        batch.setLong(v);
    }

    protected void doubleValue(double v) throws IOException, SQLException
    {
        long lv;
        try {
            // TODO configurable rounding mode
            lv = DoubleMath.roundToLong(v, RoundingMode.HALF_UP);
        } catch (ArithmeticException ex) {
            // NaN / Infinite / -Infinite
            nullValue();
            return;
        }
        batch.setLong(lv);
    }

    protected void stringValue(String v) throws IOException, SQLException
    {
        long lv;
        try {
            lv = Long.parseLong(v);
        } catch (NumberFormatException e) {
            nullValue();
            return;
        }
        batch.setLong(lv);
    }

    protected void timestampValue(Timestamp v) throws IOException, SQLException
    {
        nullValue();
    }
}
