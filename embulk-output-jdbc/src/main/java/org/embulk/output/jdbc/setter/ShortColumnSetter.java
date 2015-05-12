package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.math.RoundingMode;
import com.google.common.math.DoubleMath;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class ShortColumnSetter
        extends ColumnSetter
{
    public ShortColumnSetter(BatchInsert batch, PageReader pageReader,
            JdbcColumn column)
    {
        super(batch, pageReader, column);
    }

    @Override
    protected void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setShort(v ? (short) 1 : (short) 0);
    }

    @Override
    protected void longValue(long v) throws IOException, SQLException
    {
        if (v > Short.MAX_VALUE || v < Short.MIN_VALUE) {
            nullValue();
        } else {
            batch.setShort((short) v);
        }
    }

    @Override
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
        longValue(lv);
    }

    @Override
    protected void stringValue(String v) throws IOException, SQLException
    {
        short sv;
        try {
            sv = Short.parseShort(v);
        } catch (NumberFormatException e) {
            nullValue();
            return;
        }
        batch.setShort(sv);
    }

    @Override
    protected void timestampValue(Timestamp v) throws IOException, SQLException
    {
        nullValue();
    }
}
