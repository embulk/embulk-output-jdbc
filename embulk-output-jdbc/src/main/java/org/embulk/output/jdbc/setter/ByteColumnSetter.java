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

public class ByteColumnSetter
        extends ColumnSetter
{
    public ByteColumnSetter(BatchInsert batch, PageReader pageReader,
            JdbcColumn column)
    {
        super(batch, pageReader, column);
    }

    @Override
    protected void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setByte(v ? (byte) 1 : (byte) 0);
    }

    @Override
    protected void longValue(long v) throws IOException, SQLException
    {
        if (v > Byte.MAX_VALUE || v < Byte.MIN_VALUE) {
            nullValue();
        } else {
            batch.setByte((byte) v);
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
        byte sv;
        try {
            sv = Byte.parseByte(v);
        } catch (NumberFormatException e) {
            nullValue();
            return;
        }
        batch.setByte(sv);
    }

    @Override
    protected void timestampValue(Timestamp v) throws IOException, SQLException
    {
        nullValue();
    }
}
