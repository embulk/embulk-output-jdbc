package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.math.RoundingMode;

import com.google.common.math.DoubleMath;

import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;
import org.msgpack.value.Value;

public class ShortColumnSetter
        extends ColumnSetter
{
    public ShortColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue)
    {
        super(batch, column, defaultValue);
    }

    @Override
    public void nullValue() throws IOException, SQLException
    {
        defaultValue.setShort();
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setShort(v ? (short) 1 : (short) 0);
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        if (v > Short.MAX_VALUE || v < Short.MIN_VALUE) {
            defaultValue.setShort();
        } else {
            batch.setShort((short) v);
        }
    }

    @Override
    public void doubleValue(double v) throws IOException, SQLException
    {
        long lv;
        try {
            // TODO configurable rounding mode
            lv = DoubleMath.roundToLong(v, RoundingMode.HALF_UP);
        } catch (ArithmeticException ex) {
            // NaN / Infinite / -Infinite
            defaultValue.setShort();
            return;
        }
        longValue(lv);
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        short sv;
        try {
            sv = Short.parseShort(v);
        } catch (NumberFormatException e) {
            defaultValue.setShort();
            return;
        }
        batch.setShort(sv);
    }

    @Override
    public void timestampValue(Timestamp v) throws IOException, SQLException
    {
        defaultValue.setShort();
    }

    @Override
    public void jsonValue(Value v) throws IOException, SQLException
    {
        defaultValue.setShort();
    }
}
