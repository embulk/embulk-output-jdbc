package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.math.RoundingMode;

import com.google.common.math.DoubleMath;

import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;
import org.msgpack.value.Value;

public class IntColumnSetter
        extends ColumnSetter
{
    public IntColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue)
    {
        super(batch, column, defaultValue);
    }

    @Override
    public void nullValue() throws IOException, SQLException
    {
        defaultValue.setInt();
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setInt(v ? 1 : 0);
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
            defaultValue.setInt();
        } else {
            batch.setInt((int) v);
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
            defaultValue.setInt();
            return;
        }
        longValue(lv);
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        int iv;
        try {
            iv = Integer.parseInt(v);
        } catch (NumberFormatException e) {
            defaultValue.setInt();
            return;
        }
        batch.setInt(iv);
    }

    @Override
    public void timestampValue(Timestamp v) throws IOException, SQLException
    {
        defaultValue.setInt();
    }

    @Override
    public void jsonValue(Value v) throws IOException, SQLException
    {
        defaultValue.setInt();
    }
}
