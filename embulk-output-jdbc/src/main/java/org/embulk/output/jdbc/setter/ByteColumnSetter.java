package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.math.RoundingMode;

import com.google.common.math.DoubleMath;

import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;
import org.msgpack.value.Value;

public class ByteColumnSetter
        extends ColumnSetter
{
    public ByteColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue)
    {
        super(batch, column, defaultValue);
    }

    @Override
    public void nullValue() throws IOException, SQLException
    {
        defaultValue.setByte();
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setByte(v ? (byte) 1 : (byte) 0);
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        if (v > Byte.MAX_VALUE || v < Byte.MIN_VALUE) {
            defaultValue.setByte();
        } else {
            batch.setByte((byte) v);
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
            defaultValue.setByte();
            return;
        }
        longValue(lv);
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        byte sv;
        try {
            sv = Byte.parseByte(v);
        } catch (NumberFormatException e) {
            defaultValue.setByte();
            return;
        }
        batch.setByte(sv);
    }

    @Override
    public void timestampValue(Timestamp v) throws IOException, SQLException
    {
        defaultValue.setByte();
    }

    @Override
    public void jsonValue(Value v) throws IOException, SQLException
    {
        defaultValue.setByte();
    }
}
