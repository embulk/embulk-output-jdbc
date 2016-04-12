package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.math.RoundingMode;

import com.google.common.math.DoubleMath;

import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;
import org.msgpack.value.Value;

public class LongColumnSetter
        extends ColumnSetter
{
    public LongColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue)
    {
        super(batch, column, defaultValue);
    }

    @Override
    public void nullValue() throws IOException, SQLException
    {
        defaultValue.setLong();
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setLong(v ? 1L : 0L);
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        batch.setLong(v);
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
            defaultValue.setLong();
            return;
        }
        batch.setLong(lv);
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        long lv;
        try {
            lv = Long.parseLong(v);
        } catch (NumberFormatException e) {
            defaultValue.setLong();
            return;
        }
        batch.setLong(lv);
    }

    @Override
    public void timestampValue(Timestamp v) throws IOException, SQLException
    {
        defaultValue.setLong();
    }

    @Override
    public void jsonValue(Value v) throws IOException, SQLException
    {
        defaultValue.setLong();
    }
}
