package org.embulk.output.jdbc.setter;

import java.math.BigDecimal;
import java.io.IOException;
import java.sql.SQLException;

import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;
import org.msgpack.value.Value;

public class BigDecimalColumnSetter
        extends ColumnSetter
{
    private static final BigDecimal ZERO = BigDecimal.valueOf(0L);
    private static final BigDecimal ONE = BigDecimal.valueOf(1L);

    public BigDecimalColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue)
    {
        super(batch, column, defaultValue);
    }

    @Override
    public void nullValue() throws IOException, SQLException
    {
        defaultValue.setBigDecimal();
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setBigDecimal(v ? ONE : ZERO);
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        batch.setBigDecimal(BigDecimal.valueOf(v));
    }

    @Override
    public void doubleValue(double v) throws IOException, SQLException
    {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            defaultValue.setBigDecimal();
        } else {
            batch.setBigDecimal(BigDecimal.valueOf(v));
        }
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        BigDecimal dv;
        try {
            dv = new BigDecimal(v);
        } catch (NumberFormatException ex) {
            defaultValue.setBigDecimal();
            return;
        }
        batch.setBigDecimal(dv);
    }

    @Override
    public void timestampValue(Timestamp v) throws IOException, SQLException
    {
        defaultValue.setBigDecimal();
    }

    @Override
    public void jsonValue(Value v) throws IOException, SQLException
    {
        defaultValue.setBigDecimal();
    }
}
