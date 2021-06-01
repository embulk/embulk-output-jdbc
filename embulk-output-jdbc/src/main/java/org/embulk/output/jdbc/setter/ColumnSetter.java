package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.msgpack.value.Value;

public abstract class ColumnSetter
{
    protected final BatchInsert batch;
    protected final JdbcColumn column;
    protected final DefaultValueSetter defaultValue;

    public ColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue)
    {
        this.batch = batch;
        this.column = column;
        this.defaultValue = defaultValue;
    }

    public JdbcColumn getColumn()
    {
        return column;
    }

    public int getSqlType()
    {
        return column.getSqlType();
    }

    public abstract void nullValue() throws IOException, SQLException;

    public abstract void booleanValue(boolean v) throws IOException, SQLException;

    public abstract void longValue(long v) throws IOException, SQLException;

    public abstract void doubleValue(double v) throws IOException, SQLException;

    public abstract void stringValue(String v) throws IOException, SQLException;

    public abstract void timestampValue(final Instant v) throws IOException, SQLException;

    public abstract void jsonValue(Value v) throws IOException, SQLException;

    final long roundDoubleToLong(final double v) {
        if (Math.getExponent(v) > Double.MAX_EXPONENT) {
            throw new ArithmeticException("input is infinite or NaN");
        }
        final double z = Math.rint(v);
        if (Math.abs(v - z) == 0.5) {
            return (long) (v + Math.copySign(0.5, v));
        } else {
            return (long) z;
        }
    }
}
