package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.spi.time.Timestamp;
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

    public abstract void timestampValue(Timestamp v) throws IOException, SQLException;

    public abstract void jsonValue(Value v) throws IOException, SQLException;
}
