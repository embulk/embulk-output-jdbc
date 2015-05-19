package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import org.embulk.spi.Column;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

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

    public void nullValue() throws IOException, SQLException
    {
        batch.setNull(getSqlType());
    }

    public abstract void booleanValue(boolean v) throws IOException, SQLException;

    public abstract void longValue(long v) throws IOException, SQLException;

    public abstract void doubleValue(double v) throws IOException, SQLException;

    public abstract void stringValue(String v) throws IOException, SQLException;

    public abstract void timestampValue(Timestamp v) throws IOException, SQLException;
}
