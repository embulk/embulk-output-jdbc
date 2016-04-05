package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public abstract class DefaultValueSetter
{
    protected final BatchInsert batch;
    protected final JdbcColumn column;

    public DefaultValueSetter(BatchInsert batch, JdbcColumn column)
    {
        this.batch = batch;
        this.column = column;
    }

    public abstract void setNull() throws IOException, SQLException;

    public abstract void setBoolean() throws IOException, SQLException;

    public abstract void setByte() throws IOException, SQLException;

    public abstract void setShort() throws IOException, SQLException;

    public abstract void setInt() throws IOException, SQLException;

    public abstract void setLong() throws IOException, SQLException;

    public abstract void setFloat() throws IOException, SQLException;

    public abstract void setDouble() throws IOException, SQLException;

    public abstract void setBigDecimal() throws IOException, SQLException;

    public abstract void setString() throws IOException, SQLException;

    public abstract void setNString() throws IOException, SQLException;

    public abstract void setBytes() throws IOException, SQLException;

    public abstract void setSqlDate() throws IOException, SQLException;

    public abstract void setSqlTime() throws IOException, SQLException;

    public abstract void setSqlTimestamp() throws IOException, SQLException;

    public abstract void setJson() throws IOException, SQLException;
}
