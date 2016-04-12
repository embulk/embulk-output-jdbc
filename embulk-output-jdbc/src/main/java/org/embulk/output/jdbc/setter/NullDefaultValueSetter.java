package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class NullDefaultValueSetter
        extends DefaultValueSetter
{
    public NullDefaultValueSetter(BatchInsert batch, JdbcColumn column)
    {
        super(batch, column);
    }

    @Override
    public void setNull() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setBoolean() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setByte() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setShort() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setInt() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setLong() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setFloat() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setDouble() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setBigDecimal() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setString() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setNString() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setBytes() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setSqlDate() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setSqlTime() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setSqlTimestamp() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }

    @Override
    public void setJson() throws IOException, SQLException
    {
        batch.setNull(column.getSqlType());
    }
}
