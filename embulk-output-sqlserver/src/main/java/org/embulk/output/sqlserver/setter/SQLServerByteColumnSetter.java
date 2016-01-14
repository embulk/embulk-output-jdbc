package org.embulk.output.sqlserver.setter;

import java.io.IOException;
import java.sql.SQLException;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.setter.ByteColumnSetter;
import org.embulk.output.jdbc.setter.DefaultValueSetter;

public class SQLServerByteColumnSetter
        extends ByteColumnSetter
{
    private static final short MIN_VALUE = 0;
    private static final short MAX_VALUE = 255;

    public SQLServerByteColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue)
    {
        super(batch, column, defaultValue);
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        // SQLServer TINYINT value is from 0 to 255
        if (v > MAX_VALUE || v < MIN_VALUE) {
            defaultValue.setByte();
        } else {
            batch.setShort((short) v);
        }
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        short sv;
        try {
            sv = Short.parseShort(v);
        } catch (NumberFormatException e) {
            defaultValue.setByte();
            return;
        }
        longValue(sv);
    }
}
