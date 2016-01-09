package org.embulk.output.sqlserver.setter;

import java.sql.Types;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcColumnOption;
import org.embulk.output.jdbc.setter.ColumnSetter;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.joda.time.DateTimeZone;

public class SQLServerColumnSetterFactory
        extends ColumnSetterFactory
{
    public SQLServerColumnSetterFactory(BatchInsert batch, DateTimeZone defaultTimeZone)
    {
        super(batch, defaultTimeZone);
    }

    @Override
    public ColumnSetter newCoalesceColumnSetter(JdbcColumn column, JdbcColumnOption option)
    {
        if (column.getSqlType() == Types.TINYINT) {
            return new SQLServerByteColumnSetter(batch, column, newDefaultValueSetter(column, option));
        }
        return super.newCoalesceColumnSetter(column, option);
    }

    @Override
    public ColumnSetter newColumnSetter(JdbcColumn column, JdbcColumnOption option)
    {
        if (option.getValueType().equals("byte")) {
            return new SQLServerByteColumnSetter(batch, column, newDefaultValueSetter(column, option));
        }
        return super.newColumnSetter(column, option);
    }

}
