package org.embulk.output.postgresql.setter;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcColumnOption;
import org.embulk.output.jdbc.setter.ColumnSetter;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.embulk.output.jdbc.setter.JsonColumnSetter;
import org.embulk.output.jdbc.setter.StringColumnSetter;
import org.joda.time.DateTimeZone;

public class PostgreSQLColumnSetterFactory
        extends ColumnSetterFactory
{
    public PostgreSQLColumnSetterFactory(BatchInsert batch, DateTimeZone defaultTimeZone)
    {
        super(batch, defaultTimeZone);
    }

    @Override
    public ColumnSetter newCoalesceColumnSetter(JdbcColumn column, JdbcColumnOption option)
    {
        if (column.getSimpleTypeName().equalsIgnoreCase("json") || column.getSimpleTypeName().equalsIgnoreCase("jsonb")) {
            // actually "JSON"/"JSONB"
            return new JsonColumnSetter(batch, column, newDefaultValueSetter(column, option));
        } else if (column.getSimpleTypeName().equalsIgnoreCase("inet")) {
            return new StringColumnSetter(batch, column, newDefaultValueSetter(column, option), newTimestampFormatter(option));
        } else {
            return super.newCoalesceColumnSetter(column, option);
        }
    }

}
