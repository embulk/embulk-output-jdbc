package org.embulk.output.postgresql.setter;

import java.time.ZoneId;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcColumnOption;
import org.embulk.output.jdbc.setter.ColumnSetter;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.embulk.output.jdbc.setter.JsonColumnSetter;

public class PostgreSQLColumnSetterFactory
        extends ColumnSetterFactory
{
    public PostgreSQLColumnSetterFactory(final BatchInsert batch, final ZoneId defaultTimeZone)
    {
        super(batch, defaultTimeZone);
    }

    @Override
    public ColumnSetter newCoalesceColumnSetter(JdbcColumn column, JdbcColumnOption option)
    {
        if (column.getSimpleTypeName().equalsIgnoreCase("json") || column.getSimpleTypeName().equalsIgnoreCase("jsonb")) {
            // actually "JSON"/"JSONB"
            return new JsonColumnSetter(batch, column, newDefaultValueSetter(column, option));
        } else {
            return super.newCoalesceColumnSetter(column, option);
        }
    }

}
