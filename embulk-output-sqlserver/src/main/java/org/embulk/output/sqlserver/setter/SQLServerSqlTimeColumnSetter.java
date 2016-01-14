package org.embulk.output.sqlserver.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.setter.DefaultValueSetter;
import org.embulk.output.jdbc.setter.SqlTimeColumnSetter;
import org.embulk.spi.time.Timestamp;

public class SQLServerSqlTimeColumnSetter
        extends SqlTimeColumnSetter
{
    public SQLServerSqlTimeColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue,
            Calendar calendar)
    {
        super(batch, column, defaultValue, calendar);
    }

    @Override
    public void timestampValue(Timestamp v) throws IOException, SQLException
    {
        // fractional precision of SQLServer TIME is 7, but that of java.sql.Time is only 3.
        batch.setSqlTimestamp(v, calendar);
    }
}
