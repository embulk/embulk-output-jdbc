package org.embulk.output.sqlserver.setter;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.setter.DefaultValueSetter;
import org.embulk.output.jdbc.setter.SqlTimeColumnSetter;

public class SQLServerSqlTimeColumnSetter
        extends SqlTimeColumnSetter
{
    public SQLServerSqlTimeColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue,
            ZoneId zone)
    {
        super(batch, column, defaultValue, zone);
    }

    @Override
    public void timestampValue(final Instant v) throws IOException, SQLException
    {
        // fractional precision of SQLServer TIME is 7, but that of java.sql.Time is only 3.
        batch.setSqlTimestamp(v, this.zone);
    }
}
