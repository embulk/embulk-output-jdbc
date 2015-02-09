package org.embulk.output.jdbc.batch;

import java.io.IOException;
import java.sql.Types;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLBatchInsert
        extends StandardBatchInsert
{
    public MySQLBatchInsert(PreparedStatement batch) throws IOException, SQLException
    {
        super(batch);
    }

    public void setFloat(float v) throws IOException, SQLException
    {
        if (Float.isNaN(v) || Float.isInfinite(v)) {
            setNull(Types.REAL);  // TODO get through argument
        } else {
            super.setFloat(v);
        }
    }

    public void setDouble(double v) throws IOException, SQLException
    {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            setNull(Types.DOUBLE);  // TODO get through argument
        } else {
            super.setDouble(v);
        }
    }
}
