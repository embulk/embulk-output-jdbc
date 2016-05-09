package org.embulk.output.mysql;

import java.util.List;
import java.io.IOException;
import java.sql.Types;
import java.sql.SQLException;
import com.google.common.base.Optional;
import org.embulk.output.jdbc.StandardBatchInsert;

public class MySQLBatchInsert
        extends StandardBatchInsert
{
    public MySQLBatchInsert(MySQLOutputConnector connector, Optional<List<String>> mergeKeys, Optional<String> onDuplicateKeyUpdateSql) throws IOException, SQLException
    {
        super(connector, mergeKeys, onDuplicateKeyUpdateSql);
    }

    @Override
    public void setFloat(float v) throws IOException, SQLException
    {
        if (Float.isNaN(v) || Float.isInfinite(v)) {
            setNull(Types.REAL);  // TODO get through argument
        } else {
            super.setFloat(v);
        }
    }

    @Override
    public void setDouble(double v) throws IOException, SQLException
    {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            setNull(Types.DOUBLE);  // TODO get through argument
        } else {
            super.setDouble(v);
        }
    }
}
