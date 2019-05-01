package org.embulk.output.db2;

import java.io.IOException;
import java.sql.SQLException;

import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.StandardBatchInsert;

import com.google.common.base.Optional;

public class DB2BatchInsert
        extends StandardBatchInsert
{
    public DB2BatchInsert(JdbcOutputConnector connector, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        super(connector, mergeConfig);
    }

    @Override
    public void flush() throws IOException, SQLException
    {
        try {
            super.flush();

        } catch (SQLException e) {
            if (e.getNextException() != null) {
                // SQLException of DB2 doesn't contain details.
                throw new SQLException(e.toString(), e.getNextException());
            }
            throw e;
        }
    }
}
