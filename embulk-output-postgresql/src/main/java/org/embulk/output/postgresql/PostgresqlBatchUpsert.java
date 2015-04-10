package org.embulk.output.postgresql;

import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.StandardBatchInsert;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresqlBatchUpsert extends StandardBatchInsert {

    public PostgresqlBatchUpsert(JdbcOutputConnector connector) throws IOException, SQLException {
        super(connector);
    }

    protected PreparedStatement newPreparedStatement(JdbcOutputConnection connection,
                                                     String loadTable, JdbcSchema insertSchema) throws SQLException
    {
        return connection.prepareUpsertSql(loadTable, insertSchema);
    }

}
