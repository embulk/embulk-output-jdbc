package org.embulk.output.mysql;

import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcSchema;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLBatchUpsert extends MySQLBatchInsert {

    public MySQLBatchUpsert(MySQLOutputConnector connector) throws IOException, SQLException {
        super(connector);
    }

    @Override
    protected PreparedStatement newPreparedStatement(JdbcOutputConnection connection,
                                                     String loadTable, JdbcSchema insertSchema) throws SQLException
    {
        return connection.prepareUpsertSql(loadTable, insertSchema);
    }

}
