package org.embulk.output.postgresql;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;

import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.TableIdentifier;
import org.embulk.spi.Exec;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;

public class PostgreSQLCopyBatchInsert
        extends AbstractPostgreSQLCopyBatchInsert
{
    private final Logger logger = Exec.getLogger(PostgreSQLCopyBatchInsert.class);
    private final JdbcOutputConnector connector;

    private PostgreSQLOutputConnection connection = null;
    private CopyManager copyManager = null;
    private String copySql = null;
    private long totalRows;

    public PostgreSQLCopyBatchInsert(JdbcOutputConnector connector) throws IOException, SQLException
    {
        super();
        this.connector = connector;
    }

    @Override
    public void prepare(TableIdentifier loadTable, JdbcSchema insertSchema) throws SQLException
    {
        this.connection = (PostgreSQLOutputConnection)connector.connect(true);
        this.copySql = connection.buildCopySql(loadTable, insertSchema);
        this.copyManager = connection.newCopyManager();
        logger.info("Copy SQL: "+copySql);
    }

    @Override
    public void flush() throws IOException, SQLException
    {
        File file = closeCurrentFile();  // flush buffered data in writer
        if (getBatchWeight() == 0) return;

        logger.info(String.format("Loading %,d rows (%,d bytes)", batchRows, file.length()));
        long startTime = System.currentTimeMillis();
        FileInputStream in = new FileInputStream(file);
        try {
            // TODO check age of connection and call isValid if it's old and reconnect if it's invalid
            copyManager.copyIn(copySql, in);
        } finally {
            in.close();
        }
        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;

        totalRows += batchRows;
        batchRows = 0;
        logger.info(String.format("> %.2f seconds (loaded %,d rows in total)", seconds, totalRows));

        openNewFile();
        file.delete();
    }

    @Override
    public void finish() throws IOException ,SQLException
    {
    }

    @Override
    public void close() throws IOException, SQLException
    {
        closeCurrentFile().delete();
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
}
