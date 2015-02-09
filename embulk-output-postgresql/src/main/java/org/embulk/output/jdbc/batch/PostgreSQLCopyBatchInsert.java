package org.embulk.output.jdbc.batch;

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.sql.SQLException;
import java.sql.Connection;
import org.slf4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.embulk.spi.Exec;

public class PostgreSQLCopyBatchInsert
        extends AbstractPostgreSQLCopyBatchInsert
{
    private final Logger logger = Exec.getLogger(PostgreSQLCopyBatchInsert.class);

    private final CopyManager copyManager;
    private final String copySQL;
    private long totalRows;

    public PostgreSQLCopyBatchInsert(Connection connection, File tempFile, String copySQL) throws IOException, SQLException
    {
        super(tempFile);
        this.copyManager = new CopyManager((BaseConnection) connection);
        this.copySQL = copySQL;
    }

    @Override
    public void close() throws IOException
    {
        deleteFile();
        closeFile();
    }

    @Override
    public void flush() throws IOException, SQLException
    {
        closeFile();  // flush buffered data in writer

        logger.info(String.format("Loading %,d rows (%,d bytes)", batchRows, file.length()));
        long startTime = System.currentTimeMillis();
        FileInputStream in = new FileInputStream(file);
        try {
            copyManager.copyIn(copySQL, in);
        } finally {
            in.close();
        }
        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;

        totalRows += batchRows;
        logger.info(String.format("> %.2f seconds (loaded %,d rows in total)", seconds, totalRows));
        reopenFile();
        batchRows = 0;
    }

    public void finish() throws IOException, SQLException
    {
        closeFile();  // flush buffered data in writer
        if (getBatchWeight() != 0) {
            flush();
        }
    }

    public String getCopySQL()
    {
        return this.copySQL;
    }
}
