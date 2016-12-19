package org.embulk.output.jdbc;

import java.util.Calendar;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Time;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.Exec;

public class StandardBatchInsert
        implements BatchInsert
{
    private final Logger logger = Exec.getLogger(StandardBatchInsert.class);

    private final JdbcOutputConnector connector;
    private final Optional<MergeConfig> mergeConfig;

    private JdbcOutputConnection connection;
    private PreparedStatement batch;
    private int index;
    private int batchWeight;
    private int batchRows;
    private long totalRows;

    public StandardBatchInsert(JdbcOutputConnector connector, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        this.connector = connector;
        this.mergeConfig = mergeConfig;
    }

    public void prepare(String loadTable, JdbcSchema insertSchema) throws SQLException
    {
        this.connection = connector.connect(true);
        this.index = 1;  // PreparedStatement index begings from 1
        this.batchRows = 0;
        this.totalRows = 0;
        this.batch = prepareStatement(loadTable, insertSchema);
        batch.clearBatch();
    }

    protected PreparedStatement prepareStatement(String loadTable, JdbcSchema insertSchema) throws SQLException
    {
        return connection.prepareBatchInsertStatement(loadTable, insertSchema, mergeConfig);
    }

    public int getBatchWeight()
    {
        return batchWeight;
    }

    public void add() throws IOException, SQLException
    {
        batch.addBatch();
        index = 1;  // PreparedStatement index begins from 1
        batchRows++;
        batchWeight += 32;  // add weight as overhead of each rows
    }

    public void close() throws IOException, SQLException
    {
        if (connection != null) {
            connection.close();
        }
    }

    public void flush() throws IOException, SQLException
    {
        logger.info(String.format("Loading %,d rows", batchRows));
        long startTime = System.currentTimeMillis();
        batch.executeBatch();  // here can't use returned value because MySQL Connector/J returns SUCCESS_NO_INFO as a batch result
        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;

        totalRows += batchRows;
        logger.info(String.format("> %.2f seconds (loaded %,d rows in total)", seconds, totalRows));
        batch.clearBatch();
        batchRows = 0;
        batchWeight = 0;
    }

    public void finish() throws IOException, SQLException
    {
        if (getBatchWeight() != 0) {
            flush();
        }
    }

    public void setNull(int sqlType) throws IOException, SQLException
    {
        batch.setNull(index, sqlType);
        nextColumn(0);
    }

    public void setBoolean(boolean v) throws IOException, SQLException
    {
        batch.setBoolean(index, v);
        nextColumn(1);
    }

    public void setByte(byte v) throws IOException, SQLException
    {
        batch.setByte(index, v);
        nextColumn(1);
    }

    public void setShort(short v) throws IOException, SQLException
    {
        batch.setShort(index, v);
        nextColumn(2);
    }

    public void setInt(int v) throws IOException, SQLException
    {
        batch.setInt(index, v);
        nextColumn(4);
    }

    public void setLong(long v) throws IOException, SQLException
    {
        batch.setLong(index, v);
        nextColumn(8);
    }

    public void setFloat(float v) throws IOException, SQLException
    {
        batch.setFloat(index, v);
        nextColumn(4);
    }

    public void setDouble(double v) throws IOException, SQLException
    {
        batch.setDouble(index, v);
        nextColumn(8);
    }

    public void setBigDecimal(BigDecimal v) throws IOException, SQLException
    {
        // use estimated number of necessary bytes + 8 byte for the weight
        // assuming one place needs 4 bits. ceil(v.precision() / 2.0) + 8
        batch.setBigDecimal(index, v);
        nextColumn((v.precision() & ~2) / 2 + 8);
    }

    public void setString(String v) throws IOException, SQLException
    {
        batch.setString(index, v);
        // estimate all chracters use 2 bytes; almost enough for the worst case
        nextColumn(v.length() * 2 + 4);
    }

    public void setNString(String v) throws IOException, SQLException
    {
        batch.setNString(index, v);
        // estimate all chracters use 2 bytes; almost enough for the worst case
        nextColumn(v.length() * 2 + 4);
    }

    public void setBytes(byte[] v) throws IOException, SQLException
    {
        batch.setBytes(index, v);
        nextColumn(v.length + 4);
    }

    public void setSqlDate(Timestamp v, Calendar cal) throws IOException, SQLException
    {
        // JavaDoc of java.sql.Time says:
        // >> To conform with the definition of SQL DATE, the millisecond values wrapped by a java.sql.Date instance must be 'normalized' by setting the hours, minutes, seconds, and milliseconds to zero in the particular time zone with which the instance is associated.
        cal.setTimeInMillis(v.getEpochSecond() * 1000);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        Date normalized = new Date(cal.getTimeInMillis());
        batch.setDate(index, normalized, cal);
        nextColumn(32);
    }

    public void setSqlTime(Timestamp v, Calendar cal) throws IOException, SQLException
    {
        Time t = new Time(v.toEpochMilli());
        batch.setTime(index, t, cal);
        nextColumn(32);
    }

    public void setSqlTimestamp(Timestamp v, Calendar cal) throws IOException, SQLException
    {
        java.sql.Timestamp t = new java.sql.Timestamp(v.toEpochMilli());
        t.setNanos(v.getNano());
        batch.setTimestamp(index, t, cal);
        nextColumn(32);
    }

    private void nextColumn(int weight)
    {
        index++;
        batchWeight += weight + 4;  // add weight as overhead of each columns
    }
}
