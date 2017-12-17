package org.embulk.output.sqlserver;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.StandardBatchInsert;
import org.embulk.output.jdbc.TableIdentifier;
import org.embulk.output.jdbc.TimestampFormat;
import org.embulk.output.sqlserver.nativeclient.NativeClientWrapper;
import org.embulk.spi.Exec;
import org.embulk.spi.time.Timestamp;
import org.slf4j.Logger;

import com.google.common.base.Optional;

public class NativeBatchInsert implements BatchInsert
{
    private final Logger logger = Exec.getLogger(StandardBatchInsert.class);

    private NativeClientWrapper client = new NativeClientWrapper();

    private final String server;
    private final int port;
    private final Optional<String> instance;
    private final String database;
    private final Optional<String> user;
    private final Optional<String> password;
    private final Optional<String> nativeDriverName;
    private final String databaseEncoding;

    private int batchWeight;
    private int batchRows;
    private long totalRows;

    private int columnCount;
    private int lastColumnIndex;

    private DateFormat[] formats;


    public NativeBatchInsert(String server, int port, Optional<String> instance,
            String database, Optional<String> user, Optional<String> password,
            Optional<String> nativeDriverName, String databaseEncoding)
    {
        this.server = server;
        this.port = port;
        this.instance = instance;
        this.database = database;
        this.user = user;
        this.password = password;
        this.nativeDriverName = nativeDriverName;
        this.databaseEncoding = databaseEncoding;
        lastColumnIndex = 0;
    }


    @Override
    public void prepare(TableIdentifier loadTable, JdbcSchema insertSchema) throws SQLException
    {
        columnCount = insertSchema.getCount();
        client.open(server, port, instance, database, user, password, loadTable.getTableName(), nativeDriverName, databaseEncoding);
        formats = new DateFormat[insertSchema.getCount()];
        for (int i = 0; i < insertSchema.getCount(); i++) {
            JdbcColumn column = insertSchema.getColumn(i);
            switch (column.getSqlType()) {
                case Types.DATE:
                    formats[i] = new SimpleDateFormat("yyyy-MM-dd");
                    break;

                case Types.TIME:
                    formats[i] = new TimestampFormat("HH:mm:ss", column.getScaleTypeParameter());
                    break;

                case Types.TIMESTAMP:
                    if (column.getSimpleTypeName().equals("SMALLDATETIME")) {
                        formats[i] = new SmallDateTimeFormat("yyyy-MM-dd HH:mm:ss");
                    } else {
                        formats[i] = new TimestampFormat("yyyy-MM-dd HH:mm:ss", column.getScaleTypeParameter());
                    }
                    break;

                default:
                    break;
            }
        }
    }

    @Override
    public int getBatchWeight()
    {
        return batchWeight;
    }

    @Override
    public void add() throws IOException, SQLException
    {
        client.sendRow();

        batchRows++;
        batchWeight += 32;  // add weight as overhead of each rows
    }

    private int nextColumnIndex()
    {
        int nextColumnIndex = lastColumnIndex + 1;
        if (nextColumnIndex == columnCount) {
            lastColumnIndex = 0;
        } else {
            lastColumnIndex++;
        }
        return nextColumnIndex;
    }

    @Override
    public void setNull(int sqlType) throws IOException, SQLException
    {
        batchWeight += client.bindNull(nextColumnIndex(), sqlType);
    }

    @Override
    public void setBoolean(boolean v) throws IOException, SQLException
    {
        batchWeight += client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setByte(byte v) throws IOException, SQLException
    {
        batchWeight += client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setShort(short v) throws IOException, SQLException
    {
        batchWeight += client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setInt(int v) throws IOException, SQLException
    {
        batchWeight += client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setLong(long v) throws IOException, SQLException
    {
        batchWeight += client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setFloat(float v) throws IOException, SQLException
    {
        batchWeight += client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setDouble(double v) throws IOException, SQLException
    {
        batchWeight += client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setBigDecimal(BigDecimal v) throws IOException, SQLException
    {
        batchWeight += client.bindValue(nextColumnIndex(), v.toPlainString());
    }

    @Override
    public void setString(String v) throws IOException, SQLException
    {
        batchWeight += client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setNString(String v) throws IOException, SQLException
    {
        batchWeight += client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setBytes(byte[] v) throws IOException, SQLException
    {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setSqlDate(Timestamp v, Calendar cal) throws IOException, SQLException
    {
        setSqlTimestamp(v, cal);
    }

    @Override
    public void setSqlTime(Timestamp v, Calendar cal) throws IOException, SQLException
    {
        setSqlTimestamp(v, cal);
    }

    @Override
    public void setSqlTimestamp(Timestamp v, Calendar cal) throws IOException, SQLException
    {
        int columnIndex = nextColumnIndex();
        DateFormat format = formats[columnIndex - 1];
        format.setCalendar(cal);

        java.sql.Timestamp timestamp = new java.sql.Timestamp(v.toEpochMilli());
        timestamp.setNanos(v.getNano());

        batchWeight += client.bindValue(columnIndex, format.format(timestamp));
    }

    @Override
    public void flush() throws IOException, SQLException
    {
        logger.info(String.format("Loading %,d rows", batchRows));
        long startTime = System.currentTimeMillis();

        client.commit(false);

        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
        totalRows += batchRows;

        logger.info(String.format("> %.2f seconds (loaded %,d rows in total)", seconds, totalRows));

        batchRows = 0;
        batchWeight = 0;
    }

    @Override
    public void finish() throws IOException, SQLException
    {
        if (getBatchWeight() != 0) {
            flush();
        }
        client.commit(true);
    }

    @Override
    public void close() throws IOException, SQLException
    {
        client.close();
    }

}
