package org.embulk.output.oracle;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.oracle.oci.ColumnDefinition;
import org.embulk.output.oracle.oci.OCIManager;
import org.embulk.output.oracle.oci.OCIWrapper;
import org.embulk.output.oracle.oci.RowBuffer;
import org.embulk.output.oracle.oci.TableDefinition;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

public class DirectBatchInsert implements BatchInsert
{
    private static OCIManager ociManager = new OCIManager();

    private final Logger logger = Exec.getLogger(DirectBatchInsert.class);

    private final List<String> ociKey;
    private final String database;
    private final String user;
    private final String password;
    private final String table;
    private final OracleCharset charset;
    private final int batchSize;
    private RowBuffer buffer;
    private long totalRows;
    private int rowSize;
    private int batchWeight;

    private DateFormat[] formats;


    public DirectBatchInsert(String database, String user, String password, String table, OracleCharset charset, int batchSize)
    {
        this.database = database;
        this.user = user;
        this.password = password;
        this.table = table;
        this.charset = charset;
        this.batchSize = batchSize;

        ociKey = Arrays.asList(database, user, table);
    }

    @Override
    public void prepare(String loadTable, JdbcSchema insertSchema) throws SQLException
    {

        /*
         * available mappings
         *
         * boolean      -> unused
         * byte         -> unused
         * short        -> unused
         * int          -> unused
         * long         -> unused
         * BigDecimal   -> unused
         * String       -> CHAR,VARCHAR,LONGVARCHAR,CLOB, NCHAR,NVARCHAR,NCLOB, NUMBER
         * NString      -> unused
         * bytes        -> unused
         * SqlDate      -> unused
         * SqlTime      -> unused
         * SqlTimeStamp -> TIMESTAMP
         *
         */

        formats = new DateFormat[insertSchema.getCount()];
        List<ColumnDefinition> columns = new ArrayList<ColumnDefinition>();
        Timestamp dummy = new Timestamp(System.currentTimeMillis());
        for (int i = 0; i < insertSchema.getCount(); i++) {
            JdbcColumn insertColumn = insertSchema.getColumn(i);
            switch (insertColumn.getSqlType()) {
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CLOB:
                    // TODO: CHAR(n CHAR)
                    columns.add(new ColumnDefinition(insertColumn.getName(),
                            ColumnDefinition.SQLT_CHR,
                            insertColumn.getSizeTypeParameter()));
                    break;

                case Types.DECIMAL:
                    // sign + size
                    int size = 1 + insertColumn.getSizeTypeParameter();
                    if (insertColumn.getSizeTypeParameter() > 0) {
                        // decimal point
                        size += 1;
                    }
                    columns.add(new ColumnDefinition(insertColumn.getName(),
                            ColumnDefinition.SQLT_CHR,
                            size));
                    break;

                case Types.DATE:
                    break;

                case Types.TIMESTAMP:
                    String oracleFormat;
                    DateFormat javaFormat;
                    if (insertColumn.getSimpleTypeName().equals("DATE")) {
                        oracleFormat = "YYYY-MM-DD HH24:MI:SS";
                        javaFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    } else {
                        oracleFormat = "YYYY-MM-DD HH24:MI:SS.FF9";
                        javaFormat = new TimestampFormat("yyyy-MM-dd HH:mm:ss", 9);
                    }
                    formats[i] = javaFormat;
                    columns.add(new ColumnDefinition(insertColumn.getName(),
                            ColumnDefinition.SQLT_CHR,
                            javaFormat.format(dummy).length(),
                            oracleFormat));
                    break;

                default:
                    throw new SQLException("Unsupported type : " + insertColumn.getSimpleTypeName());
            }

        }

        rowSize = 0;
        for (ColumnDefinition column : columns) {
            rowSize += column.columnSize;
        }

        TableDefinition tableDefinition = new TableDefinition(table, charset.getId(), columns);
        ociManager.open(ociKey, database, user, password, tableDefinition);

        buffer = new RowBuffer(tableDefinition, Math.max(batchSize / rowSize, 8), charset.getJavaCharset());
    }

    @Override
    public int getBatchWeight()
    {
        return batchWeight;
    }

    @Override
    public void add() throws IOException, SQLException
    {
        batchWeight += rowSize;
        if (buffer.isFull()) {
            flush();
        }
    }

    @Override
    public void close() throws IOException, SQLException
    {
        ociManager.close(ociKey);
    }

    @Override
    public void flush() throws IOException, SQLException
    {
        if (buffer.getRowCount() > 0) {
            try {
                logger.info(String.format("Loading %,d rows", buffer.getRowCount()));

                long startTime = System.currentTimeMillis();

                OCIWrapper oci = ociManager.get(ociKey);
                synchronized (oci) {
                    oci.loadBuffer(buffer.getBuffer(), buffer.getRowCount());
                }

                totalRows += buffer.getRowCount();
                double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
                logger.info(String.format("> %.2f seconds (loaded %,d rows in total)", seconds, totalRows));

            } finally {
                buffer.clear();
                batchWeight = 0;
            }
        }
    }

    @Override
    public void finish() throws IOException, SQLException
    {
        flush();
    }

    @Override
    public void setNull(int sqlType) throws IOException, SQLException
    {
        buffer.addValue("");
    }

    @Override
    public void setBoolean(boolean v) throws IOException, SQLException
    {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setByte(byte v) throws IOException, SQLException
    {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setShort(short v) throws IOException, SQLException
    {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setInt(int v) throws IOException, SQLException
    {
        buffer.addValue(v);
    }

    @Override
    public void setLong(long v) throws IOException, SQLException
    {
        buffer.addValue(Long.toString(v));
    }

    @Override
    public void setFloat(float v) throws IOException, SQLException
    {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setDouble(double v) throws IOException, SQLException
    {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setBigDecimal(BigDecimal v) throws IOException, SQLException
    {
        buffer.addValue(v);
    }

    @Override
    public void setString(String v) throws IOException, SQLException
    {
        buffer.addValue(v);
    }

    @Override
    public void setNString(String v) throws IOException, SQLException
    {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setBytes(byte[] v) throws IOException, SQLException
    {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setSqlDate(Date v, int sqlType) throws IOException, SQLException
    {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setSqlTime(Time v, int sqlType) throws IOException, SQLException
    {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setSqlTimestamp(Timestamp v, int sqlType) throws IOException, SQLException
    {
        buffer.addValue(formats[buffer.getCurrentColumn()].format(v));
    }

}
