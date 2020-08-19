package org.embulk.output.oracle;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.TableIdentifier;
import org.embulk.output.jdbc.TimestampFormat;
import org.embulk.output.oracle.oci.ColumnDefinition;
import org.embulk.output.oracle.oci.OCI;
import org.embulk.output.oracle.oci.OCIManager;
import org.embulk.output.oracle.oci.OCIWrapper;
import org.embulk.output.oracle.oci.RowBuffer;
import org.embulk.output.oracle.oci.TableDefinition;

public class DirectBatchInsert implements BatchInsert
{
    private static OCIManager ociManager = new OCIManager();

    private List<String> ociKey;
    private final String database;
    private final String user;
    private final String password;
    private final OracleCharset charset;
    private final OracleCharset nationalCharset;
    private final int batchSize;
    private RowBuffer buffer;
    private boolean closed;

    private DateFormat[] formats;


    public DirectBatchInsert(String database, String user, String password,
            OracleCharset charset, OracleCharset nationalCharset, int batchSize)
    {
        this.database = database;
        this.user = user;
        this.password = password;
        this.charset = charset;
        this.nationalCharset = nationalCharset;
        this.batchSize = batchSize;
    }

    @Override
    public void prepare(TableIdentifier loadTable, JdbcSchema insertSchema) throws SQLException
    {

        /*
         * available mappings
         *
         * boolean      -> unused
         * byte         -> unused
         * short        -> unused
         * int          -> NUMBER
         * long         -> NUMBER
         * BigDecimal   -> NUMBER
         * String       -> CHAR,VARCHAR,LONGVARCHAR,CLOB,NUMBER
         * NString      -> NCHAR,NVARCHAR,LONGNVARCHAR,NCLOB
         * bytes        -> unused
         * SqlDate      -> unused
         * SqlTime      -> unused
         * SqlTimeStamp -> TIMESTAMP
         *
         */

        formats = new DateFormat[insertSchema.getCount()];
        List<ColumnDefinition> columns = new ArrayList<ColumnDefinition>();
        java.sql.Timestamp dummy = new java.sql.Timestamp(System.currentTimeMillis());
        for (int i = 0; i < insertSchema.getCount(); i++) {
            JdbcColumn insertColumn = insertSchema.getColumn(i);
            switch (insertColumn.getSqlType()) {
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CLOB:
                    columns.add(new ColumnDefinition(insertColumn.getName(),
                            OCI.SQLT_CHR,
                            insertColumn.getDataLength(),
                            charset));
                    break;

                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                case Types.NCLOB:
                    columns.add(new ColumnDefinition(insertColumn.getName(),
                            OCI.SQLT_CHR,
                            insertColumn.getDataLength(),
                            nationalCharset));
                    break;

                case Types.DECIMAL:
                case Types.NUMERIC:
                    // sign + size
                    int size = 1 + insertColumn.getSizeTypeParameter();
                    if (insertColumn.getSizeTypeParameter() > 0) {
                        // decimal point
                        size += 1;
                    }
                    columns.add(new ColumnDefinition(insertColumn.getName(),
                            OCI.SQLT_CHR,
                            size,
                            charset));
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
                            OCI.SQLT_CHR,
                            javaFormat.format(dummy).length(),
                            charset,
                            oracleFormat));
                    break;

                default:
                    throw new SQLException("Unsupported type : " + insertColumn.getSimpleTypeName());
            }

        }

        TableDefinition tableDefinition = new TableDefinition(loadTable.getSchemaName(), loadTable.getTableName(), columns);
        ociKey = Arrays.asList(database, user, loadTable.getTableName());
        OCIWrapper oci = ociManager.open(ociKey, database, user, password, tableDefinition, batchSize);

        buffer = new RowBuffer(oci, tableDefinition);
    }

    @Override
    public int getBatchWeight()
    {
        // Automatically flushed in RowBuffer
        return 0;
    }

    @Override
    public void add() throws IOException, SQLException
    {
    }

    @Override
    public void close() throws IOException, SQLException
    {
        if (!closed) {
            ociManager.close(ociKey);
            closed = true;
        }
    }

    @Override
    public void flush() throws IOException, SQLException
    {
        buffer.flush();
    }

    @Override
    public int[] getLastUpdateCounts()
    {
        // need not be implemented because DirectBatchInsert won't retry.
        return new int[]{};
    }

    @Override
    public void finish() throws IOException, SQLException
    {
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
        buffer.addValue(v);
    }

    @Override
    public void setBytes(byte[] v) throws IOException, SQLException
    {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setSqlDate(final Instant v, final Calendar calendar) throws IOException, SQLException
    {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setSqlTime(final Instant v, final Calendar calendar) throws IOException, SQLException
    {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setSqlTimestamp(final Instant v, final Calendar calendar) throws IOException, SQLException
    {
        java.sql.Timestamp t = new java.sql.Timestamp(v.toEpochMilli());
        t.setNanos(v.getNano());
        DateFormat format = formats[buffer.getCurrentColumn()];
        format.setTimeZone(calendar.getTimeZone());
        buffer.addValue(format.format(t));
    }
}
