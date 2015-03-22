package org.embulk.output.oracle;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.oracle.oci.ColumnDefinition;
import org.embulk.output.oracle.oci.OCIWrapper;
import org.embulk.output.oracle.oci.RowBuffer;
import org.embulk.output.oracle.oci.TableDefinition;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

public class DirectBatchInsert implements BatchInsert
{

    private final Logger logger = Exec.getLogger(DirectBatchInsert.class);

    private static OCIWrapper oci = new OCIWrapper();
    private static int open;

    private final String database;
    private final String user;
    private final String password;
    private final String table;
    private RowBuffer buffer;
    private long totalRows;

    private DateFormat[] formats;


    public DirectBatchInsert(String database, String user, String password, String table)
    {
        this.database = database;
        this.user = user;
        this.password = password;
        this.table = table;
    }

    @Override
    public void prepare(String loadTable, JdbcSchema insertSchema) throws SQLException {

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
        for (int i = 0; i < insertSchema.getCount(); i++) {
            JdbcColumn insertColumn = insertSchema.getColumn(i);
            switch (insertColumn.getSqlType()) {
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CLOB:
                    // TODO: CHAR(n CHAR)
                    columns.add(new ColumnDefinition(insertColumn.getName(), ColumnDefinition.SQLT_CHR, insertColumn.getSizeTypeParameter()));
                    break;

                case Types.DECIMAL:
                    // sign + size
                    int size = 1 + insertColumn.getSizeTypeParameter();
                    if (insertColumn.getSizeTypeParameter() > 0) {
                        // decimal point
                        size += 1;
                    }
                    columns.add(new ColumnDefinition(insertColumn.getName(), ColumnDefinition.SQLT_CHR, size));
                    break;

                case Types.DATE:
                    break;

                case Types.TIMESTAMP:
                    if (insertColumn.getTypeName().equals("DATE")) {
                        String datePattern = "yy-MM-dd";
                        formats[i] = new SimpleDateFormat(datePattern);
                        columns.add(new ColumnDefinition(insertColumn.getName(), ColumnDefinition.SQLT_CHR, datePattern.length()));
                    } else {
                        String timestampPattern = "yy-MM-dd HH:mm:ss";
                        formats[i] = new SimpleDateFormat(timestampPattern);
                        columns.add(new ColumnDefinition(insertColumn.getName(), ColumnDefinition.SQLT_CHR, timestampPattern.length() + 10));
                    }
                    break;

                default:
                    throw new SQLException("Unsupported type : " + insertColumn.getTypeName());
            }

        }

        int rowSize = 0;
        for (ColumnDefinition column : columns) {
            rowSize += column.columnSize;
        }



        /*
        JdbcOutputConnection connection = connector.connect(true);
        try {
            connection.

        } finally {
            connection.close();
        }
        */


        TableDefinition tableDefinition = new TableDefinition(table, columns);
        /*
                "EXAMPLE",
                //new ColumnDefinition("ID", ColumnDefinition.SQLT_INT, 4),
                //new ColumnDefinition("NUM", ColumnDefinition.SQLT_INT, 4),
                new ColumnDefinition("ID", ColumnDefinition.SQLT_CHR, 8),
                new ColumnDefinition("NUM", ColumnDefinition.SQLT_CHR, 12),
                new ColumnDefinition("VALUE1", ColumnDefinition.SQLT_CHR, 60),
                new ColumnDefinition("VALUE2", ColumnDefinition.SQLT_CHR, 60),
                new ColumnDefinition("VALUE3", ColumnDefinition.SQLT_CHR, 60),
                new ColumnDefinition("VALUE4", ColumnDefinition.SQLT_CHR, 60),
                new ColumnDefinition("VALUE5", ColumnDefinition.SQLT_CHR, 60),
                new ColumnDefinition("VALUE6", ColumnDefinition.SQLT_CHR, 60),
                new ColumnDefinition("VALUE7", ColumnDefinition.SQLT_CHR, 60),
                new ColumnDefinition("VALUE8", ColumnDefinition.SQLT_CHR, 60),
                new ColumnDefinition("VALUE9", ColumnDefinition.SQLT_CHR, 60),
                new ColumnDefinition("VALUE10", ColumnDefinition.SQLT_CHR, 60)
                );
*/
        synchronized (oci) {
            if (open == 0) {
                oci.open(database, user, password);
                oci.prepareLoad(tableDefinition);
            }
            open++;
        }

        buffer = new RowBuffer(tableDefinition, 10000, Charset.forName("MS932"));
    }

    @Override
    public int getBatchWeight() {
        // TODO 自動生成されたメソッド・スタブ
        return 1000000;
    }

    @Override
    public void add() throws IOException, SQLException {
        if (buffer.isFull()) {
            flush();
        }
    }

    @Override
    public void close() throws IOException, SQLException {
        synchronized (oci) {
            open--;
            if (open == 0) {
                oci.commit();
                oci.close();
            }
        }
    }

    @Override
    public void flush() throws IOException, SQLException {
        if (buffer.getRowCount() > 0) {
            try {
                logger.info(String.format("Loading %,d rows", buffer.getRowCount()));

                long startTime = System.currentTimeMillis();

                synchronized (oci) {
                    oci.loadBuffer(buffer.getBuffer(), buffer.getRowCount());
                }

                totalRows += buffer.getRowCount();
                double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
                logger.info(String.format("> %.2f seconds (loaded %,d rows in total)", seconds, totalRows));

            } finally {
                buffer.clear();
            }
        }
    }

    @Override
    public void finish() throws IOException, SQLException {
        flush();
        /*
        synchronized (oci) {
            oci.commit();
        }
        */
    }

    @Override
    public void setNull(int sqlType) throws IOException, SQLException {
        buffer.addValue("");
    }

    @Override
    public void setBoolean(boolean v) throws IOException, SQLException {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setByte(byte v) throws IOException, SQLException {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setShort(short v) throws IOException, SQLException {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setInt(int v) throws IOException, SQLException {
        buffer.addValue(v);
    }

    @Override
    public void setLong(long v) throws IOException, SQLException {
        buffer.addValue(Long.toString(v));
    }

    @Override
    public void setFloat(float v) throws IOException, SQLException {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setDouble(double v) throws IOException, SQLException {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setBigDecimal(BigDecimal v) throws IOException, SQLException {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setString(String v) throws IOException, SQLException {
        buffer.addValue(v);
    }

    @Override
    public void setNString(String v) throws IOException, SQLException {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setBytes(byte[] v) throws IOException, SQLException {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setSqlDate(Date v, int sqlType) throws IOException, SQLException {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setSqlTime(Time v, int sqlType) throws IOException, SQLException {
        throw new SQLException("Unsupported");
    }

    @Override
    public void setSqlTimestamp(Timestamp v, int sqlType) throws IOException, SQLException {
        buffer.addValue(formats[buffer.getCurrentColumn()].format(v));
        /*
        if (sqlType == Types.DATE) {
            buffer.addValue(formats[buffer.getCurrentColumn()].format(v));
        } else if (sqlType == Types.TIMESTAMP) {
            buffer.addValue(formats[buffer.getCurrentColumn()].format(v) + ".000000000");
        }
        */

        /*
        if (v.getHours() == 0) {
            DateFormat df = new SimpleDateFormat("yy-MM-dd");
            buffer.addValue(df.format(v));
        } else {
            DateFormat df = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
            buffer.addValue(df.format(v));
        }
        */
        //DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        //buffer.addValue(df.format(v) + "000000");
    }

}
