package org.embulk.output.jdbc.setter;

import java.sql.Types;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.PageReader;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;

public class ColumnSetterFactory
{
    protected final BatchInsert batch;
    protected final PageReader pageReader;
    protected final TimestampFormatter timestampFormatter;

    public ColumnSetterFactory(BatchInsert batch, PageReader pageReader,
            TimestampFormatter timestampFormatter)
    {
        this.batch = batch;
        this.pageReader = pageReader;
        this.timestampFormatter = timestampFormatter;
    }

    public SkipColumnSetter newSkipColumnSetter()
    {
        return new SkipColumnSetter(batch, pageReader);
    }

    public ColumnSetter newStringPassThroughColumnSetter(JdbcColumn column)
    {
        System.out.println("string pass through: "+column);
        switch(column.getSqlType()) {
        // setNString, NClob
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
            return new NStringColumnSetter(batch, pageReader, column, timestampFormatter);
        default:
            return new StringColumnSetter(batch, pageReader, column, timestampFormatter);
        }
    }

    public ColumnSetter newColumnSetter(JdbcColumn column)
    {
        switch(column.getSqlType()) {
        // setByte
        case Types.TINYINT:
            return new ByteColumnSetter(batch, pageReader, column);

        // setShort
        case Types.SMALLINT:
            return new ShortColumnSetter(batch, pageReader, column);

        // setInt
        case Types.INTEGER:
            return new IntColumnSetter(batch, pageReader, column);

        // setLong
        case Types.BIGINT:
            return new LongColumnSetter(batch, pageReader, column);

        // setDouble
        case Types.DOUBLE:
        case Types.FLOAT:
            return new DoubleColumnSetter(batch, pageReader, column);

        // setFloat
        case Types.REAL:
            return new FloatColumnSetter(batch, pageReader, column);

        // setBool
        case Types.BOOLEAN:
        case Types.BIT:  // JDBC BIT is boolean, unlike SQL-92
            return new BooleanColumnSetter(batch, pageReader, column);

        // setString, Clob
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.CLOB:
            return new StringColumnSetter(batch, pageReader, column, timestampFormatter);

        // setNString, NClob
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
            return new NStringColumnSetter(batch, pageReader, column, timestampFormatter);

        // TODO
        //// setBytes Blob
        //case Types.BINARY:
        //case Types.VARBINARY:
        //case Types.LONGVARBINARY:
        //case Types.BLOB:
        //    return new BytesColumnSetter(batch, pageReader, column);

        // Time
        case Types.DATE:
            return new SqlDateColumnSetter(batch, pageReader, column, timestampFormatter.getTimeZone());
        case Types.TIME:
            return new SqlTimeColumnSetter(batch, pageReader, column);
        case Types.TIMESTAMP:
            return new SqlTimestampColumnSetter(batch, pageReader, column);

        // Null
        case Types.NULL:
            return new NullColumnSetter(batch, pageReader, column);

        // BigDecimal
        case Types.NUMERIC:
        case Types.DECIMAL:
            return new BigDecimalColumnSetter(batch, pageReader, column);

        // others
        case Types.ARRAY:  // array
        case Types.STRUCT: // map
        case Types.REF:
        case Types.DATALINK:
        case Types.SQLXML: // XML
        case Types.ROWID:
        case Types.DISTINCT:
        case Types.JAVA_OBJECT:
        case Types.OTHER:
        default:
            throw unsupportedOperationException(column);
        }
    }

    //private static String[] UNSUPPORTED = new String[] {
    //    "ARRAY",
    //    "STRUCT",
    //    "REF",
    //    "DATALINK",
    //    "SQLXML",
    //    "ROWID",
    //    "DISTINCT",
    //    "OTHER",
    //};

    private static UnsupportedOperationException unsupportedOperationException(JdbcColumn column)
    {
        throw new UnsupportedOperationException(
                String.format("Unsupported type %s (sqlType=%d, size=%d, scale=%d)",
                    column.getTypeName(), column.getSqlType(), column.getSizeTypeParameter(), column.getScaleTypeParameter()));
    }
}
