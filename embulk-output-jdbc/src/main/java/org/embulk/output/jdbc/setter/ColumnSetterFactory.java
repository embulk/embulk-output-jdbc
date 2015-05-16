package org.embulk.output.jdbc.setter;

import java.sql.Types;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.config.ConfigException;

public class ColumnSetterFactory
{
    protected final BatchInsert batch;
    protected final TimestampFormatter timestampFormatter;

    public ColumnSetterFactory(BatchInsert batch,
            TimestampFormatter timestampFormatter)
    {
        this.batch = batch;
        this.timestampFormatter = timestampFormatter;
    }

    public SkipColumnSetter newSkipColumnSetter()
    {
        return new SkipColumnSetter(batch);
    }

    public ColumnSetter newColumnSetter(JdbcColumn column, String valueType)
    {
        switch (valueType) {
        case "coalesce":
            return newColumnSetter(column);
        case "byte":
            return new ByteColumnSetter(batch, column);
        case "short":
            return new ShortColumnSetter(batch, column);
        case "int":
            return new IntColumnSetter(batch, column);
        case "long":
            return new LongColumnSetter(batch, column);
        case "double":
            return new DoubleColumnSetter(batch, column);
        case "float":
            return new FloatColumnSetter(batch, column);
        case "boolean":
            return new BooleanColumnSetter(batch, column);
        case "string":
            return new StringColumnSetter(batch, column, timestampFormatter);
        case "nstring":
            return new NStringColumnSetter(batch, column, timestampFormatter);
        case "data":
            return new SqlDateColumnSetter(batch, column, timestampFormatter.getTimeZone());
        case "time":
            return new SqlTimeColumnSetter(batch, column);
        case "timestamp":
            return new SqlTimestampColumnSetter(batch, column);
        case "decimal":
            return new BigDecimalColumnSetter(batch, column);
        case "null":
            return new NullColumnSetter(batch, column);
        case "pass":
            return new PassThroughColumnSetter(batch, column, timestampFormatter);
        default:
            // TODO validate valueType at AbstractJdbcOutputPlugin#transaction so that here never throws exception
            throw new ConfigException(String.format("Unknown value_type '%s' for column '%s'", valueType, column.getName()));
        }
    }

    public ColumnSetter newStringPassThroughColumnSetter(JdbcColumn column)
    {
        System.out.println("string pass through: "+column);
        switch(column.getSqlType()) {
        // setNString, NClob
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
            return new NStringColumnSetter(batch, column, timestampFormatter);
        default:
            return new StringColumnSetter(batch, column, timestampFormatter);
        }
    }

    public ColumnSetter newColumnSetter(JdbcColumn column)
    {
        switch(column.getSqlType()) {
        // setByte
        case Types.TINYINT:
            return new ByteColumnSetter(batch, column);

        // setShort
        case Types.SMALLINT:
            return new ShortColumnSetter(batch, column);

        // setInt
        case Types.INTEGER:
            return new IntColumnSetter(batch, column);

        // setLong
        case Types.BIGINT:
            return new LongColumnSetter(batch, column);

        // setDouble
        case Types.DOUBLE:
        case Types.FLOAT:
            return new DoubleColumnSetter(batch, column);

        // setFloat
        case Types.REAL:
            return new FloatColumnSetter(batch, column);

        // setBool
        case Types.BOOLEAN:
        case Types.BIT:  // JDBC BIT is boolean, unlike SQL-92
            return new BooleanColumnSetter(batch, column);

        // setString, Clob
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.CLOB:
            return new StringColumnSetter(batch, column, timestampFormatter);

        // setNString, NClob
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
            return new NStringColumnSetter(batch, column, timestampFormatter);

        // TODO
        //// setBytes Blob
        //case Types.BINARY:
        //case Types.VARBINARY:
        //case Types.LONGVARBINARY:
        //case Types.BLOB:
        //    return new BytesColumnSetter(batch, column);

        // Time
        case Types.DATE:
            return new SqlDateColumnSetter(batch, column, timestampFormatter.getTimeZone());
        case Types.TIME:
            return new SqlTimeColumnSetter(batch, column);
        case Types.TIMESTAMP:
            return new SqlTimestampColumnSetter(batch, column);

        // Null
        case Types.NULL:
            return new NullColumnSetter(batch, column);

        // BigDecimal
        case Types.NUMERIC:
        case Types.DECIMAL:
            return new BigDecimalColumnSetter(batch, column);

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
                    column.getDeclaredType().or(column.getSimpleTypeName()),
                    column.getSqlType(), column.getSizeTypeParameter(), column.getScaleTypeParameter()));
    }
}
