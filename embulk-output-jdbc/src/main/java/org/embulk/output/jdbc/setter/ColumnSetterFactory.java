package org.embulk.output.jdbc.setter;

import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.Optional;
import java.sql.Types;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.spi.Exec;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcColumnOption;
import org.embulk.config.ConfigException;
import org.embulk.util.timestamp.TimestampFormatter;

public class ColumnSetterFactory
{
    protected final BatchInsert batch;
    protected final String defaultTimeZone;

    public ColumnSetterFactory(final BatchInsert batch, final String defaultTimeZone)
    {
        this.batch = batch;
        this.defaultTimeZone = defaultTimeZone;
    }

    public SkipColumnSetter newSkipColumnSetter()
    {
        return new SkipColumnSetter(batch);
    }

    public DefaultValueSetter newDefaultValueSetter(JdbcColumn column, JdbcColumnOption option)
    {
        return new NullDefaultValueSetter(batch, column);
    }

    public ColumnSetter newColumnSetter(JdbcColumn column, JdbcColumnOption option)
    {
        switch (option.getValueType()) {
        case "coerce":
            return newCoalesceColumnSetter(column, option);
        case "byte":
            return new ByteColumnSetter(batch, column, newDefaultValueSetter(column, option));
        case "short":
            return new ShortColumnSetter(batch, column, newDefaultValueSetter(column, option));
        case "int":
            return new IntColumnSetter(batch, column, newDefaultValueSetter(column, option));
        case "long":
            return new LongColumnSetter(batch, column, newDefaultValueSetter(column, option));
        case "double":
            return new DoubleColumnSetter(batch, column, newDefaultValueSetter(column, option));
        case "float":
            return new FloatColumnSetter(batch, column, newDefaultValueSetter(column, option));
        case "boolean":
            return new BooleanColumnSetter(batch, column, newDefaultValueSetter(column, option));
        case "string":
            return new StringColumnSetter(batch, column, newDefaultValueSetter(column, option), newTimestampFormatter(option));
        case "nstring":
            return new NStringColumnSetter(batch, column, newDefaultValueSetter(column, option), newTimestampFormatter(option));
        case "date":
            return new SqlDateColumnSetter(batch, column, newDefaultValueSetter(column, option), newCalendar(option));
        case "time":
            return new SqlTimeColumnSetter(batch, column, newDefaultValueSetter(column, option), newCalendar(option));
        case "timestamp":
            return new SqlTimestampColumnSetter(batch, column, newDefaultValueSetter(column, option), newCalendar(option));
        case "decimal":
            return new BigDecimalColumnSetter(batch, column, newDefaultValueSetter(column, option));
        case "json":
            return new JsonColumnSetter(batch, column, newDefaultValueSetter(column, option));
        case "null":
            return new NullColumnSetter(batch, column, newDefaultValueSetter(column, option));
        case "pass":
            return new PassThroughColumnSetter(batch, column, newDefaultValueSetter(column, option), newCalendar(option));
        default:
            throw new ConfigException(String.format("Unknown value_type '%s' for column '%s'", option.getValueType(), column.getName()));
        }
    }

    protected TimestampFormatter newTimestampFormatter(JdbcColumnOption option)
    {
        final String format = option.getTimestampFormat();
        final String timezone = option.getTimeZone().orElse(this.defaultTimeZone);
        return TimestampFormatter.builder(format, true).setDefaultZoneFromString(timezone).build();
    }

    protected Calendar newCalendar(JdbcColumnOption option)
    {
        return Calendar.getInstance(TimeZone.getTimeZone(getTimeZone(option)), Locale.ENGLISH);
    }

    protected String getTimeZone(JdbcColumnOption option)
    {
        return option.getTimeZone().orElse(defaultTimeZone);
    }

    public ColumnSetter newCoalesceColumnSetter(JdbcColumn column, JdbcColumnOption option)
    {
        switch(column.getSqlType()) {
        // setByte
        case Types.TINYINT:
            return new ByteColumnSetter(batch, column, newDefaultValueSetter(column, option));

        // setShort
        case Types.SMALLINT:
            return new ShortColumnSetter(batch, column, newDefaultValueSetter(column, option));

        // setInt
        case Types.INTEGER:
            return new IntColumnSetter(batch, column, newDefaultValueSetter(column, option));

        // setLong
        case Types.BIGINT:
            return new LongColumnSetter(batch, column, newDefaultValueSetter(column, option));

        // setDouble
        case Types.DOUBLE:
        case Types.FLOAT:
            return new DoubleColumnSetter(batch, column, newDefaultValueSetter(column, option));

        // setFloat
        case Types.REAL:
            return new FloatColumnSetter(batch, column, newDefaultValueSetter(column, option));

        // setBool
        case Types.BOOLEAN:
        case Types.BIT:  // JDBC BIT is boolean, unlike SQL-92
            return new BooleanColumnSetter(batch, column, newDefaultValueSetter(column, option));

        // setString, Clob
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.CLOB:
            return new StringColumnSetter(batch, column, newDefaultValueSetter(column, option), newTimestampFormatter(option));

        // setNString, NClob
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
        case Types.NCLOB:
            return new NStringColumnSetter(batch, column, newDefaultValueSetter(column, option), newTimestampFormatter(option));

        // TODO
        //// setBytes Blob
        //case Types.BINARY:
        //case Types.VARBINARY:
        //case Types.LONGVARBINARY:
        //case Types.BLOB:
        //    return new BytesColumnSetter(batch, column, newDefaultValueSetter(column, option));

        // Time
        case Types.DATE:
            return new SqlDateColumnSetter(batch, column, newDefaultValueSetter(column, option), newCalendar(option));
        case Types.TIME:
            return new SqlTimeColumnSetter(batch, column, newDefaultValueSetter(column, option), newCalendar(option));
        case Types.TIMESTAMP:
            return new SqlTimestampColumnSetter(batch, column, newDefaultValueSetter(column, option), newCalendar(option));

        // Null
        case Types.NULL:
            return new NullColumnSetter(batch, column, newDefaultValueSetter(column, option));

        // BigDecimal
        case Types.NUMERIC:
        case Types.DECIMAL:
            return new BigDecimalColumnSetter(batch, column, newDefaultValueSetter(column, option));

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
                    column.getDeclaredType().orElse(column.getSimpleTypeName()),
                    column.getSqlType(), column.getSizeTypeParameter(), column.getScaleTypeParameter()));
    }
}
