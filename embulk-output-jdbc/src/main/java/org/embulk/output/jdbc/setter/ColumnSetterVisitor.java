package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;

import org.embulk.output.jdbc.Record;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;

public class ColumnSetterVisitor
        implements ColumnVisitor
{
    private final Record record;
    private final ColumnSetter setter;

    public ColumnSetterVisitor(Record record, ColumnSetter setter)
    {
        this.record = record;
        this.setter = setter;
    }

    @Override
    public void booleanColumn(Column column)
    {
        try {
            if (record.isNull(column)) {
                setter.nullValue();
            } else {
                setter.booleanValue(record.getBoolean(column));
            }
        } catch (IOException | SQLException ex) {
            // TODO exception class
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void longColumn(Column column)
    {
        try {
            if (record.isNull(column)) {
                setter.nullValue();
            } else {
                setter.longValue(record.getLong(column));
            }
        } catch (IOException | SQLException ex) {
            // TODO exception class
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void doubleColumn(Column column)
    {
        try {
            if (record.isNull(column)) {
                setter.nullValue();
            } else {
                setter.doubleValue(record.getDouble(column));
            }
        } catch (IOException | SQLException ex) {
            // TODO exception class
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void stringColumn(Column column)
    {
        try {
            if (record.isNull(column)) {
                setter.nullValue();
            } else {
                setter.stringValue(record.getString(column));
            }
        } catch (IOException | SQLException ex) {
            // TODO exception class
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void jsonColumn(Column column)
    {
        try {
            if (record.isNull(column)) {
                setter.nullValue();
            } else {
                setter.jsonValue(record.getJson(column));
            }
        } catch (IOException | SQLException ex) {
            // TODO exception class
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void timestampColumn(Column column)
    {
        try {
            if (record.isNull(column)) {
                setter.nullValue();
            } else {
                setter.timestampValue(record.getTimestamp(column));
            }
        } catch (IOException | SQLException ex) {
            // TODO exception class
            throw new RuntimeException(ex);
        }
    }
}
