package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public abstract class ColumnSetter
        implements ColumnVisitor
{
    protected final BatchInsert batch;
    protected final PageReader pageReader;
    protected final JdbcColumn column;

    public ColumnSetter(BatchInsert batch, PageReader pageReader,
            JdbcColumn column)
    {
        this.batch = batch;
        this.pageReader = pageReader;
        this.column = column;
    }

    public JdbcColumn getColumn()
    {
        return column;
    }

    public int getSqlType()
    {
        return column.getSqlType();
    }

    public void booleanColumn(Column column)
    {
        try {
            if (pageReader.isNull(column)) {
                nullValue();
            } else {
                booleanValue(pageReader.getBoolean(column));
            }
        } catch (IOException | SQLException ex) {
            // TODO exception class
            throw new RuntimeException(ex);
        }
    }

    public void longColumn(Column column)
    {
        try {
            if (pageReader.isNull(column)) {
                nullValue();
            } else {
                longValue(pageReader.getLong(column));
            }
        } catch (IOException | SQLException ex) {
            // TODO exception class
            throw new RuntimeException(ex);
        }
    }

    public void doubleColumn(Column column)
    {
        try {
            if (pageReader.isNull(column)) {
                nullValue();
            } else {
                doubleValue(pageReader.getDouble(column));
            }
        } catch (IOException | SQLException ex) {
            // TODO exception class
            throw new RuntimeException(ex);
        }
    }

    public void stringColumn(Column column)
    {
        try {
            if (pageReader.isNull(column)) {
                nullValue();
            } else {
                stringValue(pageReader.getString(column));
            }
        } catch (IOException | SQLException ex) {
            // TODO exception class
            throw new RuntimeException(ex);
        }
    }

    public void timestampColumn(Column column)
    {
        try {
            if (pageReader.isNull(column)) {
                nullValue();
            } else {
                timestampValue(pageReader.getTimestamp(column));
            }
        } catch (IOException | SQLException ex) {
            // TODO exception class
            throw new RuntimeException(ex);
        }
    }

    protected void nullValue() throws IOException, SQLException
    {
        try {
            batch.setNull(getSqlType());
        } catch (IOException | SQLException ex) {
            // TODO exception class
            throw new RuntimeException(ex);
        }
    }

    protected abstract void booleanValue(boolean v) throws IOException, SQLException;

    protected abstract void longValue(long v) throws IOException, SQLException;

    protected abstract void doubleValue(double v) throws IOException, SQLException;

    protected abstract void stringValue(String v) throws IOException, SQLException;

    protected abstract void timestampValue(Timestamp v) throws IOException, SQLException;
}
