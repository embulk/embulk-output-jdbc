package org.embulk.output.jdbc;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.embulk.spi.Column;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.msgpack.value.Value;

/**
 * Record read by PageReader.
 * The class will save read records for retry.
 */
public class PageReaderRecord implements Record
{
    private final PageReader pageReader;
    private final List<MemoryRecord> readRecords;
    private MemoryRecord lastRecord;

    public PageReaderRecord(PageReader pageReader)
    {
        this.pageReader = pageReader;
        readRecords = new ArrayList<MemoryRecord>();
    }

    public void setPage(Page page)
    {
        pageReader.setPage(page);
    }

    public boolean nextRecord()
    {
        lastRecord = null; // lastRecord will be created in next `save` method execution.
        return pageReader.nextRecord();
    }

    public boolean isNull(Column column)
    {
        return pageReader.isNull(column);
    }

    public boolean getBoolean(Column column)
    {
        return save(column, pageReader.getBoolean(column));
    }

    public long getLong(Column column)
    {
        return save(column, pageReader.getLong(column));
    }

    public double getDouble(Column column)
    {
        return save(column, pageReader.getDouble(column));
    }

    public String getString(Column column)
    {
        return save(column, pageReader.getString(column));
    }

    public Instant getTimestamp(Column column)
    {
        return save(column, pageReader.getTimestamp(column).getInstant());
    }

    public Value getJson(Column column)
    {
        return save(column, pageReader.getJson(column));
    }

    public List<? extends Record> getReadRecords()
    {
        return readRecords;
    }

    public void clearReadRecords()
    {
        readRecords.clear();
        lastRecord = null;
    }

    private <T> T save(Column column, T value)
    {
        if (lastRecord == null) {
            lastRecord = new MemoryRecord(pageReader.getSchema().getColumnCount());
            readRecords.add(lastRecord);
        }
        lastRecord.setValue(column, value);
        return value;
    }
}
