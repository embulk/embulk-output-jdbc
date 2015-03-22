package org.embulk.output.oracle.oci;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class RowBuffer
{
    private final TableDefinition table;
    private final int rowCount;
    private final byte[] buffer;
    private int currentRow = 0;
    private int currentColumn = 0;
    private int currentPosition = 0;
    private final Charset charset;


    public RowBuffer(TableDefinition table, int rowCount, Charset charset)
    {
        this.table = table;
        this.rowCount = rowCount;
        this.charset = charset;

        int rowSize = 0;
        for (ColumnDefinition column : table.columns) {
            rowSize += column.columnSize;
        }

        buffer = new byte[rowSize * rowCount];
    }

    public void addValue(int value)
    {
        if (isFull()) {
            throw new IllegalStateException();
        }

        buffer[currentPosition] = (byte)value;
        buffer[currentPosition + 1] = (byte)(value >> 8);
        buffer[currentPosition + 2] = (byte)(value >> 16);
        buffer[currentPosition + 3] = (byte)(value >> 24);

        next();
    }

    public void addValue(String value)
    {
        if (isFull()) {
            throw new IllegalStateException();
        }

        ByteBuffer bytes = charset.encode(value);
        int length = bytes.remaining();
        // TODO:オーバーフロー
        bytes.get(buffer, currentPosition, length);
        if (length < table.columns[currentColumn].columnSize) {
            buffer[currentPosition + length] = 0;
        }

        next();
    }

    private void next()
    {
        currentPosition += table.columns[currentColumn].columnSize;
        currentColumn++;
        if (currentColumn == table.columns.length) {
            currentColumn = 0;
            currentRow++;
        }
    }

    public byte[] getBuffer()
    {
        return buffer;
    }

    public int getCurrentColumn()
    {
        return currentColumn;
    }

    public int getRowCount()
    {
        return currentRow;
    }

    public boolean isFull()
    {
        return currentRow >= rowCount;
    }

    public void clear()
    {
        currentPosition = 0;
        currentRow = 0;
        currentColumn = 0;
    }

}
