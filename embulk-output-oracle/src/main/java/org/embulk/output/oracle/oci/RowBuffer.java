package org.embulk.output.oracle.oci;

import java.math.BigDecimal;
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
            if (column.columnType == ColumnDefinition.SQLT_CHR) {
                // for length of string
                rowSize += 2;
            }
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
        addValue(value, charset);
    }

    public void addValue(String value, Charset charset)
    {
        if (isFull()) {
            throw new IllegalStateException();
        }

        ByteBuffer bytes = charset.encode(value);
        int length = bytes.remaining();
        // TODO:warning or error if truncated

        buffer[currentPosition] = (byte)length;
        buffer[currentPosition + 1] = (byte)(length >> 8);
        bytes.get(buffer, currentPosition + 2, length);

        next();
    }

    public void addValue(BigDecimal value)
    {
        addValue(value.toPlainString());
    }

    private void next()
    {
        if (table.columns[currentColumn].columnType == ColumnDefinition.SQLT_CHR) {
            currentPosition += 2;
        }
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
