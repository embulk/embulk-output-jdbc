package org.embulk.output.oracle.oci;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.SQLException;

import org.embulk.output.oracle.oci.ColumnDefinition;
import org.embulk.output.oracle.oci.TableDefinition;

public class RowBuffer
{
    private final TableDefinition table;
    private final int rowCount;
    private final Charset charset;

    private int currentRow = 0;
    private int currentColumn = 0;

    private final short[] sizes;
    private final ByteBuffer buffer;
    private final ByteBuffer defaultBuffer;

    public RowBuffer(TableDefinition table, int rowCount, Charset charset)
    {
        this.table = table;
        this.rowCount = rowCount;
        this.charset = charset;

        int rowSize = 0;
        for (ColumnDefinition column : table.columns) {
            rowSize += column.columnSize;
        }

        // should be direct because used by native library
        buffer = ByteBuffer.allocateDirect(rowSize * rowCount);
        // position is not updated
        defaultBuffer = buffer.duplicate();

        sizes = new short[table.columns.length * rowCount];
    }

    public ByteBuffer getBuffer() {
        return defaultBuffer;
    }

    public short[] getSizes() {
        return sizes;
    }

    public void addValue(int value)
    {
        if (isFull()) {
            throw new IllegalStateException();
        }

        buffer.putInt(value);

        next((short)4);
    }

    public void addValue(String value) throws SQLException
    {
        addValue(value, charset);
    }

    public void addValue(String value, Charset charset) throws SQLException
    {
        if (isFull()) {
            throw new IllegalStateException();
        }

        ByteBuffer bytes = charset.encode(value);
        int length = bytes.remaining();
        if (length > 65535) {
            throw new SQLException(String.format("byte count of string is too large (max : 65535, actual : %d).", length));
        }
        if (length > table.columns[currentColumn].columnSize) {
            throw new SQLException(String.format("byte count of string is too large for column \"%s\" (max : %d, actual : %d).",
                    table.columns[currentColumn].columnName, table.columns[currentColumn].columnSize, length));
        }

        buffer.put(bytes);

        next((short)length);
    }

    public void addValue(BigDecimal value) throws SQLException
    {
        addValue(value.toPlainString());
    }

    private void next(short size)
    {
        sizes[currentRow * table.columns.length + currentColumn] = size;

        currentColumn++;
        if (currentColumn == table.columns.length) {
            currentColumn = 0;
            currentRow++;
        }
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
        currentRow = 0;
        currentColumn = 0;
        buffer.clear();
    }

}
