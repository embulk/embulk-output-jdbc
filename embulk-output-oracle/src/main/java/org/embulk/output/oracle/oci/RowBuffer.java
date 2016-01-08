package org.embulk.output.oracle.oci;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.SQLException;

import jnr.ffi.Runtime;

import org.embulk.output.oracle.oci.ColumnDefinition;
import org.embulk.output.oracle.oci.TableDefinition;

public class RowBuffer
{
    private final TableDefinition table;
    private final int rowCount;

    private int currentRow = 0;
    private int currentColumn = 0;

    private final short[] sizes;
    private final ByteBuffer buffer;
    private final ByteBuffer defaultBuffer;

    public RowBuffer(TableDefinition table, int rowCount)
    {
        this.table = table;
        this.rowCount = rowCount;

        int rowSize = 0;
        for (int i = 0; i < table.getColumnCount(); i++) {
            rowSize += table.getColumn(i).getDataSize();
        }

        // should be direct because used by native library
        buffer = ByteBuffer.allocateDirect(rowSize * rowCount).order(Runtime.getSystemRuntime().byteOrder());
        // position is not updated
        defaultBuffer = buffer.duplicate();

        sizes = new short[table.getColumnCount() * rowCount];
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
        if (isFull()) {
            throw new IllegalStateException();
        }

        ColumnDefinition column = table.getColumn(currentColumn);
        Charset charset = column.getCharset().getJavaCharset();

        ByteBuffer bytes = charset.encode(value);
        int length = bytes.remaining();
        if (length > 65535) {
            throw new SQLException(String.format("byte count of string is too large (max : 65535, actual : %d).", length));
        }
        if (length > column.getDataSize()) {
            throw new SQLException(String.format("byte count of string is too large for column \"%s\" (max : %d, actual : %d).",
                    column.getColumnName(), column.getDataSize(), length));
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
        sizes[currentRow * table.getColumnCount() + currentColumn] = size;

        currentColumn++;
        if (currentColumn == table.getColumnCount()) {
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
