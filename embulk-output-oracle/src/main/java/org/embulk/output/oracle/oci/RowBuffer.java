package org.embulk.output.oracle.oci;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.sql.SQLException;

import jnr.ffi.Runtime;

import org.embulk.output.oracle.oci.ColumnDefinition;
import org.embulk.output.oracle.oci.TableDefinition;

public class RowBuffer
{
    // this value was calculated by tests
    private static final double OPTIMAL_LOAD_TIME = 10;
    private static final int MIN_ROW_COUNT_TO_LOAD = 100;

    private final OCIWrapper oci;
    private final TableDefinition table;
    private final int maxRowCount;

    private int rowCount = 0;
    private int currentColumn = 0;

    private int rowCountToLoad;
    private int totalLoadedRowCount = 0;
    private int loadCount = 0;
    private double totalLoadTime = 0;

    private final ByteBuffer sizes;
    private final ByteBuffer defaultSizes;
    private final ByteBuffer buffer;
    private final ByteBuffer defaultBuffer;

    public RowBuffer(OCIWrapper oci, TableDefinition table)
    {
        this.oci = oci;
        this.table = table;
        maxRowCount = oci.getMaxRowCount();
        rowCountToLoad = maxRowCount;

        ByteOrder byteOrder = Runtime.getSystemRuntime().byteOrder();
        // should be direct because used by native library
        buffer = ByteBuffer.allocateDirect(table.getRowSize() * maxRowCount).order(byteOrder);
        // position is not updated
        defaultBuffer = buffer.duplicate().order(byteOrder);

        sizes = ByteBuffer.allocateDirect(table.getColumnCount() * maxRowCount * 2).order(byteOrder);
        defaultSizes = sizes.duplicate().order(byteOrder);
    }

    public void addValue(int value) throws SQLException
    {
        buffer.putInt(value);

        next((short)4);
    }

    public void addValue(String value) throws SQLException
    {
        ColumnDefinition column = table.getColumn(currentColumn);
        Charset charset = column.getCharset().getJavaCharset();

        ByteBuffer bytes = charset.encode(value);
        int length = bytes.remaining();
        if (length > Short.MAX_VALUE) {
            throw new SQLException(String.format("byte count of string is too large (max : %d, actual : %d).", Short.MAX_VALUE, length));
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

    private void next(short size) throws SQLException
    {
        sizes.putShort(size);

        currentColumn++;
        if (currentColumn == table.getColumnCount()) {
            currentColumn = 0;
            rowCount++;

            if (rowCount >= rowCountToLoad) {
                flush();
            }
        }
    }

    public int getCurrentColumn()
    {
        return currentColumn;
    }

    public void flush() throws SQLException
    {
        if (rowCount > 0) {
            synchronized (oci) {
                long time = System.currentTimeMillis();
                oci.loadBuffer(defaultBuffer, defaultSizes, rowCount);
                totalLoadTime += System.currentTimeMillis() - time;
                totalLoadedRowCount += rowCount;
                loadCount += 1;
            }

            rowCount = 0;
            currentColumn = 0;
            buffer.clear();
            sizes.clear();

            if (loadCount >= 4) {
                rowCountToLoad = Math.min(Math.max((int)(totalLoadedRowCount / totalLoadTime * OPTIMAL_LOAD_TIME), MIN_ROW_COUNT_TO_LOAD), maxRowCount);
            }
        }
    }

}
