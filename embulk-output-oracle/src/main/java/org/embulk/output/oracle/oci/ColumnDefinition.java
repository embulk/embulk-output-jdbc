package org.embulk.output.oracle.oci;

public class ColumnDefinition
{
    private final String columnName;
    private final int dataType;
    private final int dataSize;
    private final short charsetId;
    private final String dateFormat;


    public ColumnDefinition(String columnName, int dataType, int dataSize, short charsetId, String dateFormat)
    {
        this.columnName = columnName;
        this.dataType = dataType;
        this.dataSize = dataSize;
        this.charsetId = charsetId;
        this.dateFormat = dateFormat;
    }

    public ColumnDefinition(String columnName, int columnType, int columnSize, short charsetId)
    {
        this(columnName, columnType, columnSize, charsetId, null);
    }

    public String getColumnName()
    {
        return columnName;
    }

    public int getDataType()
    {
        return dataType;
    }

    public int getDataSize()
    {
        return dataSize;
    }

    public short getCharsetId()
    {
        return charsetId;
    }

    public String getDateFormat()
    {
        return dateFormat;
    }
}
