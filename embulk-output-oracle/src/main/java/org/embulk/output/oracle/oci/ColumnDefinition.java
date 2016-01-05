package org.embulk.output.oracle.oci;

public class ColumnDefinition
{
    public final String columnName;
    public final int columnType;
    public final int columnSize;
    public final short charsetId;
    public final String columnDateFormat;


    public ColumnDefinition(String columnName, int columnType, int columnSize, short charsetId, String columnDateFormat)
    {
        this.columnName = columnName;
        this.columnType = columnType;
        this.columnSize = columnSize;
        this.charsetId = charsetId;
        this.columnDateFormat = columnDateFormat;
    }

    public ColumnDefinition(String columnName, int columnType, int columnSize, short charsetId)
    {
        this(columnName, columnType, columnSize, charsetId, null);
    }
}
