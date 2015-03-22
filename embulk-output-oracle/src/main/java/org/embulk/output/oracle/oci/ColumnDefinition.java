package org.embulk.output.oracle.oci;

public class ColumnDefinition
{
    public static int SQLT_CHR = 1;
    public static int SQLT_INT = 3;

    public final String columnName;
    public final int columnType;
    public final int columnSize;
    public final String columnDateFormat;


    public ColumnDefinition(String columnName, int columnType, int columnSize, String columnDateFormat)
    {
        this.columnName = columnName;
        this.columnType = columnType;
        this.columnSize = columnSize;
        this.columnDateFormat = columnDateFormat;
    }

    public ColumnDefinition(String columnName, int columnType, int columnSize)
    {
        this(columnName, columnType, columnSize, null);
    }
}
