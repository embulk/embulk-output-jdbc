package org.embulk.output.oracle.oci;

import org.embulk.output.oracle.OracleCharset;

public class ColumnDefinition
{
    private final String columnName;
    private final int dataType;
    private final int dataSize;
    private final OracleCharset charset;
    private final String dateFormat;


    public ColumnDefinition(String columnName, int dataType, int dataSize, OracleCharset charset, String dateFormat)
    {
        this.columnName = columnName;
        this.dataType = dataType;
        this.dataSize = dataSize;
        this.charset = charset;
        this.dateFormat = dateFormat;
    }

    public ColumnDefinition(String columnName, int columnType, int columnSize, OracleCharset charset)
    {
        this(columnName, columnType, columnSize, charset, null);
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

    public OracleCharset getCharset()
    {
        return charset;
    }

    public String getDateFormat()
    {
        return dateFormat;
    }
}
