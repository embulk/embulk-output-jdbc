package org.embulk.output.oracle.oci;

import java.util.List;

public class TableDefinition
{
    private final String schemaName;
    private final String tableName;
    private final ColumnDefinition[] columns;


    public TableDefinition(String schemaName, String tableName, ColumnDefinition... columns)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = columns;
    }

    public TableDefinition(String schemaName, String tableName, List<ColumnDefinition> columns)
    {
        this(schemaName, tableName, columns.toArray(new ColumnDefinition[columns.size()]));
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public int getColumnCount()
    {
        return columns.length;
    }

    public ColumnDefinition getColumn(int index)
    {
        return columns[index];
    }

    public int getRowSize()
    {
        int rowSize = 0;
        for (ColumnDefinition column : columns) {
            rowSize += column.getDataSize();
        }
        return rowSize;
    }
}
