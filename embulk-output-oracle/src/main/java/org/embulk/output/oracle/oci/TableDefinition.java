package org.embulk.output.oracle.oci;

import java.util.List;

public class TableDefinition
{
    private final String tableName;
    private final ColumnDefinition[] columns;


    public TableDefinition(String tableName, ColumnDefinition... columns)
    {
        this.tableName = tableName;
        this.columns = columns;
    }

    public TableDefinition(String tableName, List<ColumnDefinition> columns)
    {
        this(tableName, columns.toArray(new ColumnDefinition[columns.size()]));
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
}
