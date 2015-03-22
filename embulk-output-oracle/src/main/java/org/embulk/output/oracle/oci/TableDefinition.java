package org.embulk.output.oracle.oci;

import java.util.List;

public class TableDefinition
{

    public final String tableName;
    public final ColumnDefinition[] columns;


    public TableDefinition(String tableName, ColumnDefinition... columns)
    {
        this.tableName = tableName;
        this.columns = columns;
    }

    public TableDefinition(String tableName, List<ColumnDefinition> columns)
    {
        this(tableName, columns.toArray(new ColumnDefinition[columns.size()]));
    }
}
