package org.embulk.output.oracle.oci;

import java.util.List;

public class TableDefinition
{

    public final String tableName;
    public final short charsetId;
    public final ColumnDefinition[] columns;


    public TableDefinition(String tableName, short charsetId, ColumnDefinition... columns)
    {
        this.tableName = tableName;
        this.charsetId = charsetId;
        this.columns = columns;
    }

    public TableDefinition(String tableName, short charsetId, List<ColumnDefinition> columns)
    {
        this(tableName, charsetId, columns.toArray(new ColumnDefinition[columns.size()]));
    }
}
