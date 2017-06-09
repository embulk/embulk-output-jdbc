package org.embulk.output.jdbc;

import com.fasterxml.jackson.annotation.JsonProperty;


public class TableIdentifier
{
    private String schemaName;
    private String tableName;

    public TableIdentifier(String shcemaName, String tableName)
    {
        this.schemaName = shcemaName;
        this.tableName = tableName;
    }

    public TableIdentifier()
    {
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
