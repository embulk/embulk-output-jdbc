package org.embulk.output.jdbc;

import com.fasterxml.jackson.annotation.JsonProperty;


public class TableIdentifier
{
    private String tableName;

    public TableIdentifier(String tableName)
    {
        this.tableName = tableName;
    }

    public TableIdentifier()
    {
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
