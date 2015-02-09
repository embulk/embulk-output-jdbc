package org.embulk.output.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JdbcColumn
{
    private String name;
    private String typeName;
    private int sqlType;
    private int sizeTypeParameter;
    private int scaleTypeParameter;

    @JsonCreator
    public JdbcColumn(
            @JsonProperty("name") String name,
            @JsonProperty("typeName") String typeName,
            @JsonProperty("sqlType") int sqlType,
            @JsonProperty("sizeTypeParameter") int sizeTypeParameter,
            @JsonProperty("scaleTypeParameter") int scaleTypeParameter)
    {
        this.name = name;
        this.typeName = typeName;
        this.sqlType = sqlType;
        this.sizeTypeParameter = sizeTypeParameter;
        this.scaleTypeParameter = scaleTypeParameter;
    }

    @JsonProperty("name")
    public String getName()
    {
        return name;
    }

    @JsonProperty("typeName")
    public String getTypeName()
    {
        return typeName;
    }

    @JsonProperty("sqlType")
    public int getSqlType()
    {
        return sqlType;
    }

    @JsonProperty("sizeTypeParameter")
    public int getSizeTypeParameter()
    {
        return sizeTypeParameter;
    }

    @JsonProperty("scaleTypeParameter")
    public int getScaleTypeParameter()
    {
        return scaleTypeParameter;
    }
}
