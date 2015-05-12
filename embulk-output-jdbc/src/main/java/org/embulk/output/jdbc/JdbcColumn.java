package org.embulk.output.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class JdbcColumn
{
    private final String name;
    private final String typeName;
    private final int sqlType;
    private final int sizeTypeParameter;
    private final int scaleTypeParameter;
    private final boolean isPrimaryKey;

    @JsonCreator
    public JdbcColumn(
            @JsonProperty("name") String name,
            @JsonProperty("typeName") String typeName,
            @JsonProperty("sqlType") int sqlType,
            @JsonProperty("sizeTypeParameter") int sizeTypeParameter,
            @JsonProperty("scaleTypeParameter") int scaleTypeParameter,
            @JsonProperty("primaryKey") boolean isPrimaryKey)
    {
        this.name = name;
        this.typeName = typeName;
        this.sqlType = sqlType;
        this.sizeTypeParameter = sizeTypeParameter;
        this.scaleTypeParameter = scaleTypeParameter;
        this.isPrimaryKey = isPrimaryKey;
    }

    @JsonIgnore
    public static JdbcColumn skipColumn()
    {
        return new JdbcColumn(null, null, 0, 0, 0, false);
    }

    @JsonIgnore
    public boolean isSkipColumn()
    {
        return name == null;
    }

    @JsonProperty("primaryKey")
    public boolean isPrimaryKey()
    {
        return isPrimaryKey;
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
