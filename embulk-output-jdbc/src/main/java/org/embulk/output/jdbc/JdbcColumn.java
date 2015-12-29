package org.embulk.output.jdbc;

import com.google.common.base.Optional;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class JdbcColumn
{
    private final String name;
    private final String simpleTypeName;
    private final int sqlType;
    private final int sizeTypeParameter;
    private final int scaleTypeParameter;
    private final int dataLength;
    private final Optional<String> declaredType;
    private final boolean isNotNull;
    private final boolean isUniqueKey;

    @JsonCreator
    public JdbcColumn(
            @JsonProperty("name") String name,
            @JsonProperty("sqlType") int sqlType,
            @JsonProperty("simpleTypeName") String simpleTypeName,
            @JsonProperty("sizeTypeParameter") int sizeTypeParameter,
            @JsonProperty("scaleTypeParameter") int scaleTypeParameter,
            @JsonProperty("dataLength") int dataLength,
            @JsonProperty("declaredType") Optional<String> declaredType,
            @JsonProperty("notNull") boolean isNotNull,
            @JsonProperty("uniqueKey") boolean isUniqueKey)
    {
        this.name = name;
        this.simpleTypeName = simpleTypeName;
        this.sqlType = sqlType;
        this.sizeTypeParameter = sizeTypeParameter;
        this.scaleTypeParameter = scaleTypeParameter;
        this.dataLength = dataLength;
        this.declaredType = declaredType;
        this.isNotNull = isNotNull;
        this.isUniqueKey = isUniqueKey;
    }

    public static JdbcColumn newGenericTypeColumn(String name, int sqlType,
            String simpleTypeName, int sizeTypeParameter, int scaleTypeParameter, int dataLength,
            boolean isNotNull, boolean isUniqueKey)
    {
        return new JdbcColumn(name, sqlType,
                simpleTypeName, sizeTypeParameter, scaleTypeParameter, dataLength,
                Optional.<String>absent(), isNotNull, isUniqueKey);
    }

    public static JdbcColumn newGenericTypeColumn(String name, int sqlType,
            String simpleTypeName, int sizeTypeParameter, int scaleTypeParameter,
            boolean isNotNull, boolean isUniqueKey)
    {
        return new JdbcColumn(name, sqlType,
                simpleTypeName, sizeTypeParameter, scaleTypeParameter, sizeTypeParameter,
                Optional.<String>absent(), isNotNull, isUniqueKey);
    }

    public static JdbcColumn newTypeDeclaredColumn(String name, int sqlType,
            String declaredType, boolean isNotNull, boolean isUniqueKey)
    {
        return new JdbcColumn(name, sqlType,
                declaredType, 0, 0, 0,
                Optional.of(declaredType), isNotNull, isUniqueKey);
    }

    @JsonIgnore
    public static JdbcColumn skipColumn()
    {
        return new JdbcColumn(null, 0, null, 0, 0, 0, Optional.<String>absent(), false, false);
    }

    @JsonIgnore
    public boolean isSkipColumn()
    {
        return name == null;
    }

    @JsonProperty("name")
    public String getName()
    {
        return name;
    }

    @JsonProperty("sqlType")
    public int getSqlType()
    {
        return sqlType;
    }

    @JsonProperty("simpleTypeName")
    public String getSimpleTypeName()
    {
        return simpleTypeName;
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

    @JsonProperty("dataLength")
    public int getDataLength()
    {
        return dataLength;
    }

    @JsonProperty("declaredType")
    public Optional<String> getDeclaredType()
    {
        return declaredType;
    }

    @JsonProperty("notNull")
    public boolean isNotNull()
    {
        return isNotNull;
    }

    @JsonProperty("uniqueKey")
    public boolean isUniqueKey()
    {
        return isUniqueKey;
    }
}
