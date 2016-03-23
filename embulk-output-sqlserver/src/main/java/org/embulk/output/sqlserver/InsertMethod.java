package org.embulk.output.sqlserver;

import java.util.Locale;

import org.embulk.config.ConfigException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum InsertMethod
{
    NORMAL,
    NATIVE;

    @JsonValue
    @Override
    public String toString()
    {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @JsonCreator
    public static InsertMethod fromString(String value)
    {
        for (InsertMethod insertMethod : InsertMethod.values()) {
            if (insertMethod.toString().equals(value)) {
                return insertMethod;
            }
        }
        throw new ConfigException(String.format("Unknown insert_method '%s'.", value));
    }
}
