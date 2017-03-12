package org.embulk.output.redshift;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import org.embulk.config.ConfigException;

public enum EncryptOption
{
    SSE,
    SSE_KMS,
    DISABLE;

    @JsonValue
    @Override
    public String toString()
    {
        return this.name().toLowerCase();
    }

    @JsonCreator
    public static EncryptOption fromString(String value)
    {
        switch(value) {
        case "sse":
            return SSE;
        case "disable":
        case "false":
            return DISABLE;
        case "sse_kms":
            return SSE_KMS;
        default:
            throw new ConfigException(String.format("Unknown EncryptOption value '%s'. Supported values are EncryptOption, sse, sse_kms, false or disable.", value));
        }
    }
}
