package org.embulk.output.redshift;

import org.embulk.config.ConfigException;

public enum Sse
{
    SSE,
    SSE_KMS,
    DISABLE;

    public static Sse fromString(String value)
    {
        switch(value) {
        case "SSE":
            return SSE;
        case "disable":
        case "false":
            return DISABLE;
        case "SSE_KMS":
            return SSE_KMS;
        default:
            throw new ConfigException(String.format("Unknown SSE value '%s'. Supported values are Sse, SSE, SSE_KMS, false or disable.", value));
        }
    }
}
