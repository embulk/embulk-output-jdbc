package org.embulk.output.redshift;

import java.lang.IllegalArgumentException;

public enum Ssl
{
    enable,
    disable,
    verify;

    public static Enum fromString(String value)
    {
        switch(value) {
        case "enable":
        case "true":
            return enable;
        case "disable":
        case "false":
            return disable;
        case "verify":
            return verify;
        default:
            throw new IllegalArgumentException("No constant with value " + value + " found. Supported values are: enable, disable, verify.");
        }
    }
}
