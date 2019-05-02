package org.embulk.output.jdbc;

import java.sql.Connection;
import java.util.Locale;

import org.embulk.config.ConfigException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum TransactionIsolation {
    READ_UNCOMMITTED {
        @Override
        public int toInt() {
            return Connection.TRANSACTION_READ_UNCOMMITTED;
        }
    },
    READ_COMMITTED {
        @Override
        public int toInt() {
            return Connection.TRANSACTION_READ_COMMITTED;
        }
    },
    REPEATABLE_READ {
        @Override
        public int toInt() {
            return Connection.TRANSACTION_REPEATABLE_READ;
        }
    },
    SERIALIZABLE {
        @Override
        public int toInt() {
            return Connection.TRANSACTION_SERIALIZABLE;
        }
    };

    @JsonValue
    @Override
    public String toString()
    {
        return name().toLowerCase(Locale.ENGLISH);
    }

    public abstract int toInt();

    @JsonCreator
    public static TransactionIsolation fromString(String value)
    {
        for (TransactionIsolation ti : values()) {
            if (ti.toString().equals(value)) {
                return ti;
            }
        }
        throw new ConfigException(String.format("Unknown transaction_isolation '%s'.", value));
    }

    public static TransactionIsolation fromInt(int value)
    {
        for (TransactionIsolation ti : values()) {
            if (ti.toInt() == value) {
                return ti;
            }
        }
        throw new IllegalArgumentException(String.format("Unknown transaction_isolation '%d'.", value));
    }
}
