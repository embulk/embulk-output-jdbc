package org.embulk.output.jdbc;

import org.embulk.spi.Column;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public interface Record
{
    boolean isNull(Column column);

    boolean getBoolean(Column column);

    long getLong(Column column);

    double getDouble(Column column);

    String getString(Column column);

    Timestamp getTimestamp(Column column);

    Value getJson(Column column);
}

