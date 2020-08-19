package org.embulk.output.jdbc;

import java.time.Instant;
import org.embulk.spi.Column;
import org.msgpack.value.Value;

public interface Record
{
    boolean isNull(Column column);

    boolean getBoolean(Column column);

    long getLong(Column column);

    double getDouble(Column column);

    String getString(Column column);

    Instant getTimestamp(Column column);

    Value getJson(Column column);
}
