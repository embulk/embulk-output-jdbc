package org.embulk.output.jdbc;

import java.time.ZoneId;
import java.util.Optional;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

public interface JdbcColumnOption
        extends Task
{
    @Config("type")
    @ConfigDefault("null")
    public Optional<String> getType();

    @Config("value_type")
    @ConfigDefault("\"coerce\"")
    public String getValueType();

    @Config("timestamp_format")
    @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%6N\"")
    public String getTimestampFormat();

    @Config("timezone")
    @ConfigDefault("null")
    public Optional<ZoneId> getTimeZone();
}
