package org.embulk.output.jdbc;

import java.util.Optional;
import org.embulk.config.Task;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;

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
    public Optional<String> getTimeZone();
}
