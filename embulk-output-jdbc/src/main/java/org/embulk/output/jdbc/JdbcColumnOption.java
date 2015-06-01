package org.embulk.output.jdbc;

import com.google.common.base.Optional;
import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;
import org.embulk.config.Task;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.spi.time.TimestampFormat;

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
    public TimestampFormat getTimestampFormat();

    @Config("timezone")
    @ConfigDefault("null")
    public Optional<DateTimeZone> getTimeZone();

    // required by TimestampFormatter
    @ConfigInject
    public ScriptingContainer getJRuby();
}
