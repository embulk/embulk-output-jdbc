package org.embulk.output.sqlserver;

import java.sql.Time;
import java.sql.Timestamp;

public class PreciseTime extends Time
{
    private final int nanos;

    public PreciseTime(long time, int nanos)
    {
        super(time);

        this.nanos = nanos;
    }

    public PreciseTime(Time time, int nanos)
    {
        this(time.getTime(), nanos);
    }

    public PreciseTime(Timestamp timestamp)
    {
        this(timestamp.getTime(), timestamp.getNanos());
    }

    public int getNanos()
    {
        return nanos;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof PreciseTime)) {
            return false;
        }

        return toString().equals(o.toString());
    }

    @Override
    public String toString()
    {
        return String.format("%s.%09d", super.toString(), nanos);
    }

}
