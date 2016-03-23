package org.embulk.output.jdbc;

import java.sql.Timestamp;
import java.text.FieldPosition;
import java.text.SimpleDateFormat;
import java.util.Date;


public class TimestampFormat extends SimpleDateFormat
{

    private final int scale;

    public TimestampFormat(String pattern, int scale)
    {
        super(pattern);

        this.scale = scale;
    }

    @Override
    public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition pos)
    {
        StringBuffer buffer = super.format(date, toAppendTo, pos);
        if (scale > 0) {
            buffer.append('.');
            String nanos = Integer.toString(((Timestamp)date).getNanos());
            int zeros = Math.min(scale, 9 - nanos.length());
            for (int i = 0; i < zeros; i++) {
                buffer.append('0');
            }
            buffer.append(nanos.substring(0, scale - zeros));
        }
        return buffer;
    }

}
