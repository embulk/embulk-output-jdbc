package org.embulk.output.sqlserver;

import java.text.FieldPosition;
import java.text.SimpleDateFormat;
import java.util.Date;


public class SmallDateTimeFormat extends SimpleDateFormat
{
    public SmallDateTimeFormat(String pattern)
    {
        super(pattern);
    }

    @Override
    public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition pos)
    {
        long time = date.getTime();

        // round seconds
        long underMinutes = time % 60000;
        if (underMinutes < 30000) {
            time -= underMinutes;
        } else {
            time += 60000 - underMinutes;
        }

        return super.format(new Date(time), toAppendTo, pos);
    }

}
