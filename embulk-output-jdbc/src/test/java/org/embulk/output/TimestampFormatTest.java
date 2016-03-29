package org.embulk.output;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.embulk.output.jdbc.TimestampFormat;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimestampFormatTest {

    @Test
    public void test() throws ParseException
    {
        Date date = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse("2015/03/04 17:08:09");
        Timestamp t = new Timestamp(date.getTime());

        {
            TimestampFormat format = new TimestampFormat("yyyy-MM-dd HH:mm:ss", 9);
            assertEquals("2015-03-04 17:08:09.000000000", format.format(t));
        }
        {
            TimestampFormat format = new TimestampFormat("yyyy-MM-dd HH:mm:ss", 0);
            assertEquals("2015-03-04 17:08:09", format.format(t));
        }
        {
            TimestampFormat format = new TimestampFormat("yyyy-MM-dd HH:mm:ss", 1);
            assertEquals("2015-03-04 17:08:09.0", format.format(t));
        }

        t.setNanos(1234567);
        {
            TimestampFormat format = new TimestampFormat("yyyy-MM-dd HH:mm:ss", 9);
            assertEquals("2015-03-04 17:08:09.001234567", format.format(t));
        }
        {
            TimestampFormat format = new TimestampFormat("yyyy-MM-dd HH:mm:ss", 2);
            assertEquals("2015-03-04 17:08:09.00", format.format(t));
        }
        {
            TimestampFormat format = new TimestampFormat("yyyy-MM-dd HH:mm:ss", 3);
            assertEquals("2015-03-04 17:08:09.001", format.format(t));
        }

        t.setNanos(123456789);
        {
            TimestampFormat format = new TimestampFormat("yyyy-MM-dd HH:mm:ss", 9);
            assertEquals("2015-03-04 17:08:09.123456789", format.format(t));
        }
        {
            TimestampFormat format = new TimestampFormat("yyyy-MM-dd HH:mm:ss", 1);
            assertEquals("2015-03-04 17:08:09.1", format.format(t));
        }
    }

}
