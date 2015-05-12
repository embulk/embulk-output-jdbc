package org.embulk.output.jdbc.setter;

import java.math.BigDecimal;
import java.io.IOException;
import java.sql.SQLException;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class BigDecimalColumnSetter
        extends ColumnSetter
{
    private static final BigDecimal ZERO = BigDecimal.valueOf(0L);
    private static final BigDecimal ONE = BigDecimal.valueOf(1L);

    public BigDecimalColumnSetter(BatchInsert batch, PageReader pageReader,
            JdbcColumn column)
    {
        super(batch, pageReader, column);
    }

    @Override
    protected void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setBigDecimal(v ? ONE : ZERO);
    }

    @Override
    protected void longValue(long v) throws IOException, SQLException
    {
        batch.setBigDecimal(BigDecimal.valueOf(v));
    }

    @Override
    protected void doubleValue(double v) throws IOException, SQLException
    {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            nullValue();
        } else {
            batch.setBigDecimal(BigDecimal.valueOf(v));
        }
    }

    @Override
    protected void stringValue(String v) throws IOException, SQLException
    {
        BigDecimal dv;
        try {
            dv = new BigDecimal(v);
        } catch (NumberFormatException ex) {
            nullValue();
            return;
        }
        batch.setBigDecimal(dv);
    }

    @Override
    protected void timestampValue(Timestamp v) throws IOException, SQLException
    {
        nullValue();
    }
}
