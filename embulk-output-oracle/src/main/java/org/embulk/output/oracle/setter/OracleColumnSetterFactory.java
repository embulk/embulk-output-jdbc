package org.embulk.output.oracle.setter;

import java.sql.Types;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.setter.ColumnSetter;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.embulk.output.jdbc.setter.StringColumnSetter;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.TimestampFormatter;

public class OracleColumnSetterFactory extends ColumnSetterFactory
{
	public OracleColumnSetterFactory(BatchInsert batch, PageReader pageReader,
			TimestampFormatter timestampFormatter)
	{
		super(batch, pageReader, timestampFormatter);
	}
	
	@Override
	public ColumnSetter newColumnSetter(JdbcColumn column)
    {
		switch (column.getSqlType()) {
			case Types.DECIMAL:
	            return new StringColumnSetter(batch, pageReader, column, timestampFormatter);
            default:
        		return super.newColumnSetter(column);
		}
	}
}
