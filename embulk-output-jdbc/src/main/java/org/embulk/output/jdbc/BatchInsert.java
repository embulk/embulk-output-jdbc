package org.embulk.output.jdbc;

import java.math.BigDecimal;
import java.util.Calendar;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public interface BatchInsert
{
    public void prepare(String loadTable, JdbcSchema insertSchema) throws SQLException;

    public int getBatchWeight();

    public void add() throws IOException, SQLException;

    public void close() throws IOException, SQLException;

    public void flush() throws IOException, SQLException;

    public void finish() throws IOException, SQLException;

    public void setNull(int sqlType) throws IOException, SQLException;

    public void setBoolean(boolean v) throws IOException, SQLException;

    public void setByte(byte v) throws IOException, SQLException;

    public void setShort(short v) throws IOException, SQLException;

    public void setInt(int v) throws IOException, SQLException;

    public void setLong(long v) throws IOException, SQLException;

    public void setFloat(float v) throws IOException, SQLException;

    public void setDouble(double v) throws IOException, SQLException;

    public void setBigDecimal(BigDecimal v) throws IOException, SQLException;

    public void setString(String v) throws IOException, SQLException;

    public void setNString(String v) throws IOException, SQLException;

    public void setBytes(byte[] v) throws IOException, SQLException;

    public void setSqlDate(Date v, Calendar cal) throws IOException, SQLException;

    public void setSqlTime(Time v, Calendar cal) throws IOException, SQLException;

    public void setSqlTimestamp(Timestamp v, Calendar cal) throws IOException, SQLException;
}
