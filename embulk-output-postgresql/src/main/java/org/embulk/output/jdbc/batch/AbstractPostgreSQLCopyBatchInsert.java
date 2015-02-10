package org.embulk.output.jdbc.batch;

import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.embulk.spi.Exec;
import org.embulk.output.jdbc.JdbcSchema;

public abstract class AbstractPostgreSQLCopyBatchInsert
        implements BatchInsert
{
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    protected static final String nullString = "\\N";
    protected static final String newLineString = "\n";
    protected static final String delimiterString = "\t";

    private final Logger logger = Exec.getLogger(PostgreSQLCopyBatchInsert.class);

    protected File file;
    protected BufferedWriter writer;
    protected int index;
    protected int batchRows;

    protected AbstractPostgreSQLCopyBatchInsert() throws IOException
    {
        this.index = 0;
        // TODO create temp file
    }

    protected AbstractPostgreSQLCopyBatchInsert(File file) throws IOException
    {
        this();
        reopenFile(file);
    }

    public void prepare(JdbcSchema insertSchema) throws SQLException
    {
    }

    protected void deleteFile()
    {
        file.delete();
    }

    protected void reopenFile(File newFile) throws IOException
    {
        closeFile();
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newFile), UTF_8));
        this.file = newFile;
    }

    protected void reopenFile() throws IOException
    {
        reopenFile(file);
    }

    protected void closeFile() throws IOException
    {
        if(writer != null) {
            writer.close();
            writer = null;
        }
    }

    public int getBatchWeight()
    {
        long fsize = file.length();
        if (fsize > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) fsize;
        }
    }

    public void add() throws IOException
    {
        writer.write(newLineString);
        batchRows++;
        index = 0;
    }

    private void appendDelimiter() throws IOException
    {
        if(index != 0) {
            writer.write(delimiterString);
        }
        index++;
    }

    public void setNull(int sqlType) throws IOException
    {
        appendDelimiter();
        writer.write(nullString);
    }

    public void setBoolean(boolean v) throws IOException
    {
        appendDelimiter();
        writer.write(String.valueOf(v));
    }

    public void setByte(byte v) throws IOException
    {
        appendDelimiter();
        setEscapedString(String.valueOf(v));
    }

    public void setShort(short v) throws IOException
    {
        appendDelimiter();
        writer.write(String.valueOf(v));
    }

    public void setInt(int v) throws IOException
    {
        appendDelimiter();
        writer.write(String.valueOf(v));
    }

    public void setLong(long v) throws IOException
    {
        appendDelimiter();
        writer.write(String.valueOf(v));
    }

    public void setFloat(float v) throws IOException
    {
        appendDelimiter();
        writer.write(String.valueOf(v));
    }

    public void setDouble(double v) throws IOException
    {
        appendDelimiter();
        writer.write(String.valueOf(v));
    }

    public void setBigDecimal(BigDecimal v) throws IOException
    {
        appendDelimiter();
        writer.write(String.valueOf(v));
    }

    public void setString(String v) throws IOException
    {
        appendDelimiter();
        setEscapedString(v);
    }

    public void setNString(String v) throws IOException
    {
        appendDelimiter();
        setEscapedString(v);
    }

    public void setBytes(byte[] v) throws IOException
    {
        appendDelimiter();
        setEscapedString(String.valueOf(v));
    }

    public void setSqlDate(Date v, int sqlType) throws IOException
    {
        appendDelimiter();
        writer.write(v.toString());
    }

    public void setSqlTime(Time v, int sqlType) throws IOException
    {
        appendDelimiter();
        writer.write(v.toString());
    }

    public void setSqlTimestamp(Timestamp v, int sqlType) throws IOException
    {
        appendDelimiter();
        writer.write(v.toString());
    }

    // Escape \, \n, \t, \r
    // Remove \0
    private void setEscapedString(String v) throws IOException{
        for (char c : v.toCharArray()) {
            String s;
            switch (c) {
            case '\\':
                s = "\\\\";
                break;
            case '\n':
                s = "\\n";
                break;
            case '\t':
                s = "\\t";
                break;
            case '\r':
                s = "\\r";
                break;
            case 0:
                s = "";
                break;
            default:
                s = String.valueOf(c);
            }
            writer.write(s);
        }
    }
}
