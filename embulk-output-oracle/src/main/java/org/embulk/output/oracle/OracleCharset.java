package org.embulk.output.oracle;

import java.nio.charset.Charset;

public class OracleCharset
{
    private final String name;
    private final short id;
    private final Charset javaCharset;

    public OracleCharset(String name, short id, Charset javaCharset)
    {
        this.name = name;
        this.id = id;
        this.javaCharset = javaCharset;
    }

    public String getName()
    {
        return name;
    }

    public short getId()
    {
        return id;
    }

    public Charset getJavaCharset()
    {
        return javaCharset;
    }
}
