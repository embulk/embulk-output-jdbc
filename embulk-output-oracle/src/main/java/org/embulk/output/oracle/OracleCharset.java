package org.embulk.output.oracle;

import java.nio.charset.Charset;

public class OracleCharset
{
    public final String charsetName;
    public final short charstId;
    public final Charset javaCharset;


    public OracleCharset(String charsetName, short charstId, Charset javaCharset)
    {
        this.charsetName = charsetName;
        this.charstId = charstId;
        this.javaCharset = javaCharset;
    }

}
