package org.embulk.output.oracle.oci;


public class OCI
{
    static {
        System.loadLibrary("embulk-oracle");
    }

    public native byte[] createContext();

    public native byte[] getLasetMessage(byte[] context);

    public native boolean open(byte[] context, String dbName, String userName, String password);

    public native boolean prepareLoad(byte[] context, TableDefinition tableDefinition);

    public native boolean loadBuffer(byte[] context, byte[] buffer, int rowCount);

    public native boolean commit(byte[] context);

    public native boolean rollback(byte[] context);

    public native void close(byte[] context);

}
