package org.embulk.output.oracle.oci;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;



public class OCIWrapper implements AutoCloseable
{
    private final OCI oci = new OCI();
    private byte[] context;


    public OCIWrapper()
    {
        context = oci.createContext();
    }

    public void open(String dbName, String userName, String password) throws SQLException
    {
        if (!oci.open(context, dbName, userName, password)) {
            throwException();
        }
    }

    public void prepareLoad(TableDefinition tableDefinition) throws SQLException
    {
        if (!oci.prepareLoad(context, tableDefinition)) {
            throwException();
        }
    }

    public void loadBuffer(byte[] buffer, int rowCount) throws SQLException
    {
        if (!oci.loadBuffer(context, buffer, rowCount)) {
            throwException();
        }
    }

    public void commit() throws SQLException {
        if (!oci.commit(context)) {
            throwException();
        }
    }

    public void rollback() throws SQLException {
        if (!oci.rollback(context)) {
            throwException();
        }
    }

    private void throwException() throws SQLException {
        byte[] message = oci.getLasetMessage(context);
        try {
            throw new SQLException(new String(message, "MS932"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void close()
    {
        if (context != null) {
            oci.close(context);
            context = null;
        }
    }

}
