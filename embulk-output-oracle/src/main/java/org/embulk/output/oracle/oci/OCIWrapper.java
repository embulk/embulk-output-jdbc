package org.embulk.output.oracle.oci;

import java.nio.charset.Charset;
import java.sql.SQLException;

import org.embulk.spi.Exec;
import org.slf4j.Logger;


public class OCIWrapper implements AutoCloseable
{
    private final Logger logger = Exec.getLogger(getClass());

    private final OCI oci = new OCI();
    // used for messages
    private final Charset defaultCharset;
    private byte[] context;
    private boolean errorOccured;
    private boolean committedOrRollbacked;


    public OCIWrapper()
    {
        // enable to change default encoding for test
        defaultCharset = Charset.forName(System.getProperty("file.encoding"));
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

    public void commit() throws SQLException
    {
        committedOrRollbacked = true;
        logger.info("OCI : start to commit.");
        if (!oci.commit(context)) {
            throwException();
        }
    }

    public void rollback() throws SQLException
    {
        committedOrRollbacked = true;
        logger.info("OCI : start to rollback.");
        if (!oci.rollback(context)) {
            throwException();
        }
    }

    private void throwException() throws SQLException
    {
        errorOccured = true;
        String message = new String(oci.getLasetMessage(context), defaultCharset);
        logger.error(message);
        throw new SQLException(message);
    }


    @Override
    public void close() throws SQLException
    {
        if (context != null) {
            try {
                if (!committedOrRollbacked) {
                    if (errorOccured) {
                        rollback();
                    } else {
                        commit();
                    }
                }
            } finally {
                oci.close(context);
                context = null;
            }
        }
    }

}
