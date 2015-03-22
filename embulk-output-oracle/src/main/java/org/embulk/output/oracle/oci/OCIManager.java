package org.embulk.output.oracle.oci;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class OCIManager
{
    private static class OCIWrapperAndCounter {
        public OCIWrapper oci;
        public int counter;
    }


    private Map<Object, OCIWrapperAndCounter> ociAndCounters = new HashMap<Object, OCIWrapperAndCounter>();


    public OCIWrapper open(Object key, String dbName, String userName, String password, Charset charset, TableDefinition tableDefinition) throws SQLException
    {
        synchronized(ociAndCounters) {
            OCIWrapperAndCounter ociAndCounter;
            if (ociAndCounters.containsKey(key)) {
                ociAndCounter = ociAndCounters.get(key);
            } else {
                ociAndCounter = new OCIWrapperAndCounter();
                ociAndCounter.oci = new OCIWrapper(charset);
                ociAndCounter.oci.open(dbName, userName, password);
                ociAndCounter.oci.prepareLoad(tableDefinition);
                ociAndCounters.put(key, ociAndCounter);
            }
            ociAndCounter.counter++;
            return ociAndCounter.oci;
        }
    }

    public OCIWrapper get(Object key)
    {
        synchronized(ociAndCounters) {
            return ociAndCounters.get(key).oci;
        }
    }

    public void close(Object key) throws SQLException
    {
        synchronized(ociAndCounters) {
            OCIWrapperAndCounter ociAndCounter = ociAndCounters.get(key);
            ociAndCounter.counter--;
            if (ociAndCounter.counter == 0) {
                ociAndCounter.oci.commit();
                ociAndCounter.oci.close();
                ociAndCounters.remove(key);
            }
        }
    }

}
