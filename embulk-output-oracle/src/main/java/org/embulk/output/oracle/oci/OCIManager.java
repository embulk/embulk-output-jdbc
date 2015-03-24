package org.embulk.output.oracle.oci;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.embulk.spi.Exec;
import org.slf4j.Logger;

public class OCIManager
{
    private static class OCIWrapperAndCounter {
        public OCIWrapper oci;
        public int counter;
    }


    private final Logger logger = Exec.getLogger(getClass());

    private Map<Object, OCIWrapperAndCounter> ociAndCounters = new HashMap<Object, OCIWrapperAndCounter>();


    public OCIWrapper open(Object key, String dbName, String userName, String password, TableDefinition tableDefinition) throws SQLException
    {
        synchronized(ociAndCounters) {
            OCIWrapperAndCounter ociAndCounter;
            if (ociAndCounters.containsKey(key)) {
                ociAndCounter = ociAndCounters.get(key);
            } else {
                logger.info(String.format("OCI : open for %s.", key));
                ociAndCounter = new OCIWrapperAndCounter();
                ociAndCounter.oci = new OCIWrapper();
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
            if (ociAndCounter != null) {
                ociAndCounter.counter--;
                if (ociAndCounter.counter == 0) {
                    logger.info(String.format("OCI : close for %s.", key));
                    ociAndCounter.oci.close();
                    ociAndCounters.remove(key);
                }
            }
        }
    }

}
