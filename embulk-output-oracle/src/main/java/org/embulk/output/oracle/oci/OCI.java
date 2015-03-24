package org.embulk.output.oracle.oci;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;


public class OCI
{
    static {
        try {
            loadLibrary();
        } catch (MalformedURLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public native byte[] createContext();

    public native byte[] getLasetMessage(byte[] context);

    public native boolean open(byte[] context, String dbName, String userName, String password);

    public native boolean prepareLoad(byte[] context, TableDefinition tableDefinition);

    public native boolean loadBuffer(byte[] context, byte[] buffer, int rowCount);

    public native boolean commit(byte[] context);

    public native boolean rollback(byte[] context);

    public native void close(byte[] context);

    private static void loadLibrary() throws MalformedURLException, URISyntaxException {
        URL url = OCI.class.getResource("/" + OCI.class.getName().replace('.', '/') + ".class");
        if (url.toString().startsWith("jar:")) {
            url = new URL(url.toString().replaceAll("^jar:", "").replaceAll("![^!]*$", ""));
        }

        File folder = new File(url.toURI()).getParentFile();
        for (;; folder = folder.getParentFile()) {
            if (folder == null) {
                throw new RuntimeException("embulk-oracle library not found.");
            }

            if (folder.getName().startsWith("embulk-output-oracle")) {
                break;
            }
        }




        System.out.println(folder);

        if (!loadLibrary(folder)) {
            throw new RuntimeException("embulk-oracle library not found.");
        }
    }

    private static boolean loadLibrary(File folder) {
        String libraryName = System.mapLibraryName("embulk-oracle");
        for (File child : folder.listFiles()) {
            if (child.isFile()) {
                if (child.getName().equals(libraryName)) {
                    System.load(child.getAbsolutePath());
                    return true;
                }
            }
            if (child.isDirectory()) {
                if (loadLibrary(child)) {
                    return true;
                }
            }
        }
        return false;
    }
}
