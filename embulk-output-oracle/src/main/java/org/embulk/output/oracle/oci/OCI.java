package org.embulk.output.oracle.oci;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import org.embulk.spi.Exec;
import org.slf4j.Logger;


public class OCI
{
    private static final Logger logger = Exec.getLogger(OCI.class);

    private static final String PLUGIN_NAME = "embulk-output-oracle";

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

    private static void loadLibrary() throws MalformedURLException, URISyntaxException
    {
        loadLibrary(getPluginRoot());
    }

    private static File getPluginRoot() throws MalformedURLException, URISyntaxException
    {
        URL url = OCI.class.getResource("/" + OCI.class.getName().replace('.', '/') + ".class");
        if (url.toString().startsWith("jar:")) {
            url = new URL(url.toString().replaceAll("^jar:", "").replaceAll("![^!]*$", ""));
        }

        File folder = new File(url.toURI()).getParentFile();
        for (;; folder = folder.getParentFile()) {
            if (folder == null) {
                String message = String.format("OCI : %s folder not found.", PLUGIN_NAME);
                throw new RuntimeException(message);
            }

            if (folder.getName().startsWith(PLUGIN_NAME)) {
                return folder;
            }
        }
    }

    private static void loadLibrary(File folder)
    {
        File lib = new File(new File(folder, "lib"), "embulk");

        String osName = System.getProperty("os.name");
        String osArch = System.getProperty("os.arch");

        String libraryName = System.mapLibraryName(PLUGIN_NAME);
        File libFolder = null;

        if (osName.startsWith("Windows")) {
            if (osArch.endsWith("64")) {
                libFolder = new File(lib, "win_x64");
            } else if (osArch.equals("x86")) {
                libFolder = new File(lib, "win_x86");
            }
        } else if (osName.equals("Linux")) {
            if (osArch.endsWith("64")) {
                libFolder = new File(lib, "linux_x64");
            } else if (osArch.equals("x86")) {
                libFolder = new File(lib, "linux_x86");
            }
        }

        if (libFolder != null) {
            File libFile = new File(libFolder, libraryName);
            if (libFile.exists()) {
                logger.info(String.format("OCI : load library \"%s\".", libFile.getAbsolutePath()));
                System.load(libFile.getAbsolutePath());
                return;
            }
        }

        logger.info(String.format("OCI : library \"%s\" for %s %s doesn't exist in lib folder.", libraryName, osName, osArch));
        System.loadLibrary(PLUGIN_NAME);
    }
}
