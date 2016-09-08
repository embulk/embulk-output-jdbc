package org.embulk.output.oracle.oci;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.SQLException;

import jnr.ffi.LibraryLoader;
import jnr.ffi.Platform;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.provider.jffi.ArrayMemoryIO;
import jnr.ffi.provider.jffi.ByteBufferMemoryIO;

import org.embulk.spi.Exec;
import org.slf4j.Logger;


public class OCIWrapper
{
    private static final String PLUGIN_NAME = "embulk-output-oracle";

    private static OCI oci;
    private static BulkOCI bulkOci;

    private final Logger logger = Exec.getLogger(getClass());

    private final Charset systemCharset;
    private Pointer envHandle;
    private Pointer errHandle;
    private Pointer svcHandle;
    private Pointer dpHandle;
    private Pointer dpcaHandle;
    private Pointer dpstrHandle;

    private TableDefinition tableDefinition;
    private int maxRowCount;
    private long totalRows;
    private int loadCount;

    private boolean errorOccured;
    private boolean committedOrRollbacked;


    public OCIWrapper()
    {
        // enable to change default encoding for test
        systemCharset = Charset.forName(System.getProperty("file.encoding"));

        synchronized (OCIWrapper.class) {
            if (oci == null) {
                oci = loadOCILibrary();
            }
            if (bulkOci == null) {
                bulkOci = loadBulkOCILibrary(oci);
            }
        }
    }

    private OCI loadOCILibrary()
    {
        logger.info("OCI : Loading OCI library.");

        // "oci" for Windows, "clntsh" for Linux
        StringBuilder libraryNames = new StringBuilder();
        for (String libraryName : new String[]{"oci", "clntsh"}) {
            try {
                return LibraryLoader.create(OCI.class).failImmediately().load(libraryName);
            } catch (UnsatisfiedLinkError e) {
            }

            if (libraryNames.length() > 0) {
                libraryNames.append(" / ");
            }
            libraryNames.append(System.mapLibraryName(libraryName));
        }

        throw new UnsatisfiedLinkError("Cannot find library: " + libraryNames);
    }

    private BulkOCI loadBulkOCILibrary(OCI oci)
    {
        String libraryName = "embulk-output-oracle-oci";
        logger.info("OCI : Loading " + libraryName + " library.");

        Platform platform = Platform.getNativePlatform();

        File folder = getPluginRoot();
        folder = new File(new File(new File(new File(folder ,"lib"), "embulk"), "native"), platform.getName());

        File file = new File(folder, System.mapLibraryName(libraryName));
        if (!file.exists()) {
            logger.info("OCI : Library '" + file.getAbsolutePath() + "' doesn't exist, so Java implementation is used instead.");
            return new PrimitiveBulkOCI(oci);
        }

        logger.info("OCI : Library '" + file.getAbsolutePath() + "' is found.");
        return LibraryLoader.create(BulkOCI.class).search(folder.getAbsolutePath()).failImmediately().load(libraryName);
    }

    private File getPluginRoot()
    {
        try {
            URL url = getClass().getResource("/" + getClass().getName().replace('.', '/') + ".class");
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
        } catch (MalformedURLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void open(String dbName, String userName, String password) throws SQLException
    {
        Pointer envHandlePointer = createPointerPointer();
        // OCI_THREADED is not needed because synchronized in Java side.
        check("OCIEnvCreate", oci.OCIEnvCreate(
                envHandlePointer,
                /*OCI.OCI_THREADED |*/ OCI.OCI_OBJECT,
                null,
                null,
                null,
                null,
                0,
                null));
        envHandle = envHandlePointer.getPointer(0);

        // error handle
        Pointer errHandlePointer = createPointerPointer();
        check("OCIHandleAlloc(OCI_HTYPE_ERROR)", oci.OCIHandleAlloc(
                envHandle,
                errHandlePointer,
                OCI.OCI_HTYPE_ERROR,
                0,
                null));
        errHandle = errHandlePointer.getPointer(0);

        // service context
        Pointer svcHandlePointer = createPointerPointer();
        check("OCIHandleAlloc(OCI_HTYPE_SVCCTX)", oci.OCIHandleAlloc(
                envHandle,
                svcHandlePointer,
                OCI.OCI_HTYPE_SVCCTX,
                0,
                null));

        // logon
        // dbName should be defined in 'tnsnames.ora' or a form of "host:port/db"
        check("OCILogon", oci.OCILogon(
                envHandle,
                errHandle,
                svcHandlePointer,
                userName,
                userName.length(),
                password,
                password.length(),
                dbName,
                dbName.length()));
        svcHandle = svcHandlePointer.getPointer(0);

        Pointer dpHandlePointer = createPointerPointer();
        check("OCIHandleAlloc(OCI_HTYPE_DIRPATH_CTX)", oci.OCIHandleAlloc(
                envHandle,
                dpHandlePointer,
                OCI.OCI_HTYPE_DIRPATH_CTX,
                0,
                null));
        dpHandle = dpHandlePointer.getPointer(0);
    }

    public void prepareLoad(TableDefinition tableDefinition, int bufferSize) throws SQLException
    {
        this.tableDefinition = tableDefinition;

        check("OCIAttrSet(OCI_ATTR_BUF_SIZE)", oci.OCIAttrSet(
                dpHandle,
                OCI.OCI_HTYPE_DIRPATH_CTX,
                createPointer(bufferSize),
                4,
                OCI.OCI_ATTR_BUF_SIZE,
                errHandle));

        int numRows = Math.max(bufferSize / tableDefinition.getRowSize(), 16);
        check("OCIAttrSet(OCI_ATTR_NUM_ROWS)", oci.OCIAttrSet(
                dpHandle,
                OCI.OCI_HTYPE_DIRPATH_CTX,
                createPointer(numRows),
                4,
                OCI.OCI_ATTR_NUM_ROWS,
                errHandle));

        if (tableDefinition.getSchemaName() != null) {
            Pointer schemaName = createPointer(tableDefinition.getSchemaName());
            check("OCIAttrSet(OCI_ATTR_NAME)", oci.OCIAttrSet(
                    dpHandle,
                    OCI.OCI_HTYPE_DIRPATH_CTX,
                    schemaName,
                    (int)schemaName.size()
                    , OCI.OCI_ATTR_SCHEMA_NAME,
                    errHandle));
        }

        Pointer cols = createPointer((short)tableDefinition.getColumnCount());
        check("OCIAttrSet(OCI_ATTR_NUM_COLS)", oci.OCIAttrSet(
                dpHandle,
                OCI.OCI_HTYPE_DIRPATH_CTX,
                cols,
                (int)cols.size(),
                OCI.OCI_ATTR_NUM_COLS,
                errHandle));

        // load table name (case sensitive)
        Pointer tableName = createPointer("\"" + tableDefinition.getTableName() + "\"");
        check("OCIAttrSet(OCI_ATTR_NAME)", oci.OCIAttrSet(
                dpHandle,
                OCI.OCI_HTYPE_DIRPATH_CTX,
                tableName,
                (int)tableName.size()
                , OCI.OCI_ATTR_NAME,
                errHandle));

        Pointer noIndexErrors = createPointer((byte)1);
        check("OCIAttrSet(OCI_ATTR_DIRPATH_NO_INDEX_ERRORS)", oci.OCIAttrSet(
                dpHandle,
                OCI.OCI_HTYPE_DIRPATH_CTX,
                noIndexErrors,
                (int)noIndexErrors.size(),
                OCI.OCI_ATTR_DIRPATH_NO_INDEX_ERRORS,
                errHandle));

        Pointer columnsPointer = createPointerPointer();
        check("OCIAttrGet(OCI_ATTR_LIST_COLUMNS)", oci.OCIAttrGet(
                dpHandle,
                OCI.OCI_HTYPE_DIRPATH_CTX,
                columnsPointer,
                null,
                OCI.OCI_ATTR_LIST_COLUMNS,
                errHandle));
        Pointer columns = columnsPointer.getPointer(0);

        for (int i = 0; i < tableDefinition.getColumnCount(); i++) {
            ColumnDefinition columnDefinition = tableDefinition.getColumn(i);

            Pointer columnPointer = createPointerPointer();
            check("OCIParamGet(OCI_DTYPE_PARAM)", oci.OCIParamGet(
                    columns,
                    OCI.OCI_DTYPE_PARAM,
                    errHandle,
                    columnPointer,
                    i + 1));
            Pointer column = columnPointer.getPointer(0);

            Pointer columnName = createPointer(columnDefinition.getColumnName());
            check("OCIAttrSet(OCI_ATTR_NAME)", oci.OCIAttrSet(
                    column,
                    OCI.OCI_DTYPE_PARAM,
                    columnName,
                    (int)columnName.size(),
                    OCI.OCI_ATTR_NAME,
                    errHandle));

            Pointer dataType = createPointer(columnDefinition.getDataType());
            check("OCIAttrSet(OCI_ATTR_DATA_TYPE)", oci.OCIAttrSet(
                    column,
                    OCI.OCI_DTYPE_PARAM,
                    dataType,
                    (int)dataType.size(),
                    OCI.OCI_ATTR_DATA_TYPE,
                    errHandle));

            Pointer dataSize = createPointer(columnDefinition.getDataSize());
            check("OCIAttrSet(OCI_ATTR_DATA_SIZE)", oci.OCIAttrSet(
                    column,
                    OCI.OCI_DTYPE_PARAM,
                    dataSize,
                    (int)dataSize.size(),
                    OCI.OCI_ATTR_DATA_SIZE,
                    errHandle));

            // need to set charset explicitly because database charset is not set by default.
            Pointer charsetId = createPointer(columnDefinition.getCharset().getId());
            check("OCIAttrSet(OCI_ATTR_CHARSET_ID)", oci.OCIAttrSet(
                    column,
                    OCI.OCI_DTYPE_PARAM,
                    charsetId,
                    (int)charsetId.size(),
                    OCI.OCI_ATTR_CHARSET_ID,
                    errHandle));

            if (columnDefinition.getDateFormat() != null) {
                Pointer dateFormat = createPointer(columnDefinition.getDateFormat());
                check("OCIAttrSet(OCI_ATTR_DATEFORMAT)", oci.OCIAttrSet(
                        column,
                        OCI.OCI_DTYPE_PARAM,
                        dateFormat,
                        (int)dateFormat.size(),
                        OCI.OCI_ATTR_DATEFORMAT,
                        errHandle));
            }

            check("OCIDescriptorFree(OCI_DTYPE_PARAM)", oci.OCIDescriptorFree(
                    column,
                    OCI.OCI_DTYPE_PARAM));
        }

        check("OCIDirPathPrepare", oci.OCIDirPathPrepare(
                dpHandle,
                svcHandle,
                errHandle));

        // direct path column array
        Pointer dpcaHandlePointer = createPointerPointer();
        check("OCIHandleAlloc(OCI_HTYPE_DIRPATH_COLUMN_ARRAY)", oci.OCIHandleAlloc(
                dpHandle,
                dpcaHandlePointer,
                OCI.OCI_HTYPE_DIRPATH_COLUMN_ARRAY,
                0,
                null));
        dpcaHandle = dpcaHandlePointer.getPointer(0);

        Pointer dpstrHandlePointer = createPointerPointer();
        check("OCIHandleAlloc(OCI_HTYPE_DIRPATH_STREAM)", oci.OCIHandleAlloc(
                dpHandle,
                dpstrHandlePointer,
                OCI.OCI_HTYPE_DIRPATH_STREAM,
                0,
                null));
        dpstrHandle = dpstrHandlePointer.getPointer(0);

        Pointer maxRowCountPointer = createPointer(0);
        check("OCIAttrGet(OCI_ATTR_NUM_ROWS)", oci.OCIAttrGet(
                dpcaHandle,
                OCI.OCI_HTYPE_DIRPATH_COLUMN_ARRAY,
                maxRowCountPointer,
                null,
                OCI.OCI_ATTR_NUM_ROWS,
                errHandle));
        maxRowCount = maxRowCountPointer.getInt(0);
        logger.info(String.format("OCI : DirectPathColumnArray.numRows = %,d", maxRowCount));
    }

    public int getMaxRowCount() {
        return maxRowCount;
    }

    public void loadBuffer(ByteBuffer buffer, ByteBuffer sizes, int rowCount) throws SQLException
    {
        logger.info(String.format("Loading %,d rows", rowCount));
        long startTime = System.currentTimeMillis();

        check("OCIDirPathColArrayEntriesSet", bulkOci.embulk_output_oracle_OCIDirPathColArrayEntriesSet(
                dpcaHandle,
                errHandle,
                (short)tableDefinition.getColumnCount(),
                rowCount,
                new ByteBufferMemoryIO(Runtime.getSystemRuntime(), buffer),
                new ByteBufferMemoryIO(Runtime.getSystemRuntime(), sizes)));

        loadRows(rowCount);

        totalRows += rowCount;
        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
        logger.info(String.format("> %.2f seconds (loaded %,d rows in total)", seconds, totalRows));
    }

    private void loadRows(int rowCount) throws SQLException
    {
        for (int offset = 0; offset < rowCount;) {
            check("OCIDirPathStreamReset", oci.OCIDirPathStreamReset(
                    dpstrHandle,
                    errHandle));

            short result = oci.OCIDirPathColArrayToStream(
                    dpcaHandle,
                    dpHandle,
                    dpstrHandle,
                    errHandle,
                    rowCount,
                    offset);
            if (result != OCI.OCI_SUCCESS && result != OCI.OCI_CONTINUE) {
                check("OCIDirPathColArrayToStream", result);
            }

            loadCount++;
            check("OCIDirPathLoadStream", oci.OCIDirPathLoadStream(
                    dpHandle,
                    dpstrHandle,
                    errHandle));

            if (result == OCI.OCI_SUCCESS) {
                offset = rowCount;
            } else {
                Pointer loadedRowCount = createPointer(0);
                check("OCIAttrGet(OCI_ATTR_ROW_COUNT)", oci.OCIAttrGet(
                        dpcaHandle,
                        OCI.OCI_HTYPE_DIRPATH_COLUMN_ARRAY,
                        loadedRowCount,
                        null,
                        OCI.OCI_ATTR_ROW_COUNT,
                        errHandle));
                offset += loadedRowCount.getInt(0);
            }
        }
    }

    public void commit() throws SQLException
    {
        committedOrRollbacked = true;
        if (loadCount > 0) {
            logger.info(String.format("OCI : OCIDirPathLoadStream : %,d rows x %,d times.", totalRows / loadCount, loadCount));
        }
        logger.info("OCI : start to commit.");

        try {
            check("OCIDirPathFinish", oci.OCIDirPathFinish(dpHandle, errHandle));
        } finally {
            check("OCILogoff", oci.OCILogoff(svcHandle, errHandle));
            svcHandle = null;
        }
    }

    public void rollback() throws SQLException
    {
        committedOrRollbacked = true;
        logger.info("OCI : start to rollback.");

        try {
            check("OCIDirPathAbort", oci.OCIDirPathAbort(dpHandle, errHandle));
        } finally {
            check("OCILogoff", oci.OCILogoff(svcHandle, errHandle));
            svcHandle = null;
        }
    }

    public void close() throws SQLException
    {
        if (dpHandle != null) {
            try {
                if (!committedOrRollbacked) {
                    if (errorOccured) {
                        rollback();
                    } else {
                        commit();
                    }
                }
            } finally {
                freeHandle(OCI.OCI_HTYPE_DIRPATH_STREAM, dpstrHandle);
                dpcaHandle = null;

                freeHandle(OCI.OCI_HTYPE_DIRPATH_COLUMN_ARRAY, dpcaHandle);
                dpcaHandle = null;

                freeHandle(OCI.OCI_HTYPE_DIRPATH_CTX, dpHandle);
                dpHandle = null;

                freeHandle(OCI.OCI_HTYPE_SVCCTX, svcHandle);
                svcHandle = null;

                freeHandle(OCI.OCI_HTYPE_ERROR, errHandle);
                errHandle = null;

                freeHandle(OCI.OCI_HTYPE_ENV, envHandle);
                envHandle = null;
            }
        }
    }

    private Pointer createPointerPointer()
    {
        return new ArrayMemoryIO(Runtime.getSystemRuntime(), com.kenai.jffi.Type.POINTER.size());
    }

    private Pointer createPointer(String s)
    {
        // not database charset, but system charset of client
        return Pointer.wrap(Runtime.getSystemRuntime(), ByteBuffer.wrap(s.getBytes(systemCharset)));
    }

    private Pointer createPointer(byte n)
    {
        Pointer pointer = new ArrayMemoryIO(Runtime.getSystemRuntime(), 1);
        pointer.putByte(0, n);
        return pointer;
    }

    private Pointer createPointer(short n)
    {
        Pointer pointer = new ArrayMemoryIO(Runtime.getSystemRuntime(), 2);
        pointer.putShort(0, n);
        return pointer;
    }

    private Pointer createPointer(int n)
    {
        Pointer pointer = new ArrayMemoryIO(Runtime.getSystemRuntime(), 4);
        pointer.putInt(0, n);
        return pointer;
    }

    private void freeHandle(int type, Pointer handle)
    {
        if (handle != null) {
            try {
                check("OCIHandleFree", oci.OCIHandleFree(handle, type));
            } catch (SQLException e) {
                logger.warn(String.format("Warning (OCIHandleFree(%d)) : %s", type, e.getMessage()));
            }
        }
    }

    private void check(String operation, short result) throws SQLException
    {
        switch (result) {
            case OCI.OCI_SUCCESS:
                break;

            case OCI.OCI_ERROR:
                if (errHandle == null) {
                    throwException("OCI : %s failed : %d.", operation, result);
                }
                ArrayMemoryIO errrCode = new ArrayMemoryIO(Runtime.getSystemRuntime(), 4);
                ArrayMemoryIO buffer = new ArrayMemoryIO(Runtime.getSystemRuntime(), 512);
                if (oci.OCIErrorGet(errHandle, 1, null, errrCode, buffer, (int)buffer.size(), OCI.OCI_HTYPE_ERROR) != OCI.OCI_SUCCESS) {
                    throwException("OCI : %s failed : %d. OCIErrorGet failed.", operation, result);
                }

                String message = new String(buffer.array(), systemCharset);
                throwException("OCI : %s failed : %d. %s", operation, result, message);

            case OCI.OCI_INVALID_HANDLE:
                throwException("OCI : %s failed : invalid handle.", operation);

            case OCI.OCI_NO_DATA:
                throwException("OCI : %s failed : no data.", operation);

            default:
                throwException("OCI : %s failed : %d.", operation, result);
        }
    }

    private void throwException(String format, Object... args) throws SQLException
    {
        errorOccured = true;
        String message = String.format(format, args);
        logger.error(message);
        throw new SQLException(message);
    }
}
