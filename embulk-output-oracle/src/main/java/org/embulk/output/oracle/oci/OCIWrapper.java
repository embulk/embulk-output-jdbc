package org.embulk.output.oracle.oci;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.SQLException;

import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.provider.BoundedMemoryIO;
import jnr.ffi.provider.jffi.ArrayMemoryIO;
import jnr.ffi.provider.jffi.ByteBufferMemoryIO;

import org.embulk.spi.Exec;
import org.slf4j.Logger;


public class OCIWrapper
{
    private final Logger logger = Exec.getLogger(getClass());

    private final Charset systemCharset;
    private final OCI oci;

    private Pointer envHandle;
    private Pointer errHandle;
    private Pointer svcHandlePointer;
    private Pointer svcHandle;
    private Pointer dpHandle;
    private Pointer dpcaHandle;
    private Pointer dpstrHandle;

    private TableDefinition tableDefinition;
    private int maxRowCount;

    private boolean errorOccured;
    private boolean committedOrRollbacked;


    public OCIWrapper()
    {
        // enable to change default encoding for test
        systemCharset = Charset.forName(System.getProperty("file.encoding"));

        logger.info("Loading OCI library.");
        oci = loadLibrary();
    }

    private OCI loadLibrary()
    {
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

    public void open(String dbName, String userName, String password) throws SQLException
    {
        Pointer envHandlePointer = createPointerPointer();
        check("OCIEnvCreate", oci.OCIEnvCreate(
                envHandlePointer,
                OCI.OCI_THREADED | OCI.OCI_OBJECT,
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
        svcHandlePointer = createPointerPointer();
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

    public void prepareLoad(TableDefinition tableDefinition) throws SQLException
    {
        this.tableDefinition = tableDefinition;

        // load table name
        Pointer tableName = createPointer(tableDefinition.getTableName());
        check("OCIAttrSet(OCI_ATTR_NAME)", oci.OCIAttrSet(
                dpHandle,
                OCI.OCI_HTYPE_DIRPATH_CTX,
                tableName,
                (int)tableName.size()
                , OCI.OCI_ATTR_NAME,
                errHandle));

        Pointer cols = createPointer((short)tableDefinition.getColumnCount());
        check("OCIAttrSet(OCI_ATTR_NUM_COLS)", oci.OCIAttrSet(
                dpHandle,
                OCI.OCI_HTYPE_DIRPATH_CTX,
                cols,
                (int)cols.size(),
                OCI.OCI_ATTR_NUM_COLS,
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
    }

    public void loadBuffer(RowBuffer rowBuffer) throws SQLException
    {
        Pointer pointer = new ByteBufferMemoryIO(Runtime.getSystemRuntime(), rowBuffer.getBuffer());
        short[] sizes = rowBuffer.getSizes();

        int i = 0;
        int position = 0;
        int rowCount = 0;
        for (int row = 0; row < rowBuffer.getRowCount(); row++) {
            for (short col = 0; col < tableDefinition.getColumnCount(); col++) {
                short size = sizes[i++];

                check("OCIDirPathColArrayEntrySet", oci.OCIDirPathColArrayEntrySet(
                        dpcaHandle,
                        errHandle,
                        rowCount,
                        col,
                        new BoundedMemoryIO(pointer, position, size),
                        size,
                        OCI.OCI_DIRPATH_COL_COMPLETE));

                position += size;
            }

            rowCount++;
            if (rowCount == maxRowCount) {
                loadRows(rowCount);
                rowCount = 0;
            }
        }

        if (rowCount > 0) {
            loadRows(rowCount);
        }
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
        logger.info("OCI : start to commit.");

        check("OCIDirPathFinish", oci.OCIDirPathFinish(dpHandle, errHandle));

        check("OCILogoff", oci.OCILogoff(svcHandle, errHandle));
        svcHandle = null;
    }

    public void rollback() throws SQLException
    {
        committedOrRollbacked = true;
        logger.info("OCI : start to rollback.");

        check("OCIDirPathAbort", oci.OCIDirPathAbort(dpHandle, errHandle));

        check("OCILogoff", oci.OCILogoff(svcHandle, errHandle));
        svcHandle = null;
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
        return new ArrayMemoryIO(Runtime.getSystemRuntime(), 8);
    }

    private Pointer createPointer(String s)
    {
        return Pointer.wrap(Runtime.getSystemRuntime(), ByteBuffer.wrap(s.getBytes()));
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
