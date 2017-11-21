package org.embulk.output.sqlserver.nativeclient;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import jnr.ffi.LibraryLoader;
import jnr.ffi.Platform;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.provider.jffi.ArrayMemoryIO;

import org.embulk.spi.Exec;
import org.slf4j.Logger;

import com.google.common.base.Optional;

public class NativeClientWrapper
{
    private static ODBC odbc;
    private static NativeClient client;

    private final Logger logger = Exec.getLogger(getClass());

    private Charset charset;
    private Charset wideCharset;
    private Pointer envHandle;
    private Pointer odbcHandle;

    private Map<Integer, Pointer> boundPointers = new HashMap<Integer, Pointer>();

    public NativeClientWrapper()
    {
        Platform platform = Platform.getPlatform();
        Platform.OS os = platform.getOS();
        String odbcLibName;
        String nativeClientLibName;
        if (os == Platform.OS.WINDOWS) {
          odbcLibName = "odbc32";
          nativeClientLibName = "sqlncli11";
        } else {
          odbcLibName = "odbc";
          nativeClientLibName = "msodbcsql";
        }
        synchronized (NativeClientWrapper.class) {
            if (odbc == null) {
                logger.info(String.format("Loading SQL Server Native Client library (%s).", odbcLibName));
                try {
                    odbc = LibraryLoader.create(ODBC.class).failImmediately().load(odbcLibName);
                } catch (UnsatisfiedLinkError e) {
                    throw new RuntimeException(platform.mapLibraryName(odbcLibName) + " not found.", e);
                }
            }
            if (client == null) {
                logger.info(String.format("Loading SQL Server Native Client library (%s).", nativeClientLibName));
                try {
                    client = LibraryLoader.create(NativeClient.class).failImmediately().load(nativeClientLibName);
                } catch (UnsatisfiedLinkError e) {
                    throw new RuntimeException(platform.mapLibraryName(nativeClientLibName) + " not found.", e);
                }
            }
        }
    }

    public void open(String server, int port, Optional<String> instance,
            String database, Optional<String> user, Optional<String> password,
            String table, Optional<String> nativeDriverName,
            String databaseEncoding)
                    throws SQLException
    {
        // environment handle
        charset = Charset.forName(databaseEncoding);
        wideCharset = Charset.forName("UTF-16LE");
        Pointer envHandlePointer = createPointerPointer();
        checkSQLResult("SQLAllocHandle(SQL_HANDLE_ENV)", odbc.SQLAllocHandle(
                ODBC.SQL_HANDLE_ENV,
                null,
                envHandlePointer));
        envHandle = envHandlePointer.getPointer(0);

        // set ODBC version
        checkSQLResult("SQLSetEnvAttr(SQL_ATTR_ODBC_VERSION)", odbc.SQLSetEnvAttr(
                envHandle,
                ODBC.SQL_ATTR_ODBC_VERSION,
                Pointer.wrap(Runtime.getSystemRuntime(), ODBC.SQL_OV_ODBC3),
                ODBC.SQL_IS_INTEGER));

        // ODBC handle
        Pointer odbcHandlePointer = createPointerPointer();
        checkSQLResult("SQLAllocHandle(SQL_HANDLE_DBC)", odbc.SQLAllocHandle(
                ODBC.SQL_HANDLE_DBC,
                envHandle,
                odbcHandlePointer));
        odbcHandle = odbcHandlePointer.getPointer(0);

        // set BULK COPY mode
        checkSQLResult("SQLSetConnectAttr(SQL_COPT_SS_BCP)", odbc.SQLSetConnectAttrW(
                odbcHandle,
                ODBC.SQL_COPT_SS_BCP,
                Pointer.wrap(Runtime.getSystemRuntime(), ODBC.SQL_BCP_ON),
                ODBC.SQL_IS_INTEGER));

        StringBuilder connectionString = new StringBuilder();
        if (nativeDriverName.isPresent()) {
            connectionString.append(String.format("Driver=%s;", nativeDriverName.get()));
        } else {
            connectionString.append("Driver={SQL Server Native Client 11.0};");
        }
        if (instance.isPresent()) {
            connectionString.append(String.format("Server=%s,%d\\%s;", server, port, instance.get()));
        } else {
            connectionString.append(String.format("Server=%s,%d;", server, port));
        }
        connectionString.append(String.format("Database=%s;", database));
        if (user.isPresent()) {
            connectionString.append(String.format("UID=%s;", user.get()));
        }
        if (password.isPresent()) {
            logger.info("connection string = " + connectionString + "PWD=********;");
            connectionString.append(String.format("PWD=%s;", password.get()));
        } else {
            logger.info("connection string = " + connectionString);
        }

        checkSQLResult("SQLDriverConnect", odbc.SQLDriverConnectW(
                odbcHandle,
                null,
                toWideChars(connectionString.toString()),
                ODBC.SQL_NTS,
                null,
                ODBC.SQL_NTS,
                null,
                ODBC.SQL_DRIVER_NOPROMPT));

        StringBuilder fullTableName = new StringBuilder();
        fullTableName.append("[");
        fullTableName.append(database);
        fullTableName.append("].");
        fullTableName.append(".[");
        fullTableName.append(table);
        fullTableName.append("]");
        checkBCPResult("bcp_init", client.bcp_initW(
                odbcHandle,
                toWideChars(fullTableName.toString()),
                null,
                null,
                NativeClient.DB_IN));
    }

    public int bindNull(int columnIndex) throws SQLException
    {
        Pointer pointer = prepareBuffer(columnIndex, 0);
        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                NativeClient.SQL_NULL_DATA,
                null,
                0,
                NativeClient.SQLCHARACTER,
                columnIndex));
        return (int)pointer.size();
    }

    public int bindValue(int columnIndex, String value) throws SQLException
    {
        ByteBuffer bytes = charset.encode(value);
        int size = bytes.remaining();
        Pointer pointer = prepareBuffer(columnIndex, size);
        pointer.put(0, bytes.array(), 0, size);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                size,
                null,
                0,
                NativeClient.SQLCHARACTER,
                columnIndex));
        return (int)pointer.size();
    }

    public int bindValue(int columnIndex, boolean value) throws SQLException
    {
        int size = 1;
        Pointer pointer = prepareBuffer(columnIndex, size);
        pointer.putByte(0, value ? (byte)1 : (byte)0);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                size,
                null,
                0,
                NativeClient.SQLBIT,
                columnIndex));
        return (int)pointer.size();
    }

    public int bindValue(int columnIndex, byte value) throws SQLException
    {
        int size = 1;
        Pointer pointer = prepareBuffer(columnIndex, size);
        pointer.putByte(0, value);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                size,
                null,
                0,
                NativeClient.SQLINT1,
                columnIndex));
        return (int)pointer.size();
    }

    public int bindValue(int columnIndex, short value) throws SQLException
    {
        int size = 2;
        Pointer pointer = prepareBuffer(columnIndex, size);
        pointer.putShort(0, value);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                size,
                null,
                0,
                NativeClient.SQLINT2,
                columnIndex));
        return (int)pointer.size();
    }

    public int bindValue(int columnIndex, int value) throws SQLException
    {
        int size = 4;
        Pointer pointer = prepareBuffer(columnIndex, size);
        pointer.putInt(0, value);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                size,
                null,
                0,
                NativeClient.SQLINT4,
                columnIndex));
        return (int)pointer.size();
    }

    public int bindValue(int columnIndex, long value) throws SQLException
    {
        int size = 8;
        Pointer pointer = prepareBuffer(columnIndex, size);
        pointer.putLongLong(0, value);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                size,
                null,
                0,
                NativeClient.SQLINT8,
                columnIndex));
        return (int)pointer.size();
    }

    public int bindValue(int columnIndex, float value) throws SQLException
    {
        int size = 4;
        Pointer pointer = prepareBuffer(columnIndex, size);
        pointer.putFloat(0, value);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                size,
                null,
                0,
                NativeClient.SQLFLT4,
                columnIndex));
        return (int)pointer.size();
    }

    public int bindValue(int columnIndex, double value) throws SQLException
    {
        int size = 8;
        Pointer pointer = prepareBuffer(columnIndex, size);
        pointer.putDouble(0, value);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                size,
                null,
                0,
                NativeClient.SQLFLT8,
                columnIndex));
        return (int)pointer.size();
    }

    private Pointer prepareBuffer(int columnIndex, int size)
    {
        Pointer pointer = boundPointers.get(columnIndex);
        if (pointer == null || pointer.size() < size) {
            Runtime runtime = Runtime.getSystemRuntime();
            pointer = Pointer.wrap(runtime, ByteBuffer.allocateDirect(size).order(runtime.byteOrder()));
            boundPointers.put(columnIndex, pointer);
        }
        return pointer;
    }

    public void sendRow() throws SQLException
    {
        checkBCPResult("bcp_sendrow", client.bcp_sendrow(odbcHandle));
    }

    public void commit(boolean done) throws SQLException
    {
        String operation;
        int result;
        if (done) {
            operation = "bcp_done";
            result = client.bcp_done(odbcHandle);
        } else {
            operation = "bcp_batch";
            result = client.bcp_batch(odbcHandle);
        }
        if (result < 0) {
            throwException(operation, NativeClient.FAIL);
        } else {
            if (result > 0) {
                logger.info(String.format("SQL Server Native Client : %,d rows have bean loaded.", result));
            }
        }

    }

    public void close()
    {
        if (odbcHandle != null) {
            odbc.SQLFreeHandle(ODBC.SQL_HANDLE_DBC, odbcHandle);
            odbcHandle = null;
        }
        if (envHandle != null) {
            odbc.SQLFreeHandle(ODBC.SQL_HANDLE_ENV, envHandle);
            envHandle = null;
        }
    }

    private Pointer createPointerPointer()
    {
        return new ArrayMemoryIO(Runtime.getSystemRuntime(), com.kenai.jffi.Type.POINTER.size());
    }

    private String toString(Pointer wcharPointer, int length)
    {
        byte[] bytes = new byte[length * 2];
        wcharPointer.get(0, bytes, 0, length * 2);
        CharBuffer chars = wideCharset.decode(ByteBuffer.wrap(bytes));
        return chars.toString();
    }

    private Pointer toWideChars(String s)
    {
        ByteBuffer bytes = wideCharset.encode(s);
        Pointer pointer = new ArrayMemoryIO(Runtime.getSystemRuntime(), bytes.remaining() + 2);
        pointer.put(0, bytes.array(), 0, bytes.remaining());
        pointer.putShort(bytes.remaining(), (short)0);
        return pointer;
    }

    private void checkSQLResult(String operation, short result) throws SQLException
    {
        switch (result) {
            case ODBC.SQL_SUCCESS:
                break;

            case ODBC.SQL_SUCCESS_WITH_INFO:
                StringBuilder sqlState = new StringBuilder();
                StringBuilder sqlMessage = new StringBuilder();
                if (getErrorMessage(sqlState, sqlMessage)) {
                    logger.info(String.format("SQL Server Native Client : %s : %s", operation, sqlMessage));
                }
                break;

            default:
                throwException(operation, result);
        }
    }

    private void checkBCPResult(String operation, short result) throws SQLException
    {
        switch (result) {
            case NativeClient.SUCCEED:
                break;

            default:
                throwException(operation, result);
        }
    }

    private void throwException(String operation, short result) throws SQLException
    {
        String message = String.format("SQL Server Native Client : %s failed : %d.", operation, result);

        if (odbcHandle != null) {
            StringBuilder sqlState = new StringBuilder();
            StringBuilder sqlMessage = new StringBuilder();
            if (getErrorMessage(sqlState, sqlMessage)) {
                message = String.format("SQL Server Native Client : %s failed (sql state = %s) : %s", operation, sqlState, sqlMessage);
            }
        }

        logger.error(message);
        throw new SQLException(message);
    }

    private boolean getErrorMessage(StringBuilder sqlState, StringBuilder sqlMessage)
    {
        final int sqlStateLength = 5;
        // (5 (SQL state length) + 1 (terminator length)) * 2 (wchar size)
        Pointer sqlStatePointer = new ArrayMemoryIO(Runtime.getSystemRuntime(), (sqlStateLength + 1) * 2);
        Pointer sqlMessagePointer = new ArrayMemoryIO(Runtime.getSystemRuntime(), 512);
        Pointer lengthPointer = new ArrayMemoryIO(Runtime.getSystemRuntime(), 4);

        for (short record = 1;; record++) {
            short result = odbc.SQLGetDiagRecW(
                    ODBC.SQL_HANDLE_DBC,
                    odbcHandle,
                    record,
                    sqlStatePointer,
                    null,
                    sqlMessagePointer,
                    (short)(sqlMessagePointer.size() / 2),
                    lengthPointer);

            if (result == ODBC.SQL_SUCCESS) {
                if (record > 1) {
                    sqlState.append(",");
                }
                sqlState.append(toString(sqlStatePointer, sqlStateLength));
                sqlMessage.append(toString(sqlMessagePointer, lengthPointer.getInt(0)));
            } else {
                if (record == 1) {
                    return false;
                }
                break;
            }
        }

        return true;
    }

}
