package org.embulk.output.sqlserver.nativeclient;

import jnr.ffi.Pointer;


/**
 * sqlncli11.dll also has SQLXXX methods, but we should call those methods in odbc32.dll .
 */
public interface ODBC
{
    static short SQL_SUCCESS = 0;
    static short SQL_SUCCESS_WITH_INFO = 1;

    static short SQL_HANDLE_ENV = 1;
    static short SQL_HANDLE_DBC = 2;

    static short SQL_ATTR_ODBC_VERSION = 200;

    static short SQL_COPT_SS_BASE = 1200;
    static short SQL_COPT_SS_BCP = SQL_COPT_SS_BASE + 19;

    static short SQL_DRIVER_NOPROMPT = 0;

    static long SQL_OV_ODBC3 = 3;
    static long SQL_BCP_ON = 1;

    static int SQL_IS_INTEGER = -6;
    static short SQL_NTS = -3;


    short SQLAllocHandle(short handleType, Pointer inputHandle, Pointer outputHandle);

    short SQLSetEnvAttr(Pointer environmentHandle, short attribute, Pointer value, int stringLength);

    short SQLSetConnectAttrW(Pointer hdbc, int fAttribute, Pointer rgbValue, int cbValue);

    short SQLDriverConnectW(Pointer hdbc, Pointer hwnd,
            Pointer szConnStrIn, short cchConnStrIn,
            Pointer szConnStrOut, short cchConnStrOutMax, Pointer pcchConnStrOut,
            short fDriverCompletion);

    short SQLFreeHandle(short handleType, Pointer handle);

    short SQLGetDiagRecW(short handleType, Pointer handle, short recNumber,
            Pointer sqlState, Pointer nativeErrorPtr,
            Pointer messageText, short bufferLength, Pointer textLengthPtr);
}
