package org.embulk.output.sqlserver.nativeclient;

import jnr.ffi.Pointer;

public interface NativeClient
{
    static int SQL_NULL_DATA = -1;
    static int SQLCHARACTER = 0x2F;
    static int SQLINT1 = 0x30;
    static int SQLBIT = 0x32;
    static int SQLINT2 = 0x34;
    static int SQLINT4 = 0x38;
    static int SQLFLT8 = 0x3E;
    static int SQLFLT4 = 0x3B;
    static int SQLINT8 = 0x7F;

    static short FAIL = 0;
    static short SUCCEED = 1;
    static int DB_IN = 1;

    short bcp_initW(Pointer hdbc, Pointer szTable, Pointer szDataFile, Pointer szErrorFile, int eDirection);

    short bcp_bind(Pointer hdbc,
            Pointer pData, int cbIndicator, int cbData,
            Pointer pTerm, int cbTerm,
            int eDataType, int idxServerCol);

    short bcp_sendrow(Pointer hdbc);

    int bcp_batch(Pointer hdbc);

    int bcp_done(Pointer hdbc);
}
