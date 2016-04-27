package org.embulk.output.oracle.oci;

import jnr.ffi.Pointer;
import jnr.ffi.types.size_t;
import jnr.ffi.types.u_int16_t;
import jnr.ffi.types.u_int32_t;
import jnr.ffi.types.u_int8_t;


public interface OCI
{
    static short OCI_SUCCESS = 0;
    static short OCI_ERROR = -1;
    static short OCI_INVALID_HANDLE = -2;
    static short OCI_NO_DATA = 100;
    static short OCI_CONTINUE = -24200;

    static short OCI_DEFAULT = 0;
    static int OCI_NTV_SYNTAX = 1;

    static int OCI_THREADED = 1;
    static int OCI_OBJECT = 2;

    static int OCI_HTYPE_ENV = 1;
    static int OCI_HTYPE_ERROR = 2;
    static int OCI_HTYPE_SVCCTX = 3;
    static int OCI_HTYPE_STMT = 4;
    static int OCI_HTYPE_DEFINE = 6;
    static int OCI_HTYPE_DIRPATH_CTX = 14;
    static int OCI_HTYPE_DIRPATH_COLUMN_ARRAY = 15;
    static int OCI_HTYPE_DIRPATH_STREAM = 16;

    static int OCI_ATTR_DATA_SIZE = 1;
    static int OCI_ATTR_DATA_TYPE = 2;
    static int OCI_ATTR_NAME = 4;
    static int OCI_ATTR_ROW_COUNT = 9;
    static int OCI_ATTR_SCHEMA_NAME = 9;
    static int OCI_ATTR_CHARSET_ID = 31;
    static int OCI_ATTR_DATEFORMAT = 75;
    static int OCI_ATTR_NUM_ROWS = 81;
    static int OCI_ATTR_NUM_COLS = 102;
    static int OCI_ATTR_LIST_COLUMNS = 103;

    static int OCI_DTYPE_PARAM = 53;

    static byte OCI_DIRPATH_COL_COMPLETE = 0;

    static short SQLT_CHR = 1;
    static short SQLT_INT = 3;

    short OCIErrorGet(Pointer hndlp,
            @u_int32_t int  recordno,
            String sqlstate,
            Pointer errcodep,
            Pointer bufp,
            @u_int32_t int bufsiz,
            @u_int32_t int type);

    short OCIEnvCreate(Pointer envp,
            @u_int32_t int mode,
            Pointer ctxp,
            Pointer malocfp,
            Pointer ralocfp,
            Pointer mfreefp,
            @size_t long xtramemSz,
            Pointer usrmempp);

    short OCIHandleAlloc(Pointer parenth,
            Pointer hndlpp,
            @u_int32_t int type,
            @size_t long xtramemSz,
            Pointer usrmempp);

    short OCIHandleFree(Pointer hndlpp,
            @u_int32_t int type);

    short OCILogon(Pointer envhp,
            Pointer errhp,
            Pointer svchp,
            String username,
            @u_int32_t int usernameLen,
            String password,
            @u_int32_t int passwordLen,
            String dbname,
            @u_int32_t int dbnameLen);

    short OCILogoff(Pointer svchp,
            Pointer errhp);

    short OCIAttrSet(Pointer trgthndlp,
            @u_int32_t int trghndltyp,
            Pointer attributep,
            @u_int32_t int size,
            @u_int32_t int attrtype,
            Pointer errhp);

    short OCIAttrGet(Pointer trgthndlp,
            @u_int32_t int trghndltyp,
            Pointer attributep,
            Pointer sizep,
            @u_int32_t int attrtype,
            Pointer errhp);

    short OCIParamGet(Pointer hndlp,
            @u_int32_t int htype,
            Pointer errhp,
            Pointer parmdpp,
            @u_int32_t int pos);

    short OCIDescriptorFree(Pointer descp, @u_int32_t int type);

    short OCIDirPathPrepare(Pointer dpctx, Pointer svchp, Pointer errhp);

    short OCIDirPathColArrayEntrySet(Pointer dpca,
            Pointer errhp,
            @u_int32_t int rownum,
            @u_int16_t short colIdx,
            Pointer cvalp,
            @u_int32_t int size,
            @u_int8_t byte cflg);

    short OCIDirPathStreamReset(Pointer dpstr, Pointer errhp);

    short OCIDirPathColArrayToStream(Pointer dpca,
            Pointer dpctx,
            Pointer dpstr,
            Pointer errhp,
            @u_int32_t int rowcnt,
            @u_int32_t int rowoff);

    short OCIDirPathLoadStream(Pointer dpctx, Pointer dpstr, Pointer errhp);

    short OCIDirPathFinish(Pointer dpctx, Pointer errhp);

    short OCIDirPathAbort(Pointer dpctx, Pointer errhp);

    short OCIStmtPrepare(Pointer stmtp,
            Pointer errhp,
            String stmt,
            @u_int32_t int stmtLen,
            @u_int32_t int languate,
            @u_int32_t int mode);

    short OCIStmtExecute(Pointer svchp,
            Pointer stmtp,
            Pointer errhp,
            @u_int32_t int iters,
            @u_int32_t int rowoff,
            Pointer snapIn,
            Pointer snapOut,
            @u_int32_t int mode);

    short OCIDefineByPos(Pointer stmtp,
            Pointer defnpp,
            Pointer errhp,
            @u_int32_t int position,
            Pointer valuep,
            int valueSz,
            @u_int16_t short dty,
            Pointer indp,
            Pointer rlenp,
            Pointer rcodep,
            @u_int32_t int mode);

    short OCIStmtFetch2(Pointer stmthp,
            Pointer errhp,
            @u_int32_t int nrows,
            @u_int16_t short orientation,
            int fetchOffset,
            @u_int32_t int mode);
 }
