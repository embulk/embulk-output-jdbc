#include <string.h>
#include <stdio.h>
#include <malloc.h>
#include "dir-path-load.h"

#pragma warning (disable: 4996)


static int check(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context, const char* message, sword result)
{
	strcpy(context->message, "");

	if (result == OCI_ERROR) {
		sprintf(context->message, "OCI : %s failed.", message);
		sb4 errCode;
		OraText text[512];
		if (OCIErrorGet(context->err, 1, NULL, &errCode, text, sizeof(text) / sizeof(OraText), OCI_HTYPE_ERROR) != OCI_SUCCESS) {
			strcat(context->message, " OCIErrorGet failed.");
		} else {
			strcat(context->message, " ");
			strncat(context->message, (const char*)text, sizeof(context->message) - strlen(context->message) - 1);
		}
		return OCI_ERROR;
	}

	if (result == OCI_INVALID_HANDLE) {
		sprintf(context->message, "OCI : %s failed : invalid handle.", message);
		return OCI_ERROR;
	}

	if (result == OCI_NO_DATA) {
		sprintf(context->message, "OCI : %s failed : no data.", message);
		return OCI_ERROR;
	}

	if (result != OCI_SUCCESS) {
		sprintf(context->message, "OCI : %s failed : %d.", message, result);
		return OCI_ERROR;
	}

	return OCI_SUCCESS;
}

void embulk_output_oracle_freeDirPathHandles(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context)
{
	if (context->csv != NULL) fclose(context->csv);
	if (context->buffer != NULL) free(context->buffer);
	if (context->dpstr != NULL) OCIHandleFree(context->dpstr, OCI_HTYPE_DIRPATH_STREAM);
	if (context->dpca != NULL) OCIHandleFree(context->dpca, OCI_HTYPE_DIRPATH_COLUMN_ARRAY);
	if (context->dp != NULL) OCIHandleFree(context->dp, OCI_HTYPE_DIRPATH_CTX);
	if (context->svc != NULL) OCIHandleFree(context->svc, OCI_HTYPE_SVCCTX);
	if (context->err != NULL) OCIHandleFree(context->err, OCI_HTYPE_ERROR);
	if (context->env != NULL) OCIHandleFree(context->env, OCI_HTYPE_ENV);
}

int embulk_output_oracle_prepareDirPathCtx(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context, const char *dbName, const char *userName, const char *password)
{
	if (check(context, "OCIEnvCreate", OCIEnvCreate(&context->env, 
		OCI_THREADED|OCI_OBJECT,
		(void *)0,
		0, 
		0, 
		0,
		(size_t)0, 
		(void **)0))) {
		return OCI_ERROR;
	}

	// error handle
	if (check(context, "OCIHandleAlloc(OCI_HTYPE_ERROR)", OCIHandleAlloc(
		context->env, 
		(void **)&context->err,
        OCI_HTYPE_ERROR,
		(size_t)0,
		(void **)0))) {
		return OCI_ERROR;
	}

	// service context
	if (check(context, "OCIHandleAlloc(OCI_HTYPE_SVCCTX)", OCIHandleAlloc(
		context->env, 
		(void **)&context->svc,
        OCI_HTYPE_SVCCTX,
		(size_t)0,
		(void **)0))) {
		return OCI_ERROR;
	}

	// logon
	if (check(context, "OCILogon", OCILogon(context->env,
		context->err,
		&context->svc,
		(const OraText*)userName,
		(ub4)strlen(userName),
		(const OraText*)password,
		(ub4)strlen(password),
		(const OraText*)dbName, // dbName should be defined in 'tnsnames.ora' or a form of "host:port/db"
		(ub4)strlen(dbName)))) {
		return OCI_ERROR;
	}

	// direct path context
	if (check(context, "OCIHandleAlloc(OCI_HTYPE_DIRPATH_CTX)", OCIHandleAlloc(
		context->env, 
		(void **)&context->dp,
        OCI_HTYPE_DIRPATH_CTX,
		(size_t)0,
		(void **)0))) {
		return OCI_ERROR;
	}

	return OCI_SUCCESS;
}

static int isValid(EMBULK_OUTPUT_ORACLE_OCI_COL_DEF &colDef) {
	return colDef.type != 0;
}

int embulk_output_oracle_prepareDirPathStream(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context, const char *tableName, EMBULK_OUTPUT_ORACLE_OCI_COL_DEF *colDefs) {
	// load table name
	if (check(context, "OCIAttrSet(OCI_ATTR_NAME)", OCIAttrSet(context->dp, OCI_HTYPE_DIRPATH_CTX, (void*)tableName, (ub4)strlen(tableName), OCI_ATTR_NAME, context->err))) {
		return OCI_ERROR;
	}

	ub2 cols;
	for (cols = 0; isValid(colDefs[cols]); cols++) ;
	if (check(context, "OCIAttrSet(OCI_ATTR_NUM_COLS)", OCIAttrSet(context->dp, OCI_HTYPE_DIRPATH_CTX, &cols, sizeof(ub2), OCI_ATTR_NUM_COLS, context->err))) {
		return OCI_ERROR;
	}

	OCIParam *columns;
	if (check(context, "OCIAttrGet(OCI_ATTR_LIST_COLUMNS)", OCIAttrGet(context->dp, OCI_HTYPE_DIRPATH_CTX, &columns, (ub4*)0, OCI_ATTR_LIST_COLUMNS, context->err))) {
		return OCI_ERROR;
	}

	for (int i = 0; i < cols; i++) {
		EMBULK_OUTPUT_ORACLE_OCI_COL_DEF &colDef = colDefs[i];
		OCIParam *column;
		if (check(context, "OCIParamGet(OCI_DTYPE_PARAM)", OCIParamGet(columns, OCI_DTYPE_PARAM, context->err, (void**)&column, i + 1))) {
			return OCI_ERROR;
		}
		if (check(context, "OCIAttrSet(OCI_ATTR_NAME)", OCIAttrSet(column, OCI_DTYPE_PARAM, (void*)colDef.name, (ub4)strlen(colDef.name), OCI_ATTR_NAME, context->err))) {
			return OCI_ERROR;
		}
		if (check(context, "OCIAttrSet(OCI_ATTR_DATA_TYPE)", OCIAttrSet(column, OCI_DTYPE_PARAM, &colDef.type, sizeof(ub4), OCI_ATTR_DATA_TYPE, context->err))) {
			return OCI_ERROR;
		}
		if (check(context, "OCIAttrSet(OCI_ATTR_DATA_SIZE)", OCIAttrSet(column, OCI_DTYPE_PARAM, &colDef.size, sizeof(ub4), OCI_ATTR_DATA_SIZE, context->err))) {
			return OCI_ERROR;
		}
		// need to set charset explicitly because database charset is not set by default.
		if (check(context, "OCIAttrSet(OCI_ATTR_CHARSET_ID)", OCIAttrSet(column, OCI_DTYPE_PARAM, &colDef.charsetId, sizeof(ub2), OCI_ATTR_CHARSET_ID, context->err))) {
			return OCI_ERROR;
		}
		/*
		if (check(context, "OCIAttrSet(OCI_ATTR_PRECISION)", OCIAttrSet(column, OCI_DTYPE_PARAM, &colDefs[i].precision, sizeof(ub4), OCI_ATTR_PRECISION, context->err))) {
			return OCI_ERROR;
		}
		if (check(context, "OCIAttrSet(OCI_ATTR_SCALE)", OCIAttrSet(column, OCI_DTYPE_PARAM, &colDefs[i].scale, sizeof(ub4), OCI_ATTR_SCALE, context->err))) {
			return OCI_ERROR;
		}
		*/
		if (colDef.dateFormat != NULL) {
			if (check(context, "OCIAttrSet(OCI_ATTR_DATEFORMAT)", OCIAttrSet(column, OCI_DTYPE_PARAM, (void*)colDef.dateFormat, (ub4)strlen(colDef.dateFormat), OCI_ATTR_DATEFORMAT, context->err))) {
				return OCI_ERROR;
			}
		}

		if (check(context, "OCIDescriptorFree(OCI_DTYPE_PARAM)", OCIDescriptorFree(column, OCI_DTYPE_PARAM))) {
			return OCI_ERROR;
		}
	}

	if (check(context, "OCIDirPathPrepare", OCIDirPathPrepare(context->dp, context->svc, context->err))) {
		return OCI_ERROR;
	}

	// direct path column array
	if (check(context, "OCIHandleAlloc(OCI_HTYPE_DIRPATH_COLUMN_ARRAY)", OCIHandleAlloc(
		context->dp, 
		(void **)&context->dpca,
        OCI_HTYPE_DIRPATH_COLUMN_ARRAY,
		(size_t)0,
		(void **)0))) {
		return OCI_ERROR;
	}

	// direct path stream
	if (check(context, "OCIHandleAlloc(OCI_HTYPE_DIRPATH_STREAM)", OCIHandleAlloc(
		context->dp, 
		(void **)&context->dpstr,
        OCI_HTYPE_DIRPATH_STREAM,
		(size_t)0,
		(void **)0))) {
		return OCI_ERROR;
	}

	return OCI_SUCCESS;
}

static void intToSqlInt(int n, char* buffer)
{
	buffer[0] = n & 0xFF;
	buffer[1] = (n >> 8) & 0xFF;
	buffer[2] = (n >> 16) & 0xFF;
	buffer[3] = (n >> 24) & 0xFF;
}

static int strToSqlInt(const char *s, int size, char* buffer)
{
	int n = 0;
	for (int i = 0; i < size; i++) {
		if (s[i] < '0' || s[i] > '9') {
			return OCI_ERROR;
		}
		n = n * 10 + s[i] - '0';
	}

	intToSqlInt(n, buffer);

	return OCI_SUCCESS;
}

static int loadRows(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context, ub4 rowCount)
{
	for (ub4 offset = 0; offset < rowCount;) {
		if (check(context, "OCIDirPathStreamReset", OCIDirPathStreamReset(context->dpstr, context->err))) {
			return OCI_ERROR;
		}

		sword result = OCIDirPathColArrayToStream(context->dpca, context->dp, context->dpstr, context->err, rowCount, offset);
		if (result != OCI_SUCCESS && result != OCI_CONTINUE) {
			check(context, "OCIDirPathColArrayToStream", result);
			return OCI_ERROR;
		}

		if (check(context, "OCIDirPathLoadStream", OCIDirPathLoadStream(context->dp, context->dpstr, context->err))) {
			return OCI_ERROR;
		}

		if (result == OCI_SUCCESS) {
			offset = rowCount;
		} else {
			ub4 temp;
			if (check(context, "OCIAttrGet(OCI_ATTR_ROW_COUNT)", OCIAttrGet(context->dpca, OCI_HTYPE_DIRPATH_COLUMN_ARRAY, &temp, 0, OCI_ATTR_ROW_COUNT, context->err))) {
				return OCI_ERROR;
			}
			offset += temp;
		}
	}

	return OCI_SUCCESS;
}


int embulk_output_oracle_loadBuffer(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context, EMBULK_OUTPUT_ORACLE_OCI_COL_DEF *colDefs, const char *buffer, int rowCount)
{
	ub4 maxRowCount = 0;
	if (check(context, "OCIAttrGet(OCI_ATTR_NUM_ROWS)", OCIAttrGet(context->dpca, OCI_HTYPE_DIRPATH_COLUMN_ARRAY, &maxRowCount, 0, OCI_ATTR_NUM_ROWS, context->err))) {
		return OCI_ERROR;
	}

	int rowSize = 0;
	for (int col = 0; isValid(colDefs[col]); col++) {
		rowSize += colDefs[col].size;
	}
	const char *current = buffer;

	int colArrayRowCount = 0;
	for (int row = 0; row < rowCount; row++) {
		for (int col = 0; isValid(colDefs[col]); col++) {
			ub4 size = colDefs[col].size;
			if (colDefs[col].type == SQLT_CHR) {
				size = (ub4)strnlen(current, size);
			}

			if (check(context, "OCIDirPathColArrayEntrySet", OCIDirPathColArrayEntrySet(context->dpca, context->err, colArrayRowCount, col, (ub1*)current, size, OCI_DIRPATH_COL_COMPLETE))) {
				return OCI_ERROR;
			}
			current += colDefs[col].size;
		}

		colArrayRowCount++;
		if (colArrayRowCount == maxRowCount) {
			if (loadRows(context, colArrayRowCount)) {
				return OCI_ERROR;
			}

			colArrayRowCount = 0;
		}
	}

	if (colArrayRowCount > 0) {
		if (loadRows(context, colArrayRowCount)) {
			return OCI_ERROR;
		}
	}

	return OCI_SUCCESS;
}


int embulk_output_oracle_loadCSV(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context, EMBULK_OUTPUT_ORACLE_OCI_COL_DEF *colDefs, const char *csvFileName)
{
	printf("load csv file \"%s\".\r\n", csvFileName);
	if ((context->csv = fopen(csvFileName, "r")) == NULL) {
		printf("Cannot open file.");
		return OCI_ERROR;
	}

	ub4 maxRowCount = 0;
	if (check(context, "OCIAttrGet(OCI_ATTR_NUM_ROWS)", OCIAttrGet(context->dpca, OCI_HTYPE_DIRPATH_COLUMN_ARRAY, &maxRowCount, 0, OCI_ATTR_NUM_ROWS, context->err))) {
		return OCI_ERROR;
	}

	int rowSize = 0;
	for (int i = 0; isValid(colDefs[i]); i++) {
		rowSize += colDefs[i].size;
	}

	// + 1 for '\0'
	if ((context->buffer = (char*)malloc(rowSize * maxRowCount + 1)) == NULL) {
		printf("Cannot alloc memory.");
		return OCI_ERROR;
	}
	char *current = context->buffer;

	// TODO: support a line over 1,000 bytes
	char line[1000];
	int row = 0;
	while (fgets(line, sizeof(line), context->csv) != NULL) {
		size_t len = strlen(line);
		int col = 0;
		for (const char *p = line; p < line + len;) {
			const char *comma = strchr(p, ',');
			const char *next;
			ub4 size;
			if (comma != NULL) {
				size = (ub4)(comma - p);
				next = comma + 1;
			} else {
				size = (ub4)(line + len - p);
				if (size > 0 && p[size - 1] == '\n') size--;
				if (size > 0 && p[size - 1] == '\r') size--;
				next = line + len;
			}

			if (colDefs[col].type == SQLT_INT) {
				if (strToSqlInt(p, size, current)) {
					printf("Not a number : \"%s\"\r\n", p);
					return OCI_ERROR;
				}
				size = colDefs[col].size;
			} else if (colDefs[col].type == SQLT_CHR) {
				strncpy(current, p, size);
			} else {
				printf("Unsupported type : %d\r\n", colDefs[col].type);
				return OCI_ERROR;
			}

			if (check(context, "OCIDirPathColArrayEntrySet", OCIDirPathColArrayEntrySet(context->dpca, context->err, row, col, (ub1*)current, size, OCI_DIRPATH_COL_COMPLETE))) {
				return OCI_ERROR;
			}

			p = next;
			current += size;
			col++;
		}

		row++;
		if (row == maxRowCount) {
			printf("Load %d rows.\r\n", row);
			if (loadRows(context, row)) {
				return OCI_ERROR;
			}

			current = context->buffer;
			row = 0;
		}
	}

	if (row > 0) {
		printf("Load %d rows.\r\n", row);
		if (loadRows(context, row)) {
			return OCI_ERROR;
		}
	}

	free(context->buffer);
	context->buffer = NULL;

	fclose(context->csv);
	context->csv = NULL;

	return OCI_SUCCESS;
}

int embulk_output_oracle_commitDirPath(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context) 
{
	if (check(context, "OCIDirPathFinish", OCIDirPathFinish(context->dp, context->err))) {
		return OCI_ERROR;
	}

	if (check(context, "OCILogoff", OCILogoff(context->svc, context->err))) {
		return OCI_ERROR;
	}

	return OCI_SUCCESS;
}

int embulk_output_oracle_rollbackDirPath(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context) 
{
	if (check(context, "OCIDirPathAbort", OCIDirPathAbort(context->dp, context->err))) {
		return OCI_ERROR;
	}

	if (check(context, "OCILogoff", OCILogoff(context->svc, context->err))) {
		return OCI_ERROR;
	}

	return OCI_SUCCESS;
}
