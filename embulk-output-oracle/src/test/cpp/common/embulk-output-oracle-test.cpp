#include <stdio.h>
#include <string.h>
#include <dir-path-load.h>


static int test(OCI_CONTEXT *context, const char *db, const char *user, const char *pass, const char *csvFileName)
{
	if (prepareDirPathCtx(context, db, user, pass)) {
		return OCI_ERROR;
	}

	COL_DEF colDefs[] = {
		{"ID", SQLT_INT, 4},
		//{"ID", SQLT_CHR, 8},
		{"NUM", SQLT_INT, 4},
		//{"NUM", SQLT_CHR, 12},
		{"VALUE1", SQLT_CHR, 60},
		{"VALUE2", SQLT_CHR, 60},
		{"VALUE3", SQLT_CHR, 60},
		{"VALUE4", SQLT_CHR, 60},
		{"VALUE5", SQLT_CHR, 60},
		{"VALUE6", SQLT_CHR, 60},
		{"VALUE7", SQLT_CHR, 60},
		{"VALUE8", SQLT_CHR, 60},
		{"VALUE9", SQLT_CHR, 60},
		{"VALUE10", SQLT_CHR, 60},
		{NULL, 0, 0}
	};
	if (prepareDirPathStream(context, "EXAMPLE", 832, colDefs)) {
		return OCI_ERROR;
	}

	if (loadCSV(context, colDefs, csvFileName)) {
		return OCI_ERROR;
	}

	if (commit(context)) {
		return OCI_ERROR;
	}

	return OCI_SUCCESS;
}


int main(int argc, char* argv[])
{
	if (argc < 5) {
		printf("embulk-output-oracle-test <db> <user> <password> <csv file name>\r\n");
		return OCI_ERROR;
	}

	OCI_CONTEXT context;
	memset(&context, 0, sizeof(OCI_CONTEXT));
	int result = test(&context, argv[1], argv[2], argv[3], argv[4]);
	if (result == OCI_ERROR) {
		printf("%s\r\n", context.message);
	}
	freeHandles(&context);
	return result;
}

