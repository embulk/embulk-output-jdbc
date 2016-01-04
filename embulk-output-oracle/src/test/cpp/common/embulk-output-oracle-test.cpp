#include <stdio.h>
#include <string.h>
#include <dir-path-load.h>


static int test(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context, const char *db, const char *user, const char *pass, const char *csvFileName)
{
	if (embulk_output_oracle_prepareDirPathCtx(context, db, user, pass)) {
		return OCI_ERROR;
	}

	EMBULK_OUTPUT_ORACLE_OCI_COL_DEF colDefs[] = {
		{"ID", SQLT_INT, 4, 832},
		//{"ID", SQLT_CHR, 8, 832},
		{"NUM", SQLT_INT, 4, 832},
		//{"NUM", SQLT_CHR, 12, 832},
		{"VALUE1", SQLT_CHR, 60, 832},
		{"VALUE2", SQLT_CHR, 60, 832},
		{"VALUE3", SQLT_CHR, 60, 832},
		{"VALUE4", SQLT_CHR, 60, 832},
		{"VALUE5", SQLT_CHR, 60, 832},
		{"VALUE6", SQLT_CHR, 60, 832},
		{"VALUE7", SQLT_CHR, 60, 832},
		{"VALUE8", SQLT_CHR, 60, 832},
		{"VALUE9", SQLT_CHR, 60, 832},
		{"VALUE10", SQLT_CHR, 60, 832},
		{NULL, 0, 0, 832}
	};
	if (embulk_output_oracle_prepareDirPathStream(context, "EXAMPLE", colDefs)) {
		return OCI_ERROR;
	}

	if (embulk_output_oracle_loadCSV(context, colDefs, csvFileName)) {
		return OCI_ERROR;
	}

	if (embulk_output_oracle_commitDirPath(context)) {
		return OCI_ERROR;
	}

	return OCI_SUCCESS;
}


int main(int argc, char* argv[])
{
	sword major_version;
    sword minor_version; 
    sword update_num;
    sword patch_num;
    sword port_update_num;
	OCIClientVersion(&major_version, &minor_version, &update_num, &patch_num, &port_update_num);
	printf("OCI client version = %d.%d.%d.%d.%d\r\n", major_version, minor_version, update_num, patch_num, port_update_num);

	if (argc < 5) {
		printf("embulk-output-oracle-test <db> <user> <password> <csv file name>\r\n");
		return OCI_ERROR;
	}

	EMBULK_OUTPUT_ORACLE_OCI_CONTEXT context;
	memset(&context, 0, sizeof(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT));
	int result = test(&context, argv[1], argv[2], argv[3], argv[4]);
	if (result == OCI_ERROR) {
		printf("%s\r\n", context.message);
	}
	embulk_output_oracle_freeDirPathHandles(&context);
	return result;
}

