#include <oci.h>


typedef struct _EMBULK_OUTPUT_ORACLE_OCI_CONTEXT {
	OCIEnv     *env;
	OCIDirPathCtx *dp;
	OCISvcCtx *svc;
	OCIError *err;
	OCIDirPathColArray *dpca;
	OCIDirPathStream *dpstr;
	char *buffer;
	FILE *csv;
	char message[512];
} EMBULK_OUTPUT_ORACLE_OCI_CONTEXT;

typedef struct _EMBULK_OUTPUT_ORACLE_OCI_COL_DEF {
	const char *name;
	ub4 type;
	ub4 size;
	const char *dateFormat;
} EMBULK_OUTPUT_ORACLE_OCI_COL_DEF;


int embulk_output_oracle_prepareDirPathCtx(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context, const char *dbName, const char *userName, const char *password);

int embulk_output_oracle_prepareDirPathStream(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context, const char *tableName, short charsetId, EMBULK_OUTPUT_ORACLE_OCI_COL_DEF *colDefs);
	
int embulk_output_oracle_loadBuffer(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context, EMBULK_OUTPUT_ORACLE_OCI_COL_DEF *colDefs, const char *buffer, int rowCount);
	
int embulk_output_oracle_loadCSV(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context, EMBULK_OUTPUT_ORACLE_OCI_COL_DEF *colDefs, const char *csvFileName);

int embulk_output_oracle_commitDirPath(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context);
	
int embulk_output_oracle_rollbackDirPath(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context);
	
void embulk_output_oracle_freeDirPathHandles(EMBULK_OUTPUT_ORACLE_OCI_CONTEXT *context);
