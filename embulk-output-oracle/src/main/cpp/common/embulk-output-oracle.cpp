#include "org_embulk_output_oracle_oci_OCI.h"
#include "dir-path-load.h"


static OCI_CONTEXT *toContext(JNIEnv *env, jbyteArray addrs)
{
	OCI_CONTEXT *context;
	env->GetByteArrayRegion(addrs, 0, sizeof(context), (jbyte*)&context);
	return context;
}

static OCI_COL_DEF *toColDefs(JNIEnv *env, jbyteArray addrs)
{
	OCI_COL_DEF *colDefs;
	env->GetByteArrayRegion(addrs, sizeof(OCI_CONTEXT*), sizeof(colDefs), (jbyte*)&colDefs);
	return colDefs;
}

JNIEXPORT jbyteArray JNICALL Java_org_embulk_output_oracle_oci_OCI_createContext
  (JNIEnv *env, jobject)
{
	OCI_CONTEXT *context = new OCI_CONTEXT();
	jbyteArray addrs = env->NewByteArray(sizeof(OCI_CONTEXT*) + sizeof(OCI_COL_DEF*));
	env->SetByteArrayRegion(addrs, 0, sizeof(OCI_CONTEXT*), (jbyte*)&context);
	return addrs;
}


JNIEXPORT jbyteArray JNICALL Java_org_embulk_output_oracle_oci_OCI_getLasetMessage
  (JNIEnv *env, jobject, jbyteArray addrs)
{
	OCI_CONTEXT *context = toContext(env, addrs);
	jbyteArray message = env->NewByteArray(sizeof(context->message));
	env->SetByteArrayRegion(message, 0, sizeof(context->message), (jbyte*)context->message);
	return message;
}


JNIEXPORT jboolean JNICALL Java_org_embulk_output_oracle_oci_OCI_open
  (JNIEnv *env, jobject, jbyteArray addrs, jstring dbNameString, jstring userNameString, jstring passwordString)
{
	OCI_CONTEXT *context = toContext(env, addrs);

	const char *dbName = env->GetStringUTFChars(dbNameString, NULL);
	const char *userName = env->GetStringUTFChars(userNameString, NULL);
	const char *password = env->GetStringUTFChars(passwordString, NULL);

	int result = prepareDirPathCtx(context, dbName, userName, password);

	env->ReleaseStringUTFChars(dbNameString, dbName);
	env->ReleaseStringUTFChars(userNameString, userName);
	env->ReleaseStringUTFChars(passwordString, password);

	if (result != OCI_SUCCESS) {
		return JNI_FALSE;
	}

	return JNI_TRUE;
}


JNIEXPORT jboolean JNICALL Java_org_embulk_output_oracle_oci_OCI_prepareLoad
  (JNIEnv *env, jobject, jbyteArray addrs, jobject table)
{
	OCI_CONTEXT *context = toContext(env, addrs);
	
	jclass tableClass = env->FindClass("Lorg/embulk/output/oracle/oci/TableDefinition;");
	jfieldID tableNameFieldID = env->GetFieldID(tableClass, "tableName", "Ljava/lang/String;");
	jstring tableNameString = (jstring)env->GetObjectField(table, tableNameFieldID);
	const char *tableName = env->GetStringUTFChars(tableNameString, NULL);
	
	jfieldID charsetIdFieldID = env->GetFieldID(tableClass, "charsetId", "S");
	short charsetId = env->GetShortField(table, charsetIdFieldID);

	jfieldID columnsFieldID = env->GetFieldID(tableClass, "columns", "[Lorg/embulk/output/oracle/oci/ColumnDefinition;");
	jobjectArray columnArray = (jobjectArray)env->GetObjectField(table, columnsFieldID);
	int columnCount = env->GetArrayLength(columnArray);

	jclass columnClass = env->FindClass("Lorg/embulk/output/oracle/oci/ColumnDefinition;");
	jfieldID columnNameFieldID = env->GetFieldID(columnClass, "columnName", "Ljava/lang/String;");
	jfieldID columnTypeFieldID = env->GetFieldID(columnClass, "columnType", "I");
	jfieldID columnSizeFieldID = env->GetFieldID(columnClass, "columnSize", "I");
	jfieldID columnDateFormatID = env->GetFieldID(columnClass, "columnDateFormat", "Ljava/lang/String;");

	OCI_COL_DEF *colDefs = new OCI_COL_DEF[columnCount + 1];
	for (int i = 0; i < columnCount; i++) {
		OCI_COL_DEF &colDef = colDefs[i];

		jobject column = env->GetObjectArrayElement(columnArray, i);
		jstring columnName = (jstring)env->GetObjectField(column, columnNameFieldID);
		colDefs[i].name = env->GetStringUTFChars(columnName, NULL);
		colDefs[i].type = env->GetIntField(column, columnTypeFieldID);
		colDefs[i].size = env->GetIntField(column, columnSizeFieldID);

		jstring columnDateFormat = (jstring)env->GetObjectField(column, columnDateFormatID);
		if (columnDateFormat != NULL) {
			colDef.dateFormat = env->GetStringUTFChars(columnDateFormat, NULL);
		} else {
			colDef.dateFormat = NULL;
		}

	}

	colDefs[columnCount].name = NULL;
	colDefs[columnCount].type = 0;
	colDefs[columnCount].size = 0;
	colDefs[columnCount].dateFormat = NULL;

	int result = prepareDirPathStream(context, tableName, charsetId, colDefs);

	for (int i = 0; i < columnCount; i++) {
		OCI_COL_DEF &colDef = colDefs[i];
		jobject column = env->GetObjectArrayElement(columnArray, i);

		jstring columnName = (jstring)env->GetObjectField(column, columnNameFieldID);
		env->ReleaseStringUTFChars(columnName, colDef.name);
		colDef.name = NULL;

		if (colDef.dateFormat != NULL) {
			jstring columnDateFormat = (jstring)env->GetObjectField(column, columnDateFormatID);
			env->ReleaseStringUTFChars(columnDateFormat, colDef.dateFormat);
			colDef.dateFormat = NULL;
		}
	}

	env->SetByteArrayRegion(addrs, sizeof(OCI_CONTEXT*), sizeof(colDefs), (jbyte*)&colDefs);

	env->ReleaseStringUTFChars(tableNameString, tableName);

	if (result != OCI_SUCCESS) {
		return JNI_FALSE;
	}

	return JNI_TRUE;
}


JNIEXPORT jboolean JNICALL Java_org_embulk_output_oracle_oci_OCI_loadBuffer
  (JNIEnv *env, jobject, jbyteArray addrs, jbyteArray buffer, jint rowCount)
{
	OCI_CONTEXT *context = toContext(env, addrs);
	OCI_COL_DEF *colDefs = toColDefs(env, addrs);

	jbyte *bytes = env->GetByteArrayElements(buffer, NULL);

	int result = loadBuffer(context, colDefs, (const char*)bytes, rowCount);

	env->ReleaseByteArrayElements(buffer, bytes, JNI_ABORT);

	if (result != OCI_SUCCESS) {
		return JNI_FALSE;
	}

	return JNI_TRUE;
}


JNIEXPORT jboolean JNICALL Java_org_embulk_output_oracle_oci_OCI_commit
  (JNIEnv *env, jobject, jbyteArray addrs)
{
	OCI_CONTEXT *context = toContext(env, addrs);
	if (commitDirPath(context) !=OCI_SUCCESS) {
		return JNI_FALSE;
	}

	return JNI_TRUE;
}


JNIEXPORT jboolean JNICALL Java_org_embulk_output_oracle_oci_OCI_rollback
  (JNIEnv *env, jobject, jbyteArray addrs)
{
	OCI_CONTEXT *context = toContext(env, addrs);
	if (rollbackDirPath(context) !=OCI_SUCCESS) {
		return JNI_FALSE;
	}

	return JNI_TRUE;
}


JNIEXPORT void JNICALL Java_org_embulk_output_oracle_oci_OCI_close
  (JNIEnv *env, jobject, jbyteArray addrs)
{
	OCI_CONTEXT *context = toContext(env, addrs);
	if (context != NULL) {
		freeDirPathHandles(context);
		delete context;
	}

	OCI_COL_DEF *colDefs = toColDefs(env, addrs);
	if (colDefs != NULL) {
		delete[] colDefs;
	}

}
