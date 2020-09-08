set LOG=%~dp0test-output.log
echo log file = %LOG%
del %LOG%

cd mysql-output
echo "mysql-output/test.bat"
call test.bat >> %LOG%
echo "mysql-output/test_replace.bat"
call test_replace.bat >> %LOG%
echo "mysql-output/test_merge.bat"
call test_merge.bat >> %LOG%
echo "mysql-output/test_temp_database.bat"
call test_temp_database.bat >> %LOG%
echo "mysql-output/test_temp_database-replace.bat"
call test_temp_database_replace.bat >> %LOG%
cd ..

cd oracle-output
echo "oracle-output/test.bat"
call test.bat >> %LOG%
echo "oracle-output/test-direct.bat"
call test-direct.bat >> %LOG%
echo "oracle-output/test-oci.bat"
call test-oci.bat >> %LOG%
echo "oracle-output/test-oci-n.bat"
call test-oci-n.bat >> %LOG%
echo "oracle-output/test-case.bat"
call test-case.bat >> %LOG%
echo "oracle-output/test-oci-case.bat"
call test-oci-case.bat >> %LOG%
echo "oracle-output/test-schema.bat"
call test-schema.bat >> %LOG%
echo "oracle-output/test-schema-oci.bat"
call test-schema-oci.bat >> %LOG%
echo "oracle-output/test-schema-oci-direct.bat"
call test-schema-oci-direct.bat >> %LOG%
echo "oracle-output/test-temp-schema.bat"
call test-temp-schema.bat >> %LOG%
echo "oracle-output/test-temp-schema-replace.bat"
call test-temp-schema-replace.bat >> %LOG%
cd ..

cd postgresql-output
echo "postgresql-output/test.bat"
call test.bat >> %LOG%
echo "postgresql-output/test-replace.bat"
call test-replace.bat >> %LOG%
echo "postgresql-output/test_merge.bat"
call test_merge.bat >> %LOG%
echo "postgresql-output/test-temp-schema.bat"
call test-temp-schema.bat >> %LOG%
echo "postgresql-output/test-temp-schema-replace.bat"
call test-temp-schema-replace.bat >> %LOG%
cd ..

cd sqlserver-output
echo "sqlserver-output/test.bat"
call test.bat >> %LOG%
echo "sqlserver-output/test-jtds.bat"
call test-jtds.bat >> %LOG%
echo "sqlserver-output/test-native.bat"
call test-native.bat >> %LOG%
echo "sqlserver-output/test-temp-schema.bat"
call test-temp-schema.bat >> %LOG%
echo "sqlserver-output/test-replace-temp-schema.bat"
call test-replace-temp-schema.bat >> %LOG%
cd ..

cd db2-output
echo "db2-output/test.bat"
call test.bat >> %LOG%
cd ..

grep "FAILED" %LOG%

IF "%ERRORLEVEL%" == "0" (ECHO "FAILED!") ELSE (ECHO "OK!")
