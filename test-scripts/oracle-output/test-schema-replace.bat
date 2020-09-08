sqlplus TEST_USER/XXXXXXXX@localhost/TESTDB @test-schema.sql

CALL embulk run test-schema-replace.yml

sqlplus TEST_USER/XXXXXXXX@localhost/TESTDB @select-schema.sql

echo "diff data/test-replace_expected.txt data/temp.txt"
diff data/test-replace_expected.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-output-oracle test-schema-replace FAILED!")
