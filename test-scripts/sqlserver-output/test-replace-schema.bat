osql -d TESTDB -U TEST_USER -P XXXXXXXX -i test.sql

CALL embulk run test-replace-schema.yml

osql -d TESTDB -U TEST_USER -P XXXXXXXX -Q "SELECT * FROM test.EMBULK_OUTPUT" > data/temp.txt

echo "diff data/test-replace_expected.txt data/temp.txt"
diff data/test-replace_expected.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-output-sqlserver FAILED!")