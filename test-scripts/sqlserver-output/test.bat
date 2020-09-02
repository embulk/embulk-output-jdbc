osql -d TESTDB -U TEST_USER -P XXXXXXXX -i test.sql

CALL embulk run test.yml

osql -d TESTDB -U TEST_USER -P XXXXXXXX -Q "SELECT * FROM EMBULK_OUTPUT" > data/temp.txt

echo "diff data/test_expected.txt data/temp.txt"
diff data/test_expected.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-output-sqlserver FAILED!")