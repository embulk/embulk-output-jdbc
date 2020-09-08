osql -d TESTDB -U TEST_USER -P XXXXXXXX -i test-merge.sql
osql -d TESTDB -U TEST_USER -P XXXXXXXX -i test-merge-data.sql

CALL embulk run test-merge1.yml

osql -d TESTDB -U TEST_USER -P XXXXXXXX -Q "SELECT * FROM EMBULK_OUTPUT" > data/temp.txt

echo "diff data/test-merge1_expected.txt data/temp.txt"
diff data/test-merge1_expected.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-output-sqlserver FAILED!")
