sqlplus TEST_USER/XXXXXXXX@localhost/TESTDB @test-merge.sql
sqlplus TEST_USER/XXXXXXXX@localhost/TESTDB @test-merge-data.sql

CALL embulk run test-merge1.yml

sqlplus TEST_USER/XXXXXXXX@localhost/TESTDB @select.sql

echo "diff data/test-merge1_expected.txt data/temp.txt"
diff data/test-merge1_expected.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-output-oracle test-merge1 FAILED!")
