sqlplus TEST_USER/XXXXXXXX@localhost/TESTDB @test.sql

CALL embulk run test-replace.yml

sqlplus TEST_USER/XXXXXXXX@localhost/TESTDB @select.sql

echo "diff data/test-replace_expected.txt data/temp.txt"
diff data/test-replace_expected.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-output-oracle test-replace FAILED!")
