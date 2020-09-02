sqlplus TEST_USER/XXXXXXXX@localhost/TESTDB @test.sql

CALL embulk run test-temp-schema.yml

sqlplus TEST_USER/XXXXXXXX@localhost/TESTDB @select.sql

sed -e "s/ \+/ /g" data/temp.txt > data/temp_trimmed.txt
echo "diff data/test_expected.txt data/temp_trimmed.txt"
diff data/test_expected.txt data/temp_trimmed.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-output-oracle test-temp-schema FAILED!")
