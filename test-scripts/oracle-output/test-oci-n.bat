SET NLS_LANG=JAPANESE_JAPAN.JA16SJISTILDE
sqlplus TEST_USER/XXXXXXXX@localhost/TESTDB @test-n.sql

CALL embulk run test-oci-n.yml

sqlplus TEST_USER/XXXXXXXX@localhost/TESTDB @select.sql

echo "diff data/test_expected-n.txt data/temp.txt"
diff data/test_expected-n.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-output-oracle test-oci-n FAILED!")
