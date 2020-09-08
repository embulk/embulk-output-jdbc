mysql -uTEST_USER -pXXXXXXXX -DTESTDB < test.sql

CALL embulk run test_temp_database_replace.yml

mysql -uTEST_USER -pXXXXXXXX -DTESTDB -e"SELECT * FROM EMBULK_OUTPUT" > data/temp.txt

echo "diff data/test_replace_expected.txt data/temp.txt"
diff data/test_replace_expected.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-output-mysql test_temp_database_replace FAILED!")
