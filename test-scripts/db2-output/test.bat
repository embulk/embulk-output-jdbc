CALL db2setcp
CALL db2 "CONNECT TO TESTDB USER db2admin USING XXXXXXXX"
CALL db2 "DROP TABLE EMBULK_OUTPUT"
CALL db2 "CREATE TABLE EMBULK_OUTPUT (ID DECIMAL(4) NOT NULL, NUM DECIMAL(12,2), STR CHAR(8), VARSTR VARCHAR(8), DT DATE, DTTM0 TIMESTAMP, DTTM3 TIMESTAMP(3), PRIMARY KEY(ID))"

CALL embulk run test.yml

CALL db2 "SELECT * FROM EMBULK_OUTPUT ORDER BY ID" > data\temp.txt

echo "diff data/test_expected.txt data/temp.txt"
diff data/test_expected.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-output-db2 FAILED!")
