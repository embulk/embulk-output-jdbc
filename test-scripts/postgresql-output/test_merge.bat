rem merge (merge_keys not specified)

SETLOCAL
SET PGPASSWORD=XXXXXXXX
"C:\Program Files\PostgreSQL\9.4\bin\psql.exe" -d testdb -U test_user -w -f test_merge.sql
ENDLOCAL

CALL embulk run test_merge1.yml

SETLOCAL
SET PGPASSWORD=XXXXXXXX
"C:\Program Files\PostgreSQL\9.4\bin\psql.exe" -d testdb -U test_user -w -c "SELECT * FROM merge_test" > data/temp.txt
ENDLOCAL

echo "diff data/test_merge1_expected.txt data/temp.txt"
diff data/test_merge1_expected.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-output-postgresql test_merge#1 FAILED!")


rem merge (merge_keys specified)

SETLOCAL
SET PGPASSWORD=XXXXXXXX
"C:\Program Files\PostgreSQL\9.4\bin\psql.exe" -d testdb -U test_user -w -f test_merge.sql
ENDLOCAL

CALL embulk run test_merge2.yml

SETLOCAL
SET PGPASSWORD=XXXXXXXX
"C:\Program Files\PostgreSQL\9.4\bin\psql.exe" -d testdb -U test_user -w -c "SELECT * FROM merge_test" > data/temp.txt
ENDLOCAL

echo "diff data/test_merge2_expected.txt data/temp.txt"
diff data/test_merge2_expected.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-output-postgresql test_merge#2 FAILED!")


rem merge (merge_rule specified)

SETLOCAL
SET PGPASSWORD=XXXXXXXX
"C:\Program Files\PostgreSQL\9.4\bin\psql.exe" -d testdb -U test_user -w -f test_merge.sql
ENDLOCAL

CALL embulk run test_merge_rule.yml

SETLOCAL
SET PGPASSWORD=XXXXXXXX
"C:\Program Files\PostgreSQL\9.4\bin\psql.exe" -d testdb -U test_user -w -c "SELECT * FROM merge_test" > data/temp.txt
ENDLOCAL

echo "diff data/test_merge_rule_expected.txt data/temp.txt"
diff data/test_merge_rule_expected.txt data/temp.txt

IF "%ERRORLEVEL%" == "0" (ECHO "OK!") ELSE (ECHO "embulk-output-postgresql test_merge#3 FAILED!")

