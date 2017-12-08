DROP TABLE TEST_TINYINT;
CREATE TABLE TEST_TINYINT (
    ID                  CHAR(4),
    VALUE               TINYINT,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_SMALLINT;
CREATE TABLE TEST_SMALLINT (
    ID                  CHAR(4),
    VALUE               SMALLINT,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_INT;
CREATE TABLE TEST_INT (
    ID                  CHAR(4),
    VALUE               INT,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_BIGINT;
CREATE TABLE TEST_BIGINT (
    ID                  CHAR(4),
    VALUE               BIGINT,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_BIT;
CREATE TABLE TEST_BIT (
    ID                  CHAR(4),
    VALUE               BIT,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_DECIMAL;
CREATE TABLE TEST_DECIMAL (
    ID                  CHAR(4),
    VALUE               DECIMAL(12,2),
    PRIMARY KEY (ID)
);

DROP TABLE TEST_NUMERIC;
CREATE TABLE TEST_NUMERIC (
    ID                  CHAR(4),
    VALUE               NUMERIC(5,3),
    PRIMARY KEY (ID)
);

DROP TABLE TEST_SMALLMONEY;
CREATE TABLE TEST_SMALLMONEY (
    ID                  CHAR(4),
    VALUE               SMALLMONEY,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_MONEY;
CREATE TABLE TEST_MONEY (
    ID                  CHAR(4),
    VALUE               MONEY,
    PRIMARY KEY (ID)
);

