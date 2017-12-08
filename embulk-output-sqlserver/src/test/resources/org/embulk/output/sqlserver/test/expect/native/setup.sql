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
    VALUE               DECIMAL(12,4),
    PRIMARY KEY (ID)
);

DROP TABLE TEST_NUMERIC;
CREATE TABLE TEST_NUMERIC (
    ID                  CHAR(4),
    VALUE               NUMERIC(12,4),
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

DROP TABLE TEST_BIT;
CREATE TABLE TEST_BIT (
    ID                  CHAR(4),
    VALUE               BIT,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_REAL;
CREATE TABLE TEST_REAL (
    ID                  CHAR(4),
    VALUE               REAL,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_FLOAT;
CREATE TABLE TEST_FLOAT (
    ID                  CHAR(4),
    VALUE               FLOAT,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_CHAR;
CREATE TABLE TEST_CHAR (
    ID                  INT,
    VALUE               CHAR(4),
    PRIMARY KEY (ID)
);

DROP TABLE TEST_VARCHAR;
CREATE TABLE TEST_VARCHAR (
    ID                  INT,
    VALUE               VARCHAR(4),
    PRIMARY KEY (ID)
);

DROP TABLE TEST_TEXT;
CREATE TABLE TEST_TEXT (
    ID                  INT,
    VALUE               TEXT,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_NCHAR;
CREATE TABLE TEST_NCHAR (
    ID                  INT,
    VALUE               NCHAR(4),
    PRIMARY KEY (ID)
);

DROP TABLE TEST_NVARCHAR;
CREATE TABLE TEST_NVARCHAR (
    ID                  INT,
    VALUE               NVARCHAR(4),
    PRIMARY KEY (ID)
);

DROP TABLE TEST_NTEXT;
CREATE TABLE TEST_NTEXT (
    ID                  INT,
    VALUE               NTEXT,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_DATE;
CREATE TABLE TEST_DATE (
    ID                  INT,
    VALUE               DATE,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_DATETIME;
CREATE TABLE TEST_DATETIME (
    ID                  INT,
    VALUE               DATETIME,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_DATETIME2;
CREATE TABLE TEST_DATETIME2 (
    ID                  INT,
    VALUE               DATETIME2,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_SMALLDATETIME;
CREATE TABLE TEST_SMALLDATETIME (
    ID                  INT,
    VALUE               SMALLDATETIME,
    PRIMARY KEY (ID)
);

DROP TABLE TEST_TIME;
CREATE TABLE TEST_TIME (
    ID                  INT,
    VALUE               TIME,
    PRIMARY KEY (ID)
);
