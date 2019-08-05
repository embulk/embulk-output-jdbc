DROP TABLE TEST1
CREATE TABLE TEST1 (
    ID                  CHAR(4),
    TINYINT_ITEM        TINYINT,
    SMALLINT_ITEM       SMALLINT,
    INT_ITEM            INT,
    BIGINT_ITEM         BIGINT,
    BIT_ITEM            BIT,
    DECIMAL_ITEM        DECIMAL(12,2),
    NUMERIC_ITEM        NUMERIC(5,3),
    SMALLMONEY_ITEM     SMALLMONEY,
    MONEY_ITEM          MONEY,
    REAL_ITEM           REAL,
    FLOAT_ITEM          FLOAT,
    CHAR_ITEM           CHAR(4),
    VARCHAR_ITEM        VARCHAR(8),
    TEXT_ITEM           TEXT,
    NCHAR_ITEM          NCHAR(4),
    NVARCHAR_ITEM       NVARCHAR(8),
    NTEXT_ITEM          NTEXT,
    DATE_ITEM           DATE,
    DATETIME_ITEM       DATETIME,
    DATETIME2_ITEM      DATETIME2,
    DATETIME2_2_ITEM    DATETIME2(2),
    SMALLDATETIME_ITEM  SMALLDATETIME,
    TIME_ITEM           TIME,
    TIME_2_ITEM         TIME(2),
    PRIMARY KEY (ID)
);
INSERT INTO TEST1(ID) VALUES('9999');

DROP TABLE TEST___Ａ１２３４５６７８９Ｂ１２３４５６７８９Ｃ１２３４５６７８９Ｄ１２３４５６７８９Ｅ１２３４５６７８９Ｆ１２３４５６７８９Ｇ１２３４５６７８９Ｈ１２３４５６７８９Ｉ１２３４５６７８９Ｊ１２３４５６７８９Ｋ１２３４５６７８９Ｌ１２３４５６７８９
CREATE TABLE TEST___Ａ１２３４５６７８９Ｂ１２３４５６７８９Ｃ１２３４５６７８９Ｄ１２３４５６７８９Ｅ１２３４５６７８９Ｆ１２３４５６７８９Ｇ１２３４５６７８９Ｈ１２３４５６７８９Ｉ１２３４５６７８９Ｊ１２３４５６７８９Ｋ１２３４５６７８９Ｌ１２３４５６７８９ (
    ID                  CHAR(4),
    TINYINT_ITEM        TINYINT,
    SMALLINT_ITEM       SMALLINT,
    INT_ITEM            INT,
    BIGINT_ITEM         BIGINT,
    BIT_ITEM            BIT,
    DECIMAL_ITEM        DECIMAL(12,2),
    NUMERIC_ITEM        NUMERIC(5,3),
    SMALLMONEY_ITEM     SMALLMONEY,
    MONEY_ITEM          MONEY,
    REAL_ITEM           REAL,
    FLOAT_ITEM          FLOAT,
    CHAR_ITEM           CHAR(4),
    VARCHAR_ITEM        VARCHAR(8),
    TEXT_ITEM           TEXT,
    NCHAR_ITEM          NCHAR(4),
    NVARCHAR_ITEM       NVARCHAR(8),
    NTEXT_ITEM          NTEXT,
    DATE_ITEM           DATE,
    DATETIME_ITEM       DATETIME,
    DATETIME2_ITEM      DATETIME2,
    DATETIME2_2_ITEM    DATETIME2(2),
    SMALLDATETIME_ITEM  SMALLDATETIME,
    TIME_ITEM           TIME,
    TIME_2_ITEM         TIME(2),
    PRIMARY KEY (ID)
);
INSERT INTO TEST___Ａ１２３４５６７８９Ｂ１２３４５６７８９Ｃ１２３４５６７８９Ｄ１２３４５６７８９Ｅ１２３４５６７８９Ｆ１２３４５６７８９Ｇ１２３４５６７８９Ｈ１２３４５６７８９Ｉ１２３４５６７８９Ｊ１２３４５６７８９Ｋ１２３４５６７８９Ｌ１２３４５６７８９(ID) VALUES('9999');

DROP TABLE TEST_MERGE1;
CREATE TABLE TEST_MERGE1 (
    ITEM1 INT,
    ITEM2 INT,
    ITEM3 VARCHAR(4),
    PRIMARY KEY(ITEM1, ITEM2)
);
INSERT INTO TEST_MERGE1 VALUES(10, 20, 'A');
INSERT INTO TEST_MERGE1 VALUES(10, 21, 'B');
INSERT INTO TEST_MERGE1 VALUES(11, 20, 'C');

DROP TABLE TEST_MERGE2;
CREATE TABLE TEST_MERGE2 (
    ITEM1 INT,
    ITEM2 INT,
    ITEM3 VARCHAR(4)
);
INSERT INTO TEST_MERGE2 VALUES(10, 20, 'A');
INSERT INTO TEST_MERGE2 VALUES(10, 21, 'B');
INSERT INTO TEST_MERGE2 VALUES(11, 20, 'C');

DROP TABLE TEST_MAX
CREATE TABLE TEST_MAX (
    ID INT,
    C1 VARCHAR(8000),
    C2 VARCHAR(MAX),
    C3 NVARCHAR(4000),
    C4 NVARCHAR(MAX),
    PRIMARY KEY (ID)
);