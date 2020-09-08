DROP TABLE IF EXISTS embulk_output;

CREATE TABLE embulk_output (
    id     int,
    num    decimal(12,2),
    str    char(8),
    varstr varchar(8),
    dt     date,
    dttm0  timestamp,
    dttm3  timestamp(3),
    primary key(ID)
);
