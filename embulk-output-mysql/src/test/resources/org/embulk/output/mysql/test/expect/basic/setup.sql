drop table if exists test1;

create table test1 (
    id     int,
    num    decimal(12,2),
    str    char(8),
    varstr varchar(8),
    dt     date,
    dttm0  datetime,
    dttm3  datetime(3),
    primary key(id)
);
