drop table if exists test1;

create table test1 (
    id     int,
    num    decimal(12,2),
    str    char(8),
    varstr varchar(8),
    dttm3  datetime(3),
    primary key(id)
);

drop table if exists test_merge;
create table test_merge (
    id     int,
    value1 varchar(8),
    value2 varchar(8),
    primary key(id)
);

insert into test_merge values(11, 'A1', 'B1');
insert into test_merge values(12, 'A2', 'B2');
insert into test_merge values(13, 'A3', 'B3');
