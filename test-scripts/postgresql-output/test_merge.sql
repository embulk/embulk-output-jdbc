drop table if exists merge_test;
create table merge_test (
    id     int,
    value1 varchar(8),
    value2 varchar(8),
    primary key(id)
);

insert into merge_test values(11, 'A1', 'B1');
insert into merge_test values(12, 'A2', 'B2');
insert into merge_test values(13, 'A3', 'B3');
