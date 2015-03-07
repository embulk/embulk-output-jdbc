create database embulk default character set utf8;
grant all on embulk.* to embulk_user@"%" identified by 'embulk_pw';

create table test1 (
    id              char(4),
    varchar_item    varchar(20),
    integer_item    bigint,
    number_item     double,
    date_item       timestamp,
    timestamp_item  timestamp,
    primary key (id)
);
