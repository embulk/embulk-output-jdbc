drop table if exists test_number;

create table test_number (
    id            char(4),
    bool_item     bool,
    smallint_item smallint,
    int_item      int,
    bigint_item   bigint,
    real_item     real,
    double_item   double precision,
    money_item    money,
    numeric_item  numeric(8,3),
    primary key (id)
);

drop table if exists test_string;

create table test_string (
    id            int,
    char_item     char(4),
    varchar_item  varchar(4),
    text_item     text,
    primary key (id)
);

drop table if exists test_timestamp;

create table test_timestamp (
    id             int,
    date_item      date,
    time_item      time,
    timestamp_item timestamp,
    primary key (id)
);

drop table if exists test_json;

create table test_json (
    id             int,
    json_item      json,
    jsonb_item     jsonb,
    primary key (id)
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

