drop table if exists test_number;

create table test_number (
    id char(4),
    bool_item bool,
    smallint_item smallint,
    int_item int,
    bigint_item bigint,
    real_item real,
    double_item double precision,
    money_item money,
    numeric_item numeric(8,3),
    primary key (id)
);

