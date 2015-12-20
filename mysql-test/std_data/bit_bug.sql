create database if not exists test;
create table test.bit(b bit(1));
insert into test.bit values (0x00), (0x01), (0x00), (0x00);

