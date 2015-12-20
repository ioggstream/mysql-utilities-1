create database a;
create database b;
create table a.t (i int not null);
create table b.t (i int not null);
create view a.v as (select * from b.t);
create view b.v as (select * from a.t);
