-------------------------------------
---------- hash index part1----------
-------------------------------------

set enable_seqscan = off;
set enable_indexscan = off;
------------------
-- hash_table_1 --
------------------
drop table if exists hash_table_1 cascade;
create table hash_table_1 (id int, name varchar, sex varchar default 'male');

insert into hash_table_1 values (1, 'Smith');
insert into hash_table_1 values (2, 'Jones');
insert into hash_table_1 values (3, 'Williams', 'female');
insert into hash_table_1 values (4, 'Taylor');
insert into hash_table_1 values (5, 'Brown');
insert into hash_table_1 values (6, 'Davies');

drop index if exists hash_t1_id1;
create index hash_t1_id1 on hash_table_1 using hash (id);
-- error, does not support multicolumn indexes
drop index if exists hash_t1_id2;
create index hash_t1_id2 on hash_table_1 using hash (id, sex);

-- compare with hash_t1_id1 and hash_t1_id3, hash index can be create in same column 
drop index if exists hash_t1_id3;
drop index if exists hash_t1_id4;
create index hash_t1_id3 on hash_table_1 using btree (id);
create index hash_t1_id4 on hash_table_1 using hash (id);

-- drop superfluous index now
drop index hash_t1_id3, hash_t1_id4;

-- insert into large volumns of data into hash_table_1
insert into hash_table_1 select 4, 'XXX', 'XXX' from generate_series(1,50000);
insert into hash_table_1 select 6, 'XXX', 'XXX' from generate_series(1,50000);
analyse hash_table_1;

-- after insert, hash_t1_id1 is still work
explain(costs off) select * from hash_table_1 where id = 4;
select count(*) from hash_table_1 where id = 6; --50001

-- do other dml action, then check hash_t1_id1 again
insert into hash_table_1 select random()*100, 'XXX', 'XXX' from generate_series(1,50000);
update hash_table_1 set id = 101, sex = 'male' where id = 60;
delete from hash_table_1 where id = 80;
explain(costs off) select * from hash_table_1 where id = 101;

-- cleanup env
drop table hash_table_1 cascade;

------------------
-- hash_table_2 --
------------------
drop table if exists hash_table_2 cascade;
create table hash_table_2 (id int, name varchar, sex varchar default 'male');
insert into hash_table_2 select random()*100, 'XXX', 'XXX' from generate_series(1,100000);

-- create index concurrently
-- In this fastcheck, we only check it can run properly. However, in a real 
-- situation, you should run this sql in connection a first, then doing some DML(
-- insert, delete, update) operation about this table in connection b as soon 
-- as possible. We expect the create index do not block DML operation.
-- connection a
create index concurrently hash_t2_id1 on hash_table_2 using hash (id);
-- connection b
insert into hash_table_2 select random()*100, 'XXX', 'XXX' from generate_series(1,100);
explain(costs off) select * from hash_table_2 where id = 40;

-- error, does not support unique indexes
create unique index hash_t2_id2 on hash_table_2 using hash (sex);

-- hash_t2_id3 occupies more disk space than hash_t2_id2
create index hash_t2_id2 on hash_table_2 using hash (id) with (fillfactor=25);
create index hash_t2_id3 on hash_table_2 using hash (id) with (fillfactor=75);

select count(*) from hash_table_2; --100100

-- cleanup env
drop table hash_table_2 cascade;

------------------
-- hash_table_3 --
------------------
drop schema if exists hash_sc_3 cascade;
drop tablespace if exists hash_sp_3;
create schema hash_sc_3;
create tablespace hash_sp_3 relative location 'tablespace/tablespace_1';
create table hash_sc_3.hash_table_3
(
    id int, name varchar, 
    sex varchar default 'male'
)
tablespace hash_sp_3;
-- create index specify schema and tablespace
create index concurrently hash_sc_3.hash_t3_id1 on hash_sc_3.hash_table_3 using hash (id);
create index hash_sc_3.hash_t3_id2 on hash_sc_3.hash_table_3 using hash (id) tablespace hash_sp_3;

drop table hash_sc_3.hash_table_3 cascade;
drop schema hash_sc_3 cascade;
drop tablespace hash_sp_3;

------------------
-- hash_table_4 --
------------------
drop table if exists hash_table_4 cascade;
create table hash_table_4
(
    id int, 
    name varchar, 
    sex varchar default 'male'
)
partition by range(id)
(
    partition p1 values less than (1000),
    partition p2 values less than (2000),
    partition p3 values less than (3000),
    partition p4 values less than (maxvalue)
);

-- hash index only support local index in partition table
drop index if exists hash_t4_id1;
drop index if exists hash_t4_id2;
drop index if exists hash_t4_id2_new;
create index hash_t4_id1 on hash_table_4 using hash(id) global;
create index hash_t4_id2 on hash_table_4 using hash(id) local
(
    partition index_t4_p1,
    partition index_t4_p2,
    partition index_t4_p3,
    partition index_t4_p4
);

-- alter index rename, unusable
insert into hash_table_4 select random()*5000, 'XXX', 'XXX' from generate_series(1,1000);
alter index hash_t4_id2 rename to hash_t4_id2_new;
alter index hash_t4_id2_new modify partition index_t4_p2 unusable;
reindex index hash_t4_id2_new partition index_t4_p2;

drop table hash_table_4 cascade;

------------------
-- hash_table_5 --
------------------
drop table if exists hash_table_5;
create temporary table hash_table_5(id int, name varchar, sex varchar default 'male');

drop index if exists hash_t5_id1;
create index hash_t5_id1 on hash_table_5 using hash(id) with(fillfactor = 80);

insert into hash_table_5 select random()*100, 'XXX', 'XXX' from generate_series(1,100);
update hash_table_5 set name = 'aaa' where id = 80;
alter index hash_t5_id1 set (fillfactor = 60);
alter index hash_t5_id1 reset (fillfactor);
explain (costs off) select * from hash_table_5 where id = 80;
drop table hash_table_5 cascade;

------------------
-- hash_table_6 --
------------------
drop table if exists hash_table_6;
create global temporary table hash_table_6(id int, name varchar, sex varchar default 'male');
drop index if exists hash_t6_id1;
create index hash_t6_id1 on hash_table_6 using hash((id*10)) with (fillfactor = 30);
insert into hash_table_6 select random()*100, 'XXX', 'XXX' from generate_series(1,1000);
delete from hash_table_6 where id in (50, 60, 70);
explain (costs off) select * from hash_table_6 where id*10 = 80;
drop table hash_table_6 cascade;

-- create unlogged table index, which will be delete in hash_index_002
drop table if exists hash_table_7;
create unlogged table hash_table_7(id int, name varchar, sex varchar default 'male');
insert into hash_table_7 select random()*100, 'XXX', 'XXX' from generate_series(1,1000);
create index hash_t7_id1 on hash_table_7 using hash(id) with (fillfactor = 30);
explain (costs off) select * from hash_table_7 where id = 80;
select count(*) from hash_table_7;
