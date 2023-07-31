--01--------------------------------------------------------------------
--prepare table , index

drop table if exists partition_reindex_table3;
create table partition_reindex_table3
(
	c1 int,
	c2 int,
	C3 date not null
)
partition by range (C3)
INTERVAL ('1 month') 
(
	PARTITION partition_reindex_table3_p0 VALUES LESS THAN ('2020-02-01'),
	PARTITION partition_reindex_table3_p1 VALUES LESS THAN ('2020-05-01'),
	PARTITION partition_reindex_table3_p2 VALUES LESS THAN ('2020-06-01')
)
enable row movement;

create index partition_reindex_table3_ind1 on partition_reindex_table3(c1) local;
create index partition_reindex_table3_ind2 on partition_reindex_table3(c2) local;
create index partition_reindex_table3_ind3 on partition_reindex_table3(c3) local;

--02--------------------------------------------------------------------
--reindex index, cross test with insert
insert into partition_reindex_table3 values (generate_series(1,10), generate_series(1,10), generate_series(TO_DATE('2020-01-01', 'YYYY-MM-DD'),TO_DATE('2020-07-01', 'YYYY-MM-DD'),'1, day'));
select count(*) from partition_reindex_table3;
select relname, parttype, partstrategy, boundaries from pg_partition
	where parentid = (select oid from pg_class where relname = 'partition_reindex_table3')
	order by 1;
truncate table partition_reindex_table3;
insert into partition_reindex_table3 values (generate_series(1,10), generate_series(1,10), generate_series(TO_DATE('2020-01-01', 'YYYY-MM-DD'),TO_DATE('2020-07-01', 'YYYY-MM-DD'),'1, day'));
select count(*) from partition_reindex_table3;
select relname, parttype, partstrategy, boundaries from pg_partition
	where parentid = (select oid from pg_class where relname = 'partition_reindex_table3')
	order by 1;

analyze partition_reindex_table3;
select c.relname,c.relpages > 0 as relpagesgtzero, c.reltuples > 0 as reltuplesgtzero,i.indisunique, i.indisvalid, i.indcheckxmin, i.indisready from pg_index i, pg_class c where c.relname = 'partition_reindex_table3_ind1' and c.oid = i.indexrelid;
explain (costs off) select * from partition_reindex_table3 where c3 = TO_DATE('2020-04-21', 'YYYY-MM-DD') and c2 = 8;
select * from partition_reindex_table3 where c3 = TO_DATE('2020-04-21', 'YYYY-MM-DD') and c2 = 8;
reindex index partition_reindex_table3_ind1;
select c.relname,c.relpages > 0 as relpagesgtzero, c.reltuples > 0 as reltuplesgtzero,i.indisunique, i.indisvalid, i.indcheckxmin, i.indisready from pg_index i, pg_class c where c.relname = 'partition_reindex_table3_ind1' and c.oid = i.indexrelid;
explain (costs off) select * from partition_reindex_table3 where c3 = TO_DATE('2020-04-21', 'YYYY-MM-DD') and c2 = 8;
--the plan before reindex and after reindex should be same
select * from partition_reindex_table3 where c3 = TO_DATE('2020-04-21', 'YYYY-MM-DD') and c2 = 8;

--03--------------------------------------------------------------------
--reindex table   cross test with truncate

truncate table partition_reindex_table3;

insert into partition_reindex_table3 values (generate_series(1,10), generate_series(1,10), generate_series(TO_DATE('2020-01-01', 'YYYY-MM-DD'),TO_DATE('2020-07-01', 'YYYY-MM-DD'),'1, day'));
analyze partition_reindex_table3;
select c.relname,c.relpages > 0 as relpagesgtzero, c.reltuples > 0 as reltuplesgtzero,i.indisunique, i.indisvalid, i.indcheckxmin, i.indisready from pg_index i, pg_class c where c.relname = 'partition_reindex_table3_ind1' and c.oid = i.indexrelid;
explain (costs off) select * from partition_reindex_table3 where c3 = TO_DATE('2020-04-21', 'YYYY-MM-DD') and c2 = 8;
select * from partition_reindex_table3 where c3 = TO_DATE('2020-04-21', 'YYYY-MM-DD') and c2 = 8;
reindex index partition_reindex_table3_ind1;
select c.relname,c.relpages > 0 as relpagesgtzero, c.reltuples > 0 as reltuplesgtzero,i.indisunique, i.indisvalid, i.indcheckxmin, i.indisready from pg_index i, pg_class c where c.relname = 'partition_reindex_table3_ind1' and c.oid = i.indexrelid;
explain (costs off) select * from partition_reindex_table3 where c3 = TO_DATE('2020-04-21', 'YYYY-MM-DD') and c2 = 8;
--the plan before reindex and after reindex should be same
select * from partition_reindex_table3 where c3 = TO_DATE('2020-04-21', 'YYYY-MM-DD') and c2 = 8;

--clean 
drop table partition_reindex_table3;
