create schema distribute_stat_2;
create schema distribute_stat2_2;

set current_schema = distribute_stat_2;

--test for replication table

--drop table test_analyze_rep;

set default_statistics_target=-2;
--test for column with type of character and there are blank space values with length exceed 20.
create table test_analyze_string(str character(39));
insert into test_analyze_string values ('170141183460469231731687303715884105728'),('.75000000000000000000                  '),('9941.776627963145224192                '),('170141183460469231731687303715884105727'),('170141183460469231731687303715884105726'),('                                       '),('3.0000000000000000                     ');
analyze test_analyze_string;
select histogram_bounds from pg_stats where tablename='test_analyze_string';
--drop table test_analyze_string;

--test analyze for column is not support in column store
create table test_analyze_unsupport_column(a int, b name) ;
insert into test_analyze_unsupport_column values (1,'Lincoln'),(2,'Poplar Hill'),(3,'Washington'),(4,'Davis Park');
analyze test_analyze_unsupport_column;
select histogram_bounds from pg_stats where tablename='test_analyze_unsupport_column';
--drop table test_analyze_unsupport_column;

--test analyze for the same column name with temp table.
create table test_analyze_same_colname(v text, v_count int, i float, x bool) ;
insert into test_analyze_same_colname values ('AAAAAAAABAAAAAAA',5,3.7,true),('BBBBBBBBBBBBBBB',9,10.3,false);
analyze test_analyze_same_colname;
--drop table test_analyze_same_colname;

create table date_part2_034 AS  select 1,date_part('microseconds', INTERVAL'1013 years 2 mons 29 days 11:47:59.8755');
analyze date_part2_034;
--drop table date_part2_034;
reset default_statistics_target;

--test for tsvector
--test analyze system table for coverty
analyze pg_class;
--test table which have index for enable_global_stats is off
create table ship_mode ( sm_ship_mode_sk integer not null, sm_ship_mode_id char(16) not null, sm_type char(30) , sm_code char(10) , sm_carrier char(20) , sm_contract char(20) , sm_email varchar(50) , sm_id varchar(20) , sm_phone varchar(20) , sm_num varchar(10) , sm_text varchar(2000) , sm_text_tv tsvector , sm_text_ts tsquery ) ;
create index i_ship_mode_zh5 on ship_mode using gin(to_tsvector('ngram',sm_text));
insert into ship_mode values(1,'AAAAAAAABAAAAAAA','EXPRESS ','AIR ','UPS ','YvxvxVaJI10') ;
analyze ship_mode;
set enable_global_stats=off;
analyze ship_mode;
analyze ship_mode;
--drop table ship_mode;
reset enable_global_stats;

set default_statistics_target=-5;
create table call_center_clone
(
     cc_call_center_sk         integer               not null,
     cc_call_center_id         char(16)              not null,
     cc_rec_start_date         date                          ,
     cc_rec_end_date   		 date                          
) ;
insert into call_center_clone values ( 1, 'AAAAAAAABAAAAAAA', '1998-01-01',NULL);
insert into call_center_clone values ( 8, 'AAAAAAAAIAAAAAAA', '1998-01-01','2000-12-31');
insert into call_center_clone values ( 9, 'AAAAAAAAIAAAAAAA', '2001-01-01',NULL);
insert into call_center_clone values (10, 'AAAAAAAAKAAAAAAA', '1998-01-01','2000-01-01');
insert into call_center_clone values (11, 'AAAAAAAAKAAAAAAA', '2000-01-02','2001-12-31');
insert into call_center_clone values (12, 'AAAAAAAAKAAAAAAA', '2002-01-01',NULL);
insert into call_center_clone values (13, 'AAAAAAAANAAAAAAA', '1998-01-01',NULL);
insert into call_center_clone values (25, 'AAAAAAAAJBAAAAAA', '1998-01-01',NULL);
insert into call_center_clone values (32, 'AAAAAAAAACAAAAAA', '1998-01-01','2000-12-31');
insert into call_center_clone values (33, 'AAAAAAAAACAAAAAA', '2001-01-01',NULL);
insert into call_center_clone values (34, 'AAAAAAAACCAAAAAA', '1998-01-01','2000-01-01');
insert into call_center_clone values (35, 'AAAAAAAACCAAAAAA', '2000-01-02','2001-12-31');
insert into call_center_clone values (36, 'AAAAAAAACCAAAAAA', '2002-01-01',NULL);
insert into call_center_clone values (38, 'AAAAAAAAGCAAAAAA', '1998-01-01','2000-12-31');
insert into call_center_clone values (39, 'AAAAAAAAGCAAAAAA', '2001-01-01',NULL);
insert into call_center_clone values (16, 'AAAAAAAAABAAAAAA', '1998-01-01','2000-01-01');
insert into call_center_clone values (17, 'AAAAAAAAABAAAAAA', '2000-01-02','2001-12-31');
insert into call_center_clone values (18, 'AAAAAAAAABAAAAAA', '2002-01-01',NULL);
insert into call_center_clone values (19, 'AAAAAAAADBAAAAAA', '1998-01-01',NULL);
insert into call_center_clone values (37, 'AAAAAAAAFCAAAAAA', '1998-01-01',NULL);
insert into call_center_clone values ( 7, 'AAAAAAAAHAAAAAAA', '1998-01-01',NULL);
insert into call_center_clone values (14, 'AAAAAAAAOAAAAAAA', '1998-01-01','2000-12-31');
insert into call_center_clone values (15, 'AAAAAAAAOAAAAAAA', '2001-01-01',NULL);
insert into call_center_clone values (20, 'AAAAAAAAEBAAAAAA', '1998-01-01','2000-12-31');
insert into call_center_clone values (21, 'AAAAAAAAEBAAAAAA', '2001-01-01',NULL);
insert into call_center_clone values (22, 'AAAAAAAAGBAAAAAA', '1998-01-01','2000-01-01');
insert into call_center_clone values (23, 'AAAAAAAAGBAAAAAA', '2000-01-02','2001-12-31');
insert into call_center_clone values (24, 'AAAAAAAAGBAAAAAA', '2002-01-01',NULL);
insert into call_center_clone values (26, 'AAAAAAAAKBAAAAAA', '1998-01-01','2000-12-31');
insert into call_center_clone values (27, 'AAAAAAAAKBAAAAAA', '2001-01-01',NULL);
insert into call_center_clone values (28, 'AAAAAAAAMBAAAAAA', '1998-01-01','2000-01-01');
insert into call_center_clone values (29, 'AAAAAAAAMBAAAAAA', '2000-01-02','2001-12-31');
insert into call_center_clone values (30, 'AAAAAAAAMBAAAAAA', '2002-01-01',NULL);
insert into call_center_clone values (31, 'AAAAAAAAPBAAAAAA', '1998-01-01',NULL);
insert into call_center_clone values (40, 'AAAAAAAAICAAAAAA', '1998-01-01','2000-01-01');
insert into call_center_clone values (41, 'AAAAAAAAICAAAAAA', '2000-01-02','2001-12-31');
insert into call_center_clone values (42, 'AAAAAAAAICAAAAAA', '2002-01-01',NULL);
insert into call_center_clone values ( 2, 'AAAAAAAACAAAAAAA', '1998-01-01','2000-12-31');
insert into call_center_clone values ( 3, 'AAAAAAAACAAAAAAA', '2001-01-01',NULL);
insert into call_center_clone values ( 4, 'AAAAAAAAEAAAAAAA', '1998-01-01','2000-01-01');
insert into call_center_clone values ( 5, 'AAAAAAAAEAAAAAAA', '2000-01-02','2001-12-31');
insert into call_center_clone values ( 6, 'AAAAAAAAEAAAAAAA', '2002-01-01',NULL);

-- check repro
analyze call_center_clone;
--drop table call_center_clone;

--null value
create table tbl_null(id int, name text);
analyze tbl_null;
--drop table tbl_null;
reset default_statistics_target;

reset current_schema; 
drop schema distribute_stat_2 cascade;
