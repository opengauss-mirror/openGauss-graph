CREATE schema FVT_COMPRESS_QWER;
set search_path to FVT_COMPRESS_QWER;
create table bmsql_order_line (
  ol_w_id         integer   not null,
  ol_d_id         integer   not null,
  ol_o_id         integer   not null,
  ol_number       integer   not null,
  ol_i_id         integer   not null,
  ol_delivery_d   timestamp,
  ol_amount       decimal(6,2),
  ol_supply_w_id  integer,
  ol_quantity     integer,
  ol_dist_info    char(24)
)
partition by hash(ol_d_id)
(
  partition p0,
  partition p1,
  partition p2
);
alter table bmsql_order_line add constraint bmsql_order_line_pkey primary key (ol_w_id, ol_d_id, ol_o_id, ol_number);
insert into bmsql_order_line(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_dist_info) values(1, 1, 1, 1, 1, '123');
update bmsql_order_line set ol_dist_info='ss' where ol_w_id =1;
delete from bmsql_order_line;

create table test_partition_for_null_hash_timestamp
(
	a timestamp without time zone,
	b timestamp with time zone,
	c int,
	d int) 
partition by hash (a) 
(
	partition test_partition_for_null_hash_timestamp_p1,
	partition test_partition_for_null_hash_timestamp_p2,
	partition test_partition_for_null_hash_timestamp_p3
);
create index idx_test_partition_for_null_hash_timestamp_1 on test_partition_for_null_hash_timestamp(a) LOCAL;
create index idx_test_partition_for_null_hash_timestamp_2 on test_partition_for_null_hash_timestamp(a,b) LOCAL;
create index idx_test_partition_for_null_hash_timestamp_3 on test_partition_for_null_hash_timestamp(c) LOCAL;
create index idx_test_partition_for_null_hash_timestamp_4 on test_partition_for_null_hash_timestamp(b,c,d) LOCAL;

create table test_partition_for_null_hash_text (a text, b varchar(2), c char(1), d varchar(2))
partition by hash (a) 
(
	partition test_partition_for_null_hash_text_p1,
	partition test_partition_for_null_hash_text_p2,
	partition test_partition_for_null_hash_text_p3
);
create index idx_test_partition_for_null_hash_text_1 on test_partition_for_null_hash_text(a) LOCAL;
create index idx_test_partition_for_null_hash_text_2 on test_partition_for_null_hash_text(a,b) LOCAL;
create index idx_test_partition_for_null_hash_text_3 on test_partition_for_null_hash_text(c) LOCAL;
create index idx_test_partition_for_null_hash_text_4 on test_partition_for_null_hash_text(b,c,d) LOCAL;
create index idx_test_partition_for_null_hash_text_5 on test_partition_for_null_hash_text(b,c,d);

CREATE TABLE select_partition_table_000_1(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(102400),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	partition by hash (C_BIGINT)
( 
     partition select_partition_000_1_1,
     partition select_partition_000_1_2
);
create index idx_select_partition_table_000_1_1 on select_partition_table_000_1(C_CHAR_1) LOCAL;
create index idx_select_partition_table_000_1_2 on select_partition_table_000_1(C_CHAR_1,C_VARCHAR_1) LOCAL;
create index idx_select_partition_table_000_1_3 on select_partition_table_000_1(C_BIGINT) LOCAL;
create index idx_select_partition_table_000_1_4 on select_partition_table_000_1(C_BIGINT,C_TS_WITH,C_DP) LOCAL;
create index idx_select_partition_table_000_1_5 on select_partition_table_000_1(C_BIGINT,C_NUMERIC,C_TS_WITHOUT);

CREATE TABLE select_partition_table_000_2(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(102400),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	partition by hash (C_SMALLINT)
( 
     partition select_partition_000_2_1,
     partition select_partition_000_2_2
);
create index idx_select_partition_table_000_2_1 on select_partition_table_000_2(C_CHAR_2) LOCAL;
create index idx_select_partition_table_000_2_2 on select_partition_table_000_2(C_CHAR_2,C_VARCHAR_2) LOCAL;
create index idx_select_partition_table_000_2_3 on select_partition_table_000_2(C_SMALLINT) LOCAL;
create index idx_select_partition_table_000_2_4 on select_partition_table_000_2(C_SMALLINT,C_TS_WITH,C_DP) LOCAL;
create index idx_select_partition_table_000_2_5 on select_partition_table_000_2(C_SMALLINT,C_NUMERIC,C_TS_WITHOUT);

CREATE TABLE select_partition_table_000_3(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(102400),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	partition by hash (C_NUMERIC)
( 
     partition select_partition_000_3_1,
     partition select_partition_000_3_2
);
CREATE TABLE select_partition_table_000_4(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(102400),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	partition by hash (C_FLOAT)
( 
     partition select_partition_000_4_1,
     partition select_partition_000_4_2
);

CREATE TABLE select_partition_table_000_5(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(102400),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	partition by hash (C_TS_WITHOUT)
( 
     partition select_partition_000_5_1,
     partition select_partition_000_5_2
);
create table test_hash (a int, b int)
partition by hash(a)
(
partition  p1 ,
partition  p2 ,
partition  p3 ,
partition  p4 ,
partition  p5 ,
partition  p6 ,
partition  p7 ,
partition  p8 ,
partition  p9 ,
partition  p10,
partition  p11,
partition  p12,
partition  p13,
partition  p14,
partition  p15,
partition  p16,
partition  p17,
partition  p18,
partition  p19,
partition  p20,
partition  p21,
partition  p22,
partition  p23,
partition  p24,
partition  p25,
partition  p26,
partition  p27,
partition  p28,
partition  p29,
partition  p30,
partition  p31,
partition  p32,
partition  p33,
partition  p34,
partition  p35,
partition  p36,
partition  p37,
partition  p38,
partition  p39,
partition  p40,
partition  p41,
partition  p42,
partition  p43,
partition  p44,
partition  p45,
partition  p46,
partition  p47,
partition  p48,
partition  p49,
partition  p50,
partition  p51,
partition  p52,
partition  p53,
partition  p54,
partition  p55,
partition  p56,
partition  p57,
partition  p58,
partition  p59,
partition  p60,
partition  p61,
partition  p62,
partition  p63,
partition  p64,
partition  p65,
partition  p66,
partition  p67,
partition  p68,
partition  p69,
partition  p70,
partition  p71,
partition  p72,
partition  p73,
partition  p74,
partition  p75,
partition  p76,
partition  p77,
partition  p78,
partition  p79,
partition  p80,
partition  p81,
partition  p82
);
drop table test_hash;
drop schema FVT_COMPRESS_QWER cascade;
