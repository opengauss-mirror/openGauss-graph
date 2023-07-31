\c postgres
set current_schema='sql_self_tuning';
set resource_track_duration=0;
set resource_track_cost=30;
set resource_track_level=operator;
set resource_track_log=summary;

/*
 * SQL-Self Tuning scenario[1] query not plan shipping
 */
/* distinct on */
select distinct on (not_plan_shipping_01)t4.c1 not_plan_shipping_01  from t4, t5 where t4.c1=t5.c2 and t5.c3 < 20;
/* for update */
select t5.c1 not_plan_shipping_02 from t5, t4 where t5.c1=t4.c2  for update;
/* with recursive */
with recursive t5_recursive(c1, c2) as (select 1, c2 from t5 where c1 = 1)
select c1 not_plan_shipping_03 from t5_recursive order by 1 limit 3;

/* returing list */
update t4 set c2 = 1024 where (c2 - 1024) > (select max(c2) from t5 where c3 > 1000 and t4.c2 = t5.c3 order by 1) returning c2 not_plan_shipping_04;

/* row() */
select row(t4.*, 12) not_plan_shipping_05 from t4, t5 where t4.c1 = t5.c2 and t5.c3 < 15  order by 1 limit 3;

/* row() compare */
select (t4.c1, t5.c2) < (1,2)  not_plan_shipping_06 from t4, t5 where t4.c1 = t5.c1 and t5.c3 < 6 order by 1 limit 1;

/* array cast*/
select a.c1::float[]  not_plan_shipping_07 from t1 a , t1 b where a.c1 = b.c1 and b.c2 in (select c2 from t5 order by 1);

/* array expression*/
select array[c1,1]  not_plan_shipping_08 from t5 where c2 in (select c1 from t4 where t4.c1 < 15 ) order by 1 limit 1;

/* table in targetlist */
select t4 not_plan_shipping_09,t4.c1 from t4, t5 where t4.c1=t5.c2 and t5.c3 < 30 order by 2,1 limit 1;

/* no from, no setop */
select distinct(t5.c1) not_plan_shipping_10, t4.c1  from t4, t5, (select 1 col)  tmp where t4.c1=t5.c2 and t5.c3=tmp.col and t5.c3 < 30;

/* Record in targetlist */
select each(t2.c2) not_plan_shipping_11 from t2, t4 where t4.c1=t2.c1;

/* IMMUTABLE function (every) */
select every(t4.c1) not_plan_shipping_12 from t4,t5 where  t4.c1 = t5.c2 and t5.c3 < 30;

/* volatile function */
select count(*) from (select count(*) not_plan_shipping_13 from t4,t5 where t4.c1>clock_timestamp() and t4.c1 = t5.c2 and t5.c3<30) tmp;

/* stable function */
select count(*) from (select count(*) not_plan_shipping_14 from t4,t5 where t4.c1>now() and t4.c1 = t5.c2 and t5.c3< 100) tmp ;

/* OID in Target table when insert */
insert into pg_class (relnamespace,reltype)  select count(t4.c1) not_plan_shipping_15, t5.c2 
from t4,t5 where t4.c1>t5.c2 and t5.c3< 300 and t4.c1 > 5 group by t5.c2 order by t5.c2;

/* agg function：string_agg(text, text) */
select string_agg(t3.c1,t3.c2) not_plan_shipping_16 from t3 ,t5 where t3.c3=t5.c3;

/* agg function：string_agg(bytea, bytea) */
select string_agg(t6.c1,t6.c2) not_plan_shipping_17 from t6 ,t5 where t6.c3=t5.c3;

/* order by in agg function */
select count(t5.c1 order by t5.c1) not_plan_shipping_20 from t5, t4 where t5.c2 = t4.c3 and t5.c3<100;

/* more than one count(distinct) */
with tmp as (select c3, c4 from t4 where c3>100) 
select count(distinct(c1)) not_plan_shipping_23, count(distinct(c2)) from t5, tmp where t5.c1 = tmp.c3 and t5.c2 = tmp.c4 and t5.c3<100;

/* string_agg/array_agg use in grouping sets */
select string_agg(t11.c2,',') not_plan_shipping_25 from t11, t5 where t5.c2=t11.c2 group by cube(t11.c2);

/* finalize_node_id */
CREATE TABLE t12
(C_INTEGER INTEGER NOT NULL, C_SMALLINT SMALLINT NOT NULL, C_BIGINT BIGINT NOT NULL, 
C_DECIMAL DECIMAL , C_NUMERIC NUMERIC CHECK(C_NUMERIC <> 3.24), C_REAL REAL , 
C_DOUBLE DOUBLE PRECISION  , C_BIGSERIAL BIGINT , C_MONEY MONEY , C_CHARACTER CHARACTER VARYING(4096) , 
C_VARCHAR VARCHAR(4096) , C_CHAR CHAR(4096) , C_TEXT TEXT , C_BYTEA BYTEA , C_TIMESTAMP_1 TIMESTAMP WITHOUT TIME ZONE ,
C_TIMESTAMP_2 TIMESTAMP WITH TIME ZONE , C_BOOLEAN BOOLEAN , C_POINT POINT , C_LSEG LSEG , C_BOX BOX , C_PATH PATH , C_POLYGON POLYGON , 
C_CIRCLE CIRCLE , C_CIDR CIDR , C_INET  INET , C_MACADDR MACADDR , C_BIT_1 BIT(6) , C_BIT_2 BIT VARYING(6) , C_OID OID , C_REGPROC REGPROC , 
C_REGPROCEDURE REGPROCEDURE , C_REGOPERATOR REGOPERATOR , C_REGCLASS REGCLASS , C_REGTYPE REGTYPE , C_CHARACTER_1 CHARACTER(4096) , 
C_INTERVAL INTERVAL , C_DATE DATE , C_TIMESTAMP_3 TIME WITHOUT TIME ZONE , C_TIMESTAMP_4 TIME WITH TIME ZONE,C_NUMBER INT,C_NVARCHAR2 CHARACTER VARYING(4096),
C_SMALLDATETIME  TIMESTAMP,C_TINYINT INT, C_SERIAL INT)WITH (FILLFACTOR =70)
partition by range (C_INTEGER,C_CHAR)
(
	partition TABLE_PT_114_1 values less than (500,'f'),
	partition TABLE_PT_114_2 values less than (1000,'g'),
	partition TABLE_PT_114_3 values less than (3000,'g')
)enable row movement;
 
insert into t12(C_INTEGER , C_SMALLINT , C_BIGINT , C_DECIMAL , C_NUMERIC, C_REAL , C_DOUBLE , C_BIGSERIAL  , C_MONEY , C_CHARACTER  , 
C_VARCHAR , C_CHAR , C_TEXT, C_BYTEA , C_TIMESTAMP_1  ,C_TIMESTAMP_2  , C_BOOLEAN  , C_POINT, C_LSEG, C_BOX , C_PATH , C_POLYGON, 
C_CIRCLE, C_CIDR , C_INET, C_MACADDR, C_BIT_1, C_BIT_2, C_OID, C_REGPROC  , C_REGPROCEDURE , C_REGOPERATOR , C_REGCLASS , 
C_REGTYPE , C_CHARACTER_1 , C_INTERVAL , C_DATE , C_TIMESTAMP_3 , C_TIMESTAMP_4 ,C_NUMBER ,C_NVARCHAR2 ,C_SMALLDATETIME ,C_TINYINT) 
SELECT trunc(random() * 2999 + 1) not_plan_shipping_27, 1, 3, 0.124, 3.234, 1.2345, 0.1712,  21474836478, 1000.00, 'aaaaa', 
'BBBBBBBB', 'ccccccccc', 'hello', '1101', '2004-10-19 10:23:01', '2004-10-19 10:23:01 +2', true, '(1,3)', '((-1,-7), (-4,2))', 
'((10, 3), (3,19))', '((4,2),(5,1))', '((1,2),(3,4),(5,6))', '<(1,2),3>', '10.1.2.3', '192.168.61.68', '08-00-2b-01-02-03', '101000', 
'101111', 123, 'pi', 'sqrt(numeric)', '-(bigint, bigint)', 'pg_am', 'float', 'aaaaa', '1 12:59:10', '2013-04-16', '13:50:00', '13:50:00 +8',
1.2,'FG','2013-04-16',99 FROM generate_series(1,100);

create table t_subplan1(a1 int, b1 int, c1 int, d1 int) distribute by hash(a1, b1);
create table t_subplan2(a2 int, b2 int, c2 int, d2 int) distribute by hash(a2, b2);
insert into t_subplan1 select generate_series(1, 100)%98, generate_series(1, 100)%20, generate_series(1, 100)%13, generate_series(1, 100)%6;
insert into t_subplan2 select generate_series(1, 50)%48, generate_series(1, 50)%28, generate_series(1, 50)%12, generate_series(1, 50)%9;

select (with cte(foo) as 
(select not_plan_shipping_28.b1 from t_subplan2, t_subplan1 where t_subplan2.c2 = t_subplan1.c1 order by 1 limit 1) 
values((select foo from cte order by 1))) from t_subplan1  not_plan_shipping_28 order by 1 limit 1;

SELECT
  (SELECT n
     FROM (VALUES (1)) AS x,
          (SELECT n FROM generate_series(1,10) AS n
             ORDER BY n LIMIT 1 OFFSET not_plan_shipping_29-1) AS y) AS z
  FROM generate_series(1,10) AS not_plan_shipping_29;

/* check the not plan-shipping results   */
select query, warning from pgxc_wlm_session_history where query like '%not_plan_shipping%' order by start_time,1,2;

\c regression

