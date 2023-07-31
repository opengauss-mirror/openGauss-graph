--1 range partitioned table which's partition key  has single column
--		****BD: partition's top boundary,
--		****BD(2): boundary of the second range partition
--		****-BD(2): one value less than BD(2) and greater than BD(1)
--		****BD(Z): boundary of the last range partition
--		****+BD(Z): one value greater than DB(Z)
--		****BD(N): 1<N<Z
--		****ITEM: item expression, ITEM(<): < expression
--		****OTHER: does not support expression for pruning,
--		****NOKEY: expression which does not contain partition key
--		1.1 ITEM
--			1.1.1 </<=
--Y				1.1.1.1  <  -BD(1)
--Y				1.1.1.2  <= -BD(1)
--Y				1.1.1.3  <   BD(1)
--Y				1.1.1.4  <=  BD(1)
--Y				1.1.1.5  <  -BD(N)
--N				1.1.1.6  <= -BD(N)
--Y				1.1.1.7  <   BD(N)
--Y				1.1.1.8  <=  BD(N)
--N				1.1.1.9  <  -BD(Z)
--N				1.1.1.10 <= -BD(Z)
--Y				1.1.1.11 <   BD(Z)
--Y				1.1.1.12 <=  BD(Z)
--Y				1.1.1.13 <  +BD(Z)
--Y				1.1.1.14 <=  +BD(Z)
--			1.1.2 =
--Y				1.1.2.1  =  -BD(1)
--Y				1.1.2.2  =   BD(1)
--Y				1.1.2.3  =  -BD(N)
--N				1.1.2.4  =   BD(N)
--N				1.1.2.5  =  -BD(Z)
--Y				1.1.2.6  =   BD(Z)
--Y				1.1.2.7  =  +BD(Z)
--			1.1.3 >/>=
--Y				1.1.3.1  >  -BD(1)
--Y				1.1.3.2  >= -BD(1)
--Y				1.1.3.3  >   BD(1)
--Y				1.1.3.4  >=  BD(1)
--Y				1.1.3.5  >  -BD(N)
--Y				1.1.3.6  >= -BD(N)
--Y				1.1.3.7  >   BD(N)
--Y				1.1.3.8  >=  BD(N)
--N				1.1.3.9  >  -BD(Z)
--N				1.1.3.10 >= -BD(Z)
--Y				1.1.3.11 >   BD(Z)
--Y				1.1.3.12 >=  BD(Z)
--N				1.1.3.13 >  +BD(Z)
--			1.1.4 MAXVALUE
--N				1.1.4.1  <  -BD(Z)
--N				1.1.4.2  <= -BD(Z)
--N				1.1.4.3  =  -BD(Z)
--N				1.1.4.5  >= -BD(Z)
--N				1.1.4.6  >  -DB(Z)
--Y			1.1.5 IN (ArrayExpr)
--N			1.1.6 OTHER
--N			1.1.7 NOKEY
--		1.2 AND
--Y			1.2.1  ITEM(<) AND ITEM(<)
--Y			1.2.2  ITEM(>) AND ITEM(>)
--Y			1.2.3  ITEM(>) AND ITEM(<) (NOT NULL)
--Y			1.2.4  ITEM(>) AND ITEM(<) (NULL)
--N			1.2.5  ITEM(=) AND ITEM(<) (NOT NULL)
--N			1.2.6  ITEM(=) AND ITEM(<) (NULL)
--Y			1.2.7  ITEM(=) AND ITEM(>) (NOT NULL)
--N			1.2.8  ITEM(=) AND ITEM(>) (NULL)
--N			1.2.9  BETWEEN...AND...
--Y			1.2.10 ITEM(<) AND OTHER
--N			1.2.11 NOKEY AND ITEM(>)
--		1.3 OR
--Y			1.3.1  ITEM(<) OR ITEM(<)
--Y			1.3.2  ITEM(>) OR ITEM(>)
--Y			1.3.3  ITEM(>) OR ITEM(<) (NOT NULL)
--Y			1.3.4  ITEM(>) OR ITEM(<) (NULL)
--N			1.3.5  ITEM(=) OR ITEM(<) (NOT NULL)
--N			1.3.6  ITEM(=) OR ITEM(<) (NULL)
--Y			1.3.7  ITEM(=) OR ITEM(>) (NOT NULL)
--N			1.3.8  ITEM(=) OR ITEM(>) (NULL)
--Y			1.3.9  ITEM(<) OR OTHER
--N			1.3.10 NOKEY OR ITEM(>)
--		1.4 COMPOSITE
--Y			1.4.1 ITEM(<) AND ITEM(<) AND ITEM(>)
--Y			1.4.2 ITEM(>) AND ITEM(<) OR ITEM(>)
--Y			1.4.3 ITEM(>) OR  ITEM(<) OR ITEM(>) OR ITEM(>)
--Y			1.4.4 ITEM(>) AND ITEM(<) OR ITEM(>) AND ITEM(<)
--Y			1.4.5 (ITEM(>) OR ITEM(<)) AND ITEM(<)
--Y			1.4.6 (ITEM(>) OR ITEM(<)) AND ((ITEM(>) OR ITEM(<)))
--N			1.4.7 (BETWEEN...AND...) OR ITEM(>)
--Y			1.4.8 composite
--		4 datatype
--			4.1  int
--			4.2  smallint
--			4.3  integer
--			4.4  bigint
--			4.5  decimal
--			4.6  numeric
--			4.7  real
--			4.8  double precision
--			4.9  smallserial
--			4.10 serial
--			4.11 bigserial
--			4.12 character varying(n), varchar(n)
--			4.13 character(n), char(n)
--			4.14 character.char
--			4.15 text
--			4.16 nvarchar2
--			4.17 name
--			4.18 timestamp [ (p) ] [ without time zone ]
--			4.19 timestamp [ (p) ] with time zone
--			4.20 date
--		5 boundary test
--prepare
create table t_pruning_datatype_int32(c1 int,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than(100),
 partition p2 values less than(200),
 partition p3 values less than(300),
 partition p4 values less than(500));
 
insert into t_pruning_datatype_int32 values(-100,20,20,'a'),
               		 (100,300,300,'bb'),
			         (150,75,500,NULL),
			         (200,500,50,'ccc'),
			         (250,50,50,NULL),
			         (300,700,125,''),
			         (450,35,150,'dddd');

--partition pruning for int32
--1.1.2.1  =  -BD(1)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1=50;

--1.1.2.2  =   BD(1)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1=100;

--1.1.2.3  =  -BD(N)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1=250;

--1.1.2.6  =   BD(Z)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1=500;

--1.1.2.7  =  +BD(Z)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1=550;

--1.1.1.1  <  -BD(1)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<50;

--1.1.1.2  <= -BD(1)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<=50;

--1.1.1.3  <   BD(1)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<100;

--1.1.1.4  <=  BD(1)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<=100;

--1.1.1.5  <  -BD(N)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<150;

--1.1.1.7  <   BD(N)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<200;

--1.1.1.8  <=  BD(N)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<=200;

--1.1.1.11 <   BD(Z)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<500;

--1.1.1.12 <=  BD(Z)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<=500;

--1.1.1.13 <  +BD(Z)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<700;

--1.1.1.14 <= +BD(Z)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<=700;

--1.1.3.1  >  -BD(1)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>50;

--1.1.3.2  >= -BD(1)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>=50;

--1.1.3.3  >   BD(1)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>100;

--1.1.3.4  >=  BD(1)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>=100;

--1.1.3.5  >  -BD(N)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>150;

--1.1.3.6  >= -BD(N)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>=150;

--1.1.3.7  >   BD(N)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>200;

--1.1.3.8  >=  BD(N)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>=200;

--1.1.3.11 >   BD(Z)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>500;

--1.1.3.12 >=  BD(Z)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>=500;

--1.1.5 IN (ArrayExpr)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1 IN (230,330,350);

--1.2.1  ITEM(<) AND ITEM(<)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<50 AND t_pruning_datatype_int32.c1<250;

--1.2.2  ITEM(>) AND ITEM(>)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>50 AND t_pruning_datatype_int32.c1>=150;

--1.2.2  ITEM(>) AND ITEM(>)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>100 AND t_pruning_datatype_int32.c1>=100;

--1.2.3  ITEM(>) AND ITEM(<) (NOT NULL)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<50 AND t_pruning_datatype_int32.c1>0;

--1.2.3  ITEM(>) AND ITEM(<) (NOT NULL)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>=100 AND t_pruning_datatype_int32.c1<300;

--1.2.3  ITEM(>) AND ITEM(<) (NOT NULL)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>=100 AND t_pruning_datatype_int32.c1<550;

--1.2.4  ITEM(>) AND ITEM(<) (NULL)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<50 AND t_pruning_datatype_int32.c1>100;

--1.2.4  ITEM(>) AND ITEM(<) (NULL)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>250 AND t_pruning_datatype_int32.c1<50;

--1.2.7  ITEM(=) AND ITEM(>) (NOT NULL)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>=100 AND t_pruning_datatype_int32.c1=100;

--1.2.10 ITEM(<) AND OTHER
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c4 IS NULL AND (t_pruning_datatype_int32.c1>500 OR t_pruning_datatype_int32.c1<250) AND (t_pruning_datatype_int32.c1>300 AND t_pruning_datatype_int32.c1<t_pruning_datatype_int32.c2) AND (t_pruning_datatype_int32.c2<t_pruning_datatype_int32.c3 OR t_pruning_datatype_int32.c2>100);

--1.3.1  ITEM(<) OR ITEM(<)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<50 OR t_pruning_datatype_int32.c1<250;

--1.3.2  ITEM(>) OR ITEM(>)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>50 OR t_pruning_datatype_int32.c1>=150;

--1.3.2  ITEM(>) OR ITEM(>)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>100 OR t_pruning_datatype_int32.c1>=100;

--1.3.3  ITEM(>) OR ITEM(<) (NOT NULL)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<50 OR t_pruning_datatype_int32.c1>0;

--1.3.3  ITEM(>) OR ITEM(<) (NOT NULL)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>=100 OR t_pruning_datatype_int32.c1<200;

--1.3.4  ITEM(>) OR ITEM(<) (NULL)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<50 OR t_pruning_datatype_int32.c1>100;

--1.3.4  ITEM(>) OR ITEM(<) (NULL)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>500 OR t_pruning_datatype_int32.c1<250;

--1.3.4  ITEM(>) OR ITEM(<) (NULL)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>250 OR t_pruning_datatype_int32.c1<50;

--1.3.7  ITEM(=) OR ITEM(>) (NOT NULL)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>=100 OR t_pruning_datatype_int32.c1=100;

--1.3.9  ITEM(<) OR OTHER
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c4 IS NULL OR (t_pruning_datatype_int32.c1>500 OR t_pruning_datatype_int32.c1<250) AND (t_pruning_datatype_int32.c1>300 AND t_pruning_datatype_int32.c1<t_pruning_datatype_int32.c2) AND (t_pruning_datatype_int32.c2<t_pruning_datatype_int32.c3 OR t_pruning_datatype_int32.c2>100);

--1.4.1 ITEM(<) AND ITEM(<) AND ITEM(>)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>100 AND t_pruning_datatype_int32.c1<=500 AND t_pruning_datatype_int32.c1>=100 AND t_pruning_datatype_int32.c1<500;


--1.4.1 ITEM(<) AND ITEM(<) AND ITEM(>)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>50 AND t_pruning_datatype_int32.c1>100 AND t_pruning_datatype_int32.c1>=100 AND t_pruning_datatype_int32.c1<250 AND t_pruning_datatype_int32.c1<=250 AND t_pruning_datatype_int32.c1=200;


--1.4.2 ITEM(>) AND ITEM(<) OR ITEM(>)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<170  AND ( t_pruning_datatype_int32.c1>600 OR t_pruning_datatype_int32.c1<150);


--1.4.3 ITEM(>) OR  ITEM(<) OR ITEM(>) OR ITEM(>)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>100 OR t_pruning_datatype_int32.c1<=300 OR t_pruning_datatype_int32.c1>=100 OR t_pruning_datatype_int32.c1<300;

--1.4.5 (ITEM(>) OR ITEM(<)) AND ITEM(<)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where (t_pruning_datatype_int32.c1<170 OR t_pruning_datatype_int32.c1<250)  AND ( t_pruning_datatype_int32.c1>600 OR t_pruning_datatype_int32.c1<150);

--1.4.2 ITEM(>) AND ITEM(<) OR ITEM(>)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1<50 OR t_pruning_datatype_int32.c1>250 AND t_pruning_datatype_int32.c1<400;

--1.4.4 ITEM(>) AND ITEM(<) OR ITEM(>) AND ITEM(<)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>=-100 AND t_pruning_datatype_int32.c1<50 OR t_pruning_datatype_int32.c1>300 AND t_pruning_datatype_int32.c1<700; 

--1.4.4 ITEM(>) AND ITEM(<) OR ITEM(>) AND ITEM(<)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1>=-100 AND t_pruning_datatype_int32.c1<=100 OR t_pruning_datatype_int32.c1>300 AND t_pruning_datatype_int32.c1<700; 

--1.4.6 (ITEM(>) OR ITEM(<)) AND ((ITEM(>) OR ITEM(<)))
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where (t_pruning_datatype_int32.c1>500 OR t_pruning_datatype_int32.c1<250) AND (t_pruning_datatype_int32.c1>300 AND t_pruning_datatype_int32.c1<t_pruning_datatype_int32.c2) AND (t_pruning_datatype_int32.c2<t_pruning_datatype_int32.c3 OR t_pruning_datatype_int32.c2>100) OR t_pruning_datatype_int32.c4 IS NULL;

--1.4.6 (ITEM(>) OR ITEM(<)) AND ((ITEM(>) OR ITEM(<)))
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where (t_pruning_datatype_int32.c1>500 OR t_pruning_datatype_int32.c1<250) AND (t_pruning_datatype_int32.c1>300 AND t_pruning_datatype_int32.c1<t_pruning_datatype_int32.c2) AND (t_pruning_datatype_int32.c2<t_pruning_datatype_int32.c3 OR t_pruning_datatype_int32.c2>100) AND t_pruning_datatype_int32.c4 IS NULL;

--1.4.8 composite
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where (t_pruning_datatype_int32.c1>500 OR t_pruning_datatype_int32.c1<250) AND (t_pruning_datatype_int32.c1>300 AND t_pruning_datatype_int32.c4 IS NULL) AND (t_pruning_datatype_int32.c2<t_pruning_datatype_int32.c3 OR t_pruning_datatype_int32.c2>100);

--1.4.8 composite
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where (t_pruning_datatype_int32.c1>500 OR t_pruning_datatype_int32.c1<250) AND (t_pruning_datatype_int32.c1>300 AND t_pruning_datatype_int32.c1<t_pruning_datatype_int32.c2) AND (t_pruning_datatype_int32.c4 IS NULL OR t_pruning_datatype_int32.c2>100);

--1.4.9 param(no pruning)
set enable_hashjoin=off;
set enable_mergejoin=off;
set enable_seqscan=off;
set enable_material=off;
create index t_pruning_datatype_int32_i on t_pruning_datatype_int32(c1) local;
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 where c2 in (select c1 from t_pruning_datatype_int32);
drop index t_pruning_datatype_int32_i;
reset enable_hashjoin;
reset enable_mergejoin;
reset enable_seqscan;
reset enable_material;

--1.4.10 placeholder(no pruning)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int32 t1,
  (select * from
    (select c3, 2 as "2" from t_pruning_datatype_int32) t2
    natural left join
    (select c4, 3 as "3" from t_pruning_datatype_int32) t3
  ) t4
where t1.c1 = t4."3";

--4 datatype
--4.1  int
--Done

--4.2  smallint
create table t_pruning_datatype_int16(c1 smallint,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than(100),
 partition p2 values less than(200),
 partition p3 values less than(300),
 partition p4 values less than(500));

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int16 where c1>=-100 AND c1<50 OR c1>300 AND c1<700; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int16 where c1 IN (150,250,500,600); 

--4.3  integer
--Done

--4.4  bigint
create table t_pruning_datatype_int64(c1 bigint,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than(100),
 partition p2 values less than(200),
 partition p3 values less than(300),
 partition p4 values less than(500));
 
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int64 where c1>=-100 AND c1<50 OR c1>300 AND c1<700; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_int64 where c1 IN (150,250,500,600); 

--4.5  decimal
create table t_pruning_datatype_decimal(c1 decimal,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than(100.11),
 partition p2 values less than(200.22),
 partition p3 values less than(300.33),
 partition p4 values less than(500.55));

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_decimal where c1>=-100.11 AND c1<50.0 OR c1>300.33 AND c1<700; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_decimal where c1 IN (100.11,250.0, 300.33,700); 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_decimal where c1 IN (150,250,500,600);

--4.6  numeric
create table t_pruning_datatype_numeric(c1 numeric,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than(100.11),
 partition p2 values less than(200.22),
 partition p3 values less than(300.33),
 partition p4 values less than(500.55));

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_numeric where c1>=-100.11 AND c1<50.0 OR c1>300.33 AND c1<700; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_numeric where c1 IN (100.11,250.0, 300.33,700); 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_numeric where c1 IN (150,250,500,600);


drop table t_pruning_datatype_int32;
drop table t_pruning_datatype_int16;
drop table t_pruning_datatype_int64;
drop table t_pruning_datatype_decimal;
drop table t_pruning_datatype_numeric;