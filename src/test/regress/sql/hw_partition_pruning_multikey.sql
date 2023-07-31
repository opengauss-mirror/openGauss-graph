--3 range partitioned table which's partition key contain multiple columns
--      ****BD: partition's top boundary,
--		****BD(2): boundary of the second range partition
--		****-BD(2): one value less than BD(2) and greater than BD(1)
--		****BD(Z): boundary of the last range partition
--		****+BD(Z): one value greater than DB(Z)
--		****BD(N): 1<N<Z
--		****ITEM: item expression, ITEM(<): < expression
--		****OTHER: does not support expression for pruning,
--		****NOKEY: expression which does not contain partition key
--		3.1 expression(c1)
--			3.1.1 ITEM
--Y				3.1.1.1  <  -BD(1)
--Y				3.1.1.2  <= -BD(1)
--Y				3.1.1.3  <   BD(1)
--Y				3.1.1.4  <=  BD(1)
--Y				3.1.1.5  <  -BD(N)
--Y				3.1.1.6  <= -BD(N)
--Y				3.1.1.7  <   BD(N)
--Y				3.1.1.8  <=  BD(N)
--Y				3.1.1.9  <  -BD(Z)
--Y				3.1.1.10 <= -BD(Z)
--Y				3.1.1.11 <   BD(Z)
--Y				3.1.1.12 <=  BD(Z)
--Y				3.1.1.13 <  +BD(Z)
--Y				3.1.1.14 >  -BD(1)
--Y				3.1.1.15 >= -BD(1)
--Y				3.1.1.16 >   BD(1)
--Y				3.1.1.17 >=  BD(1)
--Y				3.1.1.18 >  -BD(N)
--Y				3.1.1.19 >= -BD(N)
--Y				3.1.1.20 >   BD(N)
--Y				3.1.1.21 >=  BD(N)
--Y				3.1.1.22 >   BD(Z)
--Y				3.1.1.23 >=  BD(Z)
--Y				3.1.1.24 >  +BD(Z)
--Y				3.1.1.25 =  -BD(1)
--Y				3.1.1.26 =   BD(1)
--Y				3.1.1.27 =  -BD(N)
--Y				3.1.1.28 =   BD(N)
--Y				3.1.1.29 =   BD(Z)
--			3.1.2 composite
--Y				3.1.2.1  ITEM(<) AND ITEM(<)
--Y				3.1.2.2  ITEM(>) AND ITEM(>)
--Y				3.1.2.3  ITEM(>) AND ITEM(<) (NOT NULL)
--Y				3.1.2.4  ITEM(>) AND ITEM(<) (NULL)
--Y				3.1.2.5  ITEM(=) OR ITEM(<) (NOT NULL)
--Y				3.1.2.6  ITEM(=) OR ITEM(<) (NULL)
--Y				3.1.2.7  ITEM(=) OR ITEM(>) (NOT NULL)
--Y				3.1.2.8  ITEM(=) OR ITEM(>) (NULL)
--Y				3.1.2.9  ITEM(<) OR ITEM(>) (NOT NULL)
--Y				3.1.2.10 ITEM(<) OR ITEM(>) (NULL)
--Y				3.1.2.11 ITEM(<) OR OTHER
--Y				3.1.2.12 NOKEY OR ITEM(>)
--		3.2 expression(c1,c2)
--			3.2.1 >/>=
--Y				3.2.1.1 ITEM(c1>)  AND ITEM(c2>)
--U				3.2.1.2 ITEM(c1>=) AND ITEM(c2>)
--Y				3.2.1.3 ITEM(c1>)  AND ITEM(c2>=)
--Y				3.2.1.4 ITEM(c1>=) AND ITEM(c2>=)
--			3.2.2 =
--Y				3.2.2.1 ITEM(c1=)  AND ITEM(c2=)
--			3.2.3 </<=
--Y				3.2.3.1 ITEM(c1<)  AND ITEM(c2<)
--U				3.2.3.2 ITEM(c1<=) AND ITEM(c2<)
--Y				3.2.3.3 ITEM(c1<)  AND ITEM(c2<=)
--Y				3.2.3.4 ITEM(c1<=) AND ITEM(c2<=)
--			3.2.4 composite
--U				3.2.4.1 ITEM(c1>)   AND ITEM(c2>)  AND ITEM(c1<)   AND ITEM(c2<)
--U				3.2.4.2 ITEM(c1>=)  AND ITEM(c2>)  AND ITEM(c1<)   AND ITEM(c2<=)
--Y				3.2.4.3 ITEM(c1>=)  AND ITEM(c2>=) AND ITEM(c1<=)  AND ITEM(c2<=)
--Y				3.2.4.4 (ITEM(c1>)   AND ITEM(c2>))  OR (ITEM(c1<)   AND ITEM(c2<))
--Y				3.2.4.5 (ITEM(c1>=)  AND ITEM(c2>))  OR (ITEM(c1<)   AND ITEM(c2<=))
--Y				3.2.4.6 (ITEM(c1>=)  AND ITEM(c2>=)) OR (ITEM(c1<=)  AND ITEM(c2<=)) 
--		3.3 expression(c1,c2,c3)
--			3.3.1 >/>=
--Y				3.3.1.1 ITEM(c1>)  AND ITEM(c2>)  AND ITEM(c3>)
--U				3.3.1.2 ITEM(c1>=) AND ITEM(c2>)  AND ITEM(c3>)
--U				3.3.1.3 ITEM(c1>=)  AND ITEM(c2>=) AND ITEM(c3>)
--U				3.3.1.4 ITEM(c1>=) AND ITEM(c2>=) AND ITEM(c3>=)
--			3.3.2 =
--Y				3.3.2.1 ITEM(c1=)  AND ITEM(c2=)  AND ITEM(c3=)
--			3.3.3 </<=
--Y				3.3.3.1 ITEM(c1<)  AND ITEM(c2<)  AND ITEM(c3<)
--N				3.3.3.2 ITEM(c1<=) AND ITEM(c2<)  AND ITEM(c3<)
--U				3.3.3.3 ITEM(c1<=)  AND ITEM(c2<=) AND ITEM(c3<)
--Y				3.3.3.4 ITEM(c1<=) AND ITEM(c2<=) AND ITEM(c3<=)
--			3.3.4 composite
--N				3.3.4.1 ITEM(c1>)   AND ITEM(c2>) AND ITEM(c3>) AND ITEM(c1<)   AND ITEM(c2<)
--N				3.3.4.2 ITEM(c1>=)  AND ITEM(c2>) AND ITEM(c3>) AND ITEM(c1<)   AND ITEM(c2<=)
--Y				3.3.4.3 ITEM(c1>=)  AND ITEM(c2>=) AND ITEM(c3=>) AND ITEM(c1<=)  AND ITEM(c2<=)
--N				3.3.4.4 (ITEM(c1>)   AND ITEM(c2>) AND ITEM(c3>))  OR (ITEM(c1<)   AND ITEM(c2<))
--N				3.3.4.5 (ITEM(c1>=)  AND ITEM(c2>) AND ITEM(c3>))  OR (ITEM(c1<)   AND ITEM(c2<=))
--Y				3.3.4.6 (ITEM(c1>=)  AND ITEM(c2>=) AND ITEM(c3>=)) OR (ITEM(c1<=)  AND ITEM(c2<=)) 
--		3.4 expression(c1,c2,c3,c4)
--			3.4.1 >/>=
--Y				3.4.1.1 ITEM(c1>)  AND ITEM(c2>)  AND ITEM(c3>) AND ITEM(c4>)
--N				3.4.1.2 ITEM(c1>=) AND ITEM(c2>)  AND ITEM(c3>) AND ITEM(c4>)
--Y				3.4.1.3 ITEM(c1>)  AND ITEM(c2>=) AND ITEM(c3>=) AND ITEM(c4>=)
--Y				3.4.1.4 ITEM(c1>=) AND ITEM(c2>=) AND ITEM(c3>=) AND ITEM(c4>=)
--			3.4.2 =
--Y				3.4.2.1 ITEM(c1=)  AND ITEM(c2=)  AND ITEM(c3=) AND ITEM(c4=)
--			3.4.3 </<=
--N				3.4.3.1 ITEM(c1<)  AND ITEM(c2<)  AND ITEM(c3<) AND ITEM(c4<)
--N				3.4.3.2 ITEM(c1<=) AND ITEM(c2<)  AND ITEM(c3<) AND ITEM(c4<)
--Y				3.4.3.3 ITEM(c1<)  AND ITEM(c2<=) AND ITEM(c3<) AND ITEM(c4<)
--Y				3.4.3.4 ITEM(c1<=) AND ITEM(c2<=) AND ITEM(c3<=) AND ITEM(c4<=)
--			3.4.4 composite
--N				3.4.4.1 ITEM(c1>)   AND ITEM(c2>) AND ITEM(c3>) AND ITEM(c4>) AND ITEM(c1<)   AND ITEM(c2<)
--N				3.4.4.2 ITEM(c1>=)  AND ITEM(c2>) AND ITEM(c3>) AND ITEM(c4>) AND ITEM(c1<)   AND ITEM(c2<=)
--Y				3.4.4.3 ITEM(c1>=)  AND ITEM(c2>=) AND ITEM(c3>=) AND ITEM(c4>=) AND ITEM(c1<=)  AND ITEM(c2<=)
--N				3.4.4.4 (ITEM(c1>)   AND ITEM(c2>) AND ITEM(c3>) AND ITEM(c4>))  OR (ITEM(c1<)   AND ITEM(c2<))
--N				3.4.4.5 (ITEM(c1>)  AND ITEM(c2>=) AND ITEM(c3>) AND ITEM(c4>))  OR (ITEM(c1<)   AND ITEM(c2<=))
--U				3.4.4.6 (ITEM(c1>=)  AND ITEM(c2>=) AND ITEM(c3>=) AND ITEM(c4>=)) OR (ITEM(c1<=)  AND ITEM(c2<=)) 
--		3.5 expression(c1,c3)
--			3.5.1 >/>=
--Y				3.5.1.1 ITEM(c1>)  AND ITEM(c3>)
--			3.5.2 =
--U				3.5.2.1 ITEM(c1=)  AND ITEM(c3=)
--			3.5.3 </<=
--Y				3.5.3.1 ITEM(c1<=) AND ITEM(c3<=)
--			3.5.4 composite
--Y				3.3.4.1 ITEM(c1>)   AND ITEM(c3>)  AND ITEM(c1<)   AND ITEM(c2<)
--		3.6 expression(c2)
--Y			3.6.1 ITEM(c2>)
--Y			3.6.2 ITEM(c2<)
--Y			3.6.3 ITEM(c2>=) OR (ITEM(c1>)   AND ITEM(c2>))
--		3.7 expression(c2,c4)
--Y			3.7.1 ITEM(c2>) AND ITEM(c4>)
--Y			3.7.2 ITEM(c2<= AND ITEM(c4<=)

create table partition_pruning_multikey_t1(c1 timestamp, c2 text, c3 float, c4 int, c5 int)
partition by range(c1,c2,c3,c4)
(
	partition p0000 values less than ('2003-01-01 00:00:00','AAAA',100.0,100),
	partition p0001 values less than ('2003-01-01 00:00:00','AAAA',100.0,200),
	partition p0002 values less than ('2003-01-01 00:00:00','AAAA',100.0,300),
	partition p0010 values less than ('2003-01-01 00:00:00','AAAA',200.0,100),
	partition p0011 values less than ('2003-01-01 00:00:00','AAAA',200.0,200),
	partition p0012 values less than ('2003-01-01 00:00:00','AAAA',200.0,300),
	partition p0020 values less than ('2003-01-01 00:00:00','AAAA',300.0,100),
	partition p0021 values less than ('2003-01-01 00:00:00','AAAA',300.0,200),
	partition p0022 values less than ('2003-01-01 00:00:00','AAAA',300.0,300),
	partition p0100 values less than ('2003-01-01 00:00:00','DDDD',100.0,100),
	partition p0101 values less than ('2003-01-01 00:00:00','DDDD',100.0,200),
	partition p0102 values less than ('2003-01-01 00:00:00','DDDD',100.0,300),
	partition p0110 values less than ('2003-01-01 00:00:00','DDDD',200.0,100),
	partition p0111 values less than ('2003-01-01 00:00:00','DDDD',200.0,200),
	partition p0112 values less than ('2003-01-01 00:00:00','DDDD',200.0,300),
	partition p0120 values less than ('2003-01-01 00:00:00','DDDD',300.0,100),
	partition p0121 values less than ('2003-01-01 00:00:00','DDDD',300.0,200),
	partition p0122 values less than ('2003-01-01 00:00:00','DDDD',300.0,300),
	partition p0200 values less than ('2003-01-01 00:00:00','HHHH',100.0,100),
	partition p0201 values less than ('2003-01-01 00:00:00','HHHH',100.0,200),
	partition p0202 values less than ('2003-01-01 00:00:00','HHHH',100.0,300),
	partition p0210 values less than ('2003-01-01 00:00:00','HHHH',200.0,100),
	partition p0211 values less than ('2003-01-01 00:00:00','HHHH',200.0,200),
	partition p0212 values less than ('2003-01-01 00:00:00','HHHH',200.0,300),
	partition p0220 values less than ('2003-01-01 00:00:00','HHHH',300.0,100),
	partition p0221 values less than ('2003-01-01 00:00:00','HHHH',300.0,200),
	partition p0222 values less than ('2003-01-01 00:00:00','HHHH',300.0,300),
--27	
	partition p1000 values less than ('2003-04-01 00:00:00','AAAA',100.0,100),
	partition p1001 values less than ('2003-04-01 00:00:00','AAAA',100.0,200),
	partition p1002 values less than ('2003-04-01 00:00:00','AAAA',100.0,300),
	partition p1010 values less than ('2003-04-01 00:00:00','AAAA',200.0,100),
	partition p1011 values less than ('2003-04-01 00:00:00','AAAA',200.0,200),
	partition p1012 values less than ('2003-04-01 00:00:00','AAAA',200.0,300),
	partition p1020 values less than ('2003-04-01 00:00:00','AAAA',300.0,100),
	partition p1021 values less than ('2003-04-01 00:00:00','AAAA',300.0,200),
	partition p1022 values less than ('2003-04-01 00:00:00','AAAA',300.0,300),
	partition p1100 values less than ('2003-04-01 00:00:00','DDDD',100.0,100),
	partition p1101 values less than ('2003-04-01 00:00:00','DDDD',100.0,200),
	partition p1102 values less than ('2003-04-01 00:00:00','DDDD',100.0,300),
	partition p1110 values less than ('2003-04-01 00:00:00','DDDD',300.0,100),
	partition p1111 values less than ('2003-04-01 00:00:00','DDDD',300.0,200),
	partition p1112 values less than ('2003-04-01 00:00:00','DDDD',300.0,300),
	partition p1200 values less than ('2003-04-01 00:00:00','HHHH',100.0,100),
	partition p1201 values less than ('2003-04-01 00:00:00','HHHH',100.0,200),
	partition p1202 values less than ('2003-04-01 00:00:00','HHHH',100.0,300),
	partition p1220 values less than ('2003-04-01 00:00:00','HHHH',300.0,100),
	partition p1221 values less than ('2003-04-01 00:00:00','HHHH',300.0,200),
	partition p1222 values less than ('2003-04-01 00:00:00','HHHH',300.0,300),
	partition p1223 values less than ('2003-04-01 00:00:00','HHHH',MAXVALUE,MAXVALUE),
--49	
	partition p2000 values less than ('2003-07-01 00:00:00','AAAA',100.0,100),
	partition p2001 values less than ('2003-07-01 00:00:00','AAAA',100.0,200),
	partition p2002 values less than ('2003-07-01 00:00:00','AAAA',100.0,300),
	partition p2010 values less than ('2003-07-01 00:00:00','AAAA',200.0,100),
	partition p2011 values less than ('2003-07-01 00:00:00','AAAA',200.0,200),
	partition p2012 values less than ('2003-07-01 00:00:00','AAAA',200.0,300),
	partition p2020 values less than ('2003-07-01 00:00:00','AAAA',300.0,100),
	partition p2021 values less than ('2003-07-01 00:00:00','AAAA',300.0,200),
	partition p2022 values less than ('2003-07-01 00:00:00','AAAA',300.0,300),
	partition p2200 values less than ('2003-07-01 00:00:00','HHHH',100.0,100),
	partition p2201 values less than ('2003-07-01 00:00:00','HHHH',100.0,200),
	partition p2202 values less than ('2003-07-01 00:00:00','HHHH',100.0,300),
	partition p2210 values less than ('2003-07-01 00:00:00','HHHH',200.0,100),
	partition p2211 values less than ('2003-07-01 00:00:00','HHHH',200.0,200),
	partition p2212 values less than ('2003-07-01 00:00:00','HHHH',200.0,300),
	partition p2220 values less than ('2003-07-01 00:00:00','HHHH',300.0,100),
	partition p2221 values less than ('2003-07-01 00:00:00','HHHH',300.0,200),
	partition p2222 values less than ('2003-07-01 00:00:00','HHHH',300.0,300),
	partition p2322 values less than ('2003-07-01 00:00:00',MAXVALUE,300.0,300),
--68	
	partition p3000 values less than ('2003-10-01 00:00:00','AAAA',100.0,100),
	partition p3001 values less than ('2003-10-01 00:00:00','AAAA',100.0,200),
	partition p3002 values less than ('2003-10-01 00:00:00','AAAA',100.0,300),
	partition p3010 values less than ('2003-10-01 00:00:00','AAAA',200.0,100),
	partition p3011 values less than ('2003-10-01 00:00:00','AAAA',200.0,200),
	partition p3012 values less than ('2003-10-01 00:00:00','AAAA',200.0,300),
	partition p3020 values less than ('2003-10-01 00:00:00','AAAA',300.0,100),
	partition p3021 values less than ('2003-10-01 00:00:00','AAAA',300.0,200),
	partition p3022 values less than ('2003-10-01 00:00:00','AAAA',300.0,300),
	partition p3100 values less than ('2003-10-01 00:00:00','DDDD',100.0,100),
	partition p3101 values less than ('2003-10-01 00:00:00','DDDD',100.0,200),
	partition p3102 values less than ('2003-10-01 00:00:00','DDDD',100.0,300),
	partition p3110 values less than ('2003-10-01 00:00:00','DDDD',200.0,100),
	partition p3111 values less than ('2003-10-01 00:00:00','DDDD',200.0,200),
	partition p3112 values less than ('2003-10-01 00:00:00','DDDD',200.0,300),
	partition p3120 values less than ('2003-10-01 00:00:00','DDDD',300.0,100),
	partition p3121 values less than ('2003-10-01 00:00:00','DDDD',300.0,200),
	partition p3122 values less than ('2003-10-01 00:00:00','DDDD',300.0,300),
	partition p3200 values less than ('2003-10-01 00:00:00','HHHH',100.0,100),
	partition p3201 values less than ('2003-10-01 00:00:00','HHHH',100.0,200),
	partition p3202 values less than ('2003-10-01 00:00:00','HHHH',100.0,300),
	partition p3210 values less than ('2003-10-01 00:00:00','HHHH',200.0,100),
	partition p3211 values less than ('2003-10-01 00:00:00','HHHH',200.0,200),
	partition p3212 values less than ('2003-10-01 00:00:00','HHHH',200.0,300),
	partition p3220 values less than ('2003-10-01 00:00:00','HHHH',300.0,100),
	partition p3221 values less than ('2003-10-01 00:00:00','HHHH',300.0,200),
	partition p3222 values less than ('2003-10-01 00:00:00','HHHH',300.0,300),
	partition p3233 values less than ('2003-10-01 00:00:00',MAXVALUE,MAXVALUE,MAXVALUE)
--96
);

--expect:1-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1;

--3.1 expression(c1)
--3.1.1 ITEM
--3.1.1.1  <  -BD(1)
--expect: 1
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<'2002-10-01 00:00:00';

--3.1.1.2  <= -BD(1)
--expect: 1
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2002-10-01 00:00:00';

--3.1.1.3  <   BD(1)
--expect: 1
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<'2003-01-01 00:00:00';

--3.1.1.4  <=  BD(1)
--expect: 1-28
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2003-01-01 00:00:00';

--3.1.1.5  <  -BD(N)
--expect: 1-50
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<'2003-06-01 00:00:00';

--3.1.1.6  <= -BD(N)
--expect: 1-50
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2003-06-01 00:00:00';

--3.1.1.7  <   BD(N)
--expect: 1-50
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<'2003-07-01 00:00:00';

--3.1.1.8  <=  BD(N)
--expect: 1-69
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2003-07-01 00:00:00';

--3.1.1.9  <  -BD(Z)
--expect: 1-69
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<'2003-09-01 00:00:00';

--3.1.1.10 <= -BD(Z)
--expect: 1-69
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2003-09-01 00:00:00';

--3.1.1.11 <   BD(Z)
--expect: 1-69
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<'2003-10-01 00:00:00';

--3.1.1.12 <=  BD(Z)
--expect: 1-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2003-10-01 00:00:00';

--3.1.1.13 <  +BD(Z)
--expect: 1-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<'2004-01-01 00:00:00';

--3.1.1.14 >  -BD(1)
--expect: 1-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2002-10-01 00:00:00';

--3.1.1.15 >= -BD(1)
--expect: 1-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2002-10-01 00:00:00';

--3.1.1.16 >   BD(1)
--expect: 28-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-01-01 00:00:00';

--3.1.1.17 >=  BD(1)
--expect: 1-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-01-01 00:00:00';

--3.1.1.18 >  -BD(N)
--expect: 50-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-06-01 00:00:00';

--3.1.1.19 >= -BD(N)
--expect: 50-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-06-01 00:00:00';

--3.1.1.20 >   BD(N)
--expect: 69-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-07-01 00:00:00';

--3.1.1.21 >=  BD(N)
--expect: 50-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-07-01 00:00:00';

--3.1.1.22 >   BD(Z)
--expect: NONE
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-10-01 00:00:00';

--3.1.1.23 >=  BD(Z)
--expect: 69-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-10-01 00:00:00';

--3.1.1.24 >  +BD(Z)
--expect: NONE
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2004-10-01 00:00:00';

--3.1.1.25 =  -BD(1)
--expect: 1
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1='2002-10-01 00:00:00';

--3.1.1.26 =   BD(1)
--expect: 1-28
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1='2003-01-01 00:00:00';

--3.1.1.27 =  -BD(N)
--expect: 50
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1='2003-06-01 00:00:00';

--3.1.1.28 =   BD(N)
--expect: 50-69
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1='2003-07-01 00:00:00';

--3.1.1.29 =   BD(Z)
--expect: 69-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1='2003-10-01 00:00:00';

--3.1.2 composite
--3.1.2.1  ITEM(<) AND ITEM(<)
--expect: 1-50
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<'2003-09-01 00:00:00' AND c1<='2003-04-01 00:00:00';

--3.1.2.2  ITEM(>) AND ITEM(>)
--expect: 69-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-09-01 00:00:00' AND c1>='2003-04-01 00:00:00';

--3.1.2.3  ITEM(>) AND ITEM(<) (NOT NULL)
--expect: 28-69
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<'2003-09-01 00:00:00' AND c1>='2003-04-01 00:00:00';

--3.1.2.4  ITEM(>) AND ITEM(<) (NULL)
--expect: NONE
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-09-01 00:00:00' AND c1<='2003-04-01 00:00:00';

--expect: 28-50
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-04-01 00:00:00' AND c1<='2003-04-01 00:00:00';

--3.1.2.5  ITEM(=) OR ITEM(<) (NOT NULL)
--expect: 1-50
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1='2003-04-01 00:00:00' OR c1<'2003-06-01 00:00:00';

--3.1.2.6  ITEM(=) OR ITEM(<) (NULL)
--expect: 1-50,69
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1='2003-09-01 00:00:00' OR c1<'2003-06-01 00:00:00';

--3.1.2.7  ITEM(=) OR ITEM(>) (NOT NULL)
--expect: 50-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1='2003-09-01 00:00:00' OR c1>'2003-06-01 00:00:00';

--3.1.2.8  ITEM(=) OR ITEM(>) (NULL)
--expect: 50,69-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-09-01 00:00:00' OR c1='2003-06-01 00:00:00';

--3.1.2.9  ITEM(<) OR ITEM(>) (NOT NULL)
--expect: 1-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2003-10-01 00:00:00' OR c1>'2003-06-01 00:00:00';

--3.1.2.10 ITEM(<) OR ITEM(>) (NULL)
--expect: 1-50,69-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-10-01 00:00:00' OR c1<'2003-06-01 00:00:00';

--3.1.2.11 ITEM(<) AND OTHER
--expect: 69-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-10-01 00:00:00' AND c5 IS NULL;

--3.1.2.12 NOKEY OR ITEM(>)
--expect: 1-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-10-01 00:00:00' OR c5<100;

--3.2 expression(c1,c2)
--3.2.1 >/>=
--3.2.1.1 ITEM(c1>)  AND ITEM(c2>)
--expect: 28 / 37-50 / 59-69 / 78-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-03-01 00:00:00' AND c2>'CCCC';

--expect: 50 / 59-69 / 78-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-04-01 00:00:00' AND c2>'CCCC';

--expect: 50 / 59-69 / 87-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-04-01 00:00:00' AND c2>'DDDD';

--3.2.1.2 ITEM(c1>=) AND ITEM(c2>)
--expect: 43-50 / 59-69 / 87-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-04-01 00:00:00' AND c2>'DDDD';

--3.2.1.3 ITEM(c1>)  AND ITEM(c2>=)
--expect: 50 / 59-69 / 78-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-04-01 00:00:00' AND c2>='DDDD';

--3.2.1.4 ITEM(c1>=) AND ITEM(c2>=)
--expect: 37-50 / 59-69 / 78-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-04-01 00:00:00' AND c2>='DDDD';

--3.2.2 =
--3.2.2.1 ITEM(c1=)  AND ITEM(c2=)
--expect: 37-43
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1='2003-04-01 00:00:00' AND c2='DDDD';

--3.2.3 </<=
--3.2.3.1 ITEM(c1<)  AND ITEM(c2<)
--expect: 1-10 / 28
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<'2003-04-01 00:00:00' AND c2<'DDDD';

--3.2.3.2 ITEM(c1<=) AND ITEM(c2<)
--expect: 1-10 / 28-37
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2003-04-01 00:00:00' AND c2<'DDDD';

--3.2.3.3 ITEM(c1<)  AND ITEM(c2<=)
--expect: 1-19 / 28
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<'2003-04-01 00:00:00' AND c2<='DDDD';

--3.2.3.4 ITEM(c1<=) AND ITEM(c2<=)
--expect: 1-19 / 28-43
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2003-04-01 00:00:00' AND c2<='DDDD';

--3.2.4 composite
--3.2.4.1 ITEM(c1>)   AND ITEM(c2>)  AND ITEM(c1<)   AND ITEM(c2<)
--expect: 50 / 59 / 69
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-04-01 00:00:00' AND c2>'DDDD' AND c1<'2003-10-01 00:00:00' AND c2<'HHHH';

--3.2.4.2 ITEM(c1>=)  AND ITEM(c2>)  AND ITEM(c1<)   AND ITEM(c2<=)
--expect: 43-50 / 59-69
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-04-01 00:00:00' AND c2>'DDDD' AND c1<'2003-10-01 00:00:00' AND c2<='HHHH';

--3.2.4.3 ITEM(c1>=)  AND ITEM(c2>=) AND ITEM(c1<=)  AND ITEM(c2<=)
--expect: 37-50 / 59-69 / 78-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-04-01 00:00:00' AND c2>='DDDD' AND c1<='2003-10-01 00:00:00' AND c2<='HHHH';

--3.2.4.4 (ITEM(c1>)   AND ITEM(c2>))  OR (ITEM(c1<)   AND ITEM(c2<))
--expect: 1-10 / 28 / 69 / 96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where (c1>'2003-07-01 00:00:00' AND c2>'HHHH') OR (c1<'2003-04-01 00:00:00' AND c2<'DDDD');

--3.2.4.5 (ITEM(c1>=)  AND ITEM(c2>))  OR (ITEM(c1<)   AND ITEM(c2<=))
--expect: 1-19 / 28 / 59-69 / 78-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where (c1>='2003-07-01 00:00:00' AND c2>'BBBB') OR (c1<'2003-04-01 00:00:00' AND c2<='DDDD');

--3.2.4.6 (ITEM(c1>=)  AND ITEM(c2>=)) OR (ITEM(c1<=)  AND ITEM(c2<=)) 
--expect: 1-19 / 28-43 / 59-69 / 78-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where (c1>='2003-07-01 00:00:00' AND c2>='BBBB') OR (c1<='2003-04-01 00:00:00' AND c2<='DDDD');

--3.3 expression(c1,c2,c3)
--3.3.1 >/>=
--3.3.1.1 ITEM(c1>)  AND ITEM(c2>)  AND ITEM(c3>)
--expect: 50 / 59-69 / 87-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-04-01 00:00:00' AND c2>'DDDD' AND c3>50.0;

--3.3.1.2 ITEM(c1>=) AND ITEM(c2>)  AND ITEM(c3>)
--expect: 43-50 / 59-69 / 87-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-04-01 00:00:00' AND c2>'DDDD' AND c3>50.0;

--3.3.1.3 ITEM(c1>=)  AND ITEM(c2>=) AND ITEM(c3>)
--expect: 37 / 40-43 / 46-50 / 59 / 62-69 / 78 / 81-87 / 90-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-04-01 00:00:00' AND c2>='DDDD' AND c3>100.0;

--3.3.1.4 ITEM(c1>=) AND ITEM(c2>=) AND ITEM(c3>=)
--expect: 37-50 / 59-69 / 78-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-04-01 00:00:00' AND c2>='DDDD' AND c3>=100.0;

--3.3.2 =
--3.3.2.1 ITEM(c1=)  AND ITEM(c2=)  AND ITEM(c3=)
--expect: 40-43
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1='2003-04-01 00:00:00' AND c2='DDDD' AND c3=300.0;

--3.3.3 </<=
--3.3.3.1 ITEM(c1<)  AND ITEM(c2<)  AND ITEM(c3<)
--expect: 1 / 10 / 28
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<'2003-04-01 00:00:00' AND c2<'DDDD' AND c3<100.0;

--3.3.3.2 ITEM(c1<=) AND ITEM(c2<)  AND ITEM(c3<)
--NA

--3.3.3.3 ITEM(c1<=)  AND ITEM(c2<=) AND ITEM(c3<)
--expect: 1 / 10 / 19 / 28 / 37
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2003-04-01 00:00:00' AND c2<='DDDD' AND c3<100.0;

--3.3.3.4 ITEM(c1<=) AND ITEM(c2<=) AND ITEM(c3<=)
--expect: 1-4 / 10-13 / 19 / 28-29 / 30-31 / 37-40
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2003-04-01 00:00:00' AND c2<='DDDD' AND c3<=100.0;

--3.3.4 composite
--3.3.4.1 ITEM(c1>)   AND ITEM(c2>) AND ITEM(c3>) AND ITEM(c1<)   AND ITEM(c2<)
--NA

--3.3.4.2 ITEM(c1>=)  AND ITEM(c2>) AND ITEM(c3>) AND ITEM(c1<)   AND ITEM(c2<=)
--NA

--3.3.4.3 ITEM(c1>=)  AND ITEM(c2>=) AND ITEM(c3>=) AND ITEM(c1<=)  AND ITEM(c2<=)
--expect: 37-43 / 50 / 59
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-04-01 00:00:00' AND c2>='DDDD' AND c3>=100.0 AND c1<='2003-07-01 00:00:00' AND c2<='DDDD';

--3.3.4.4 (ITEM(c1>)   AND ITEM(c2>) AND ITEM(c3>))  OR (ITEM(c1<)   AND ITEM(c2<))
--NA

--3.3.4.5 (ITEM(c1>=)  AND ITEM(c2>) AND ITEM(c3>))  OR (ITEM(c1<)   AND ITEM(c2<=))
--NA

--3.3.4.6 (ITEM(c1>=)  AND ITEM(c2>=) AND ITEM(c3>=)) OR (ITEM(c1<=)  AND ITEM(c2<=)) 
--expect: 1-4 / 10-13 / 19 / 28-31 / 37-40 / 50-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where (c1<='2003-04-01 00:00:00' AND c2<='DDDD' AND c3<=100.0) OR (c1>='2003-07-01 00:00:00' AND c2>='AAAA');

--3.4 expression(c1,c2,c3,c4)
--3.4.1 >/>=
--3.4.1.1 ITEM(c1>)  AND ITEM(c2>)  AND ITEM(c3>) AND ITEM(c4>)
--expect: 50 / 59 / 62 / 64-65 / 67-69 / 87 / 90 / 92-93 / 95-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>'2003-04-01 00:00:00' AND c2>'DDDD' AND c3>100.0 AND c4>200;

--3.4.1.2 ITEM(c1>=) AND ITEM(c2>)  AND ITEM(c3>) AND ITEM(c4>)
--NA

--3.4.1.3 ITEM(c1>=)  AND ITEM(c2>=) AND ITEM(c3>=) AND ITEM(c4>)
--expect: 39-40 / 42-43 / 45-46 / 48-50 / 59 / 61-62 / 64-65 / 67-69 / 78 / 80-81 / 83-84 / 86-87 / 89-90 / 92-93 / 95-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-04-01 00:00:00' AND c2>='DDDD' AND c3>=100.0 AND c4>200;

--3.4.1.4 ITEM(c1>=) AND ITEM(c2>=) AND ITEM(c3>=) AND ITEM(c4>=)
--expect: 39-50 / 59-69 / 78-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-04-01 00:00:00' AND c2>='DDDD' AND c3>=100.0 AND c4>=200;

--3.4.2 =
--3.4.2.1 ITEM(c1=)  AND ITEM(c2=)  AND ITEM(c3=) AND ITEM(c4=)
--expect: 39
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1='2003-04-01 00:00:00' AND c2='DDDD' AND c3=100.0 AND c4=200;

--3.4.3 </<=
--3.4.3.1 ITEM(c1<)  AND ITEM(c2<)  AND ITEM(c3<) AND ITEM(c4<)
--NA

--3.4.3.2 ITEM(c1<=) AND ITEM(c2<)  AND ITEM(c3<) AND ITEM(c4<)
--NA

--3.4.3.3 ITEM(c1<=)  AND ITEM(c2<=) AND ITEM(c3<=) AND ITEM(c4<)
--expect: 1-2 / 4 / 10-11 / 13 / 19 / 28-29 / 31 / 37-38
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2003-04-01 00:00:00' AND c2<='DDDD' AND c3<=100.0 AND c4<200;

--3.4.3.4 ITEM(c1<=) AND ITEM(c2<=) AND ITEM(c3<=) AND ITEM(c4<=)
--expect: 1-3 / 10-12 / 19 / 28-30 / 37-39 (4, 13, 31 should be eliminated)
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2003-04-01 00:00:00' AND c2<='DDDD' AND c3<=100.0 AND c4<=200;

--3.4.4 composite
--3.4.4.1 ITEM(c1>)   AND ITEM(c2>) AND ITEM(c3>) AND ITEM(c4>) AND ITEM(c1<)   AND ITEM(c2<)
--NA

--3.4.4.2 ITEM(c1>=)  AND ITEM(c2>) AND ITEM(c3>) AND ITEM(c4>) AND ITEM(c1<)   AND ITEM(c2<=)
--NA

--3.4.4.3 ITEM(c1>=)  AND ITEM(c2>=) AND ITEM(c3>=) AND ITEM(c4>=) AND ITEM(c1<=)  AND ITEM(c2<=)
--expect: 39-43 / 50 / 59
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 
where c1>='2003-04-01 00:00:00' AND c2>='DDDD' AND c3>=100.0 AND c4>=200
	AND c1<='2003-07-01 00:00:00' AND c2<='DDDD';

--3.4.4.4 (ITEM(c1>)   AND ITEM(c2>) AND ITEM(c3>) AND ITEM(c4>))  OR (ITEM(c1<)   AND ITEM(c2<))
--NA

--3.4.4.5 (ITEM(c1>)  AND ITEM(c2>=) AND ITEM(c3>) AND ITEM(c4>))  OR (ITEM(c1<)   AND ITEM(c2<=))
--NA

--3.4.4.6 (ITEM(c1>=)  AND ITEM(c2>=) AND ITEM(c3>=) AND ITEM(c4>=)) OR (ITEM(c1<=)  AND ITEM(c2<=)) 
--expect: 1-10 / 28-37 / 59-69 / 78-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 
where (c1>='2003-07-01 00:00:00' AND c2>='DDDD' AND c3>=100.0 AND c4>=200)
	OR (c1<='2003-04-01 00:00:00' AND c2<'DDDD');
	
--3.5 expression(c1,c3)
--3.5.1 >/>=
--3.5.1.1 ITEM(c1>=)  AND ITEM(c3>)
--expect: 50-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-07-01 00:00:00' AND c3>=100.0;
	
--3.5.2 =
--3.5.2.1 ITEM(c1=)  AND ITEM(c3=)
--expect: 50-53 / 59-62 / 68
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1='2003-07-01 00:00:00' AND c3=100.0;

--3.5.3 </<=
--3.5.3.1 ITEM(c1<=) AND ITEM(c3<=)
--expect: 1-4 / 10-13 / 19-22 / 28-31 / 37-40 / 43-46 / 50-53 / 59-62 / 68
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1<='2003-07-01 00:00:00' AND c3<=100.0;

--3.5.4 composite
--3.5.4.1 ITEM(c1>)   AND ITEM(c3>)  AND ITEM(c1<)   AND ITEM(c2<)
--expect: 50-59 / 69
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c1>='2003-07-01 00:00:00' AND c3>=100.0 AND  c1<'2003-10-01 00:00:00' AND c2<='DDDD';

--3.6 expression(c2)
--3.6.1 ITEM(c2>)
--expect: 1 / 19-28 / 43-50 / 59-69 / 87-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c2>'DDDD';

--3.6.2 ITEM(c2<)
--expect: 1-10 / 28-37 / 50-59 / 69-78
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c2<'DDDD';

--3.6.3 ITEM(c2>=) OR (ITEM(c1>)   AND ITEM(c2>))
--expect: 1 / 19-28 / 43-50 / 59-69 / 87-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c2>'DDDD' OR (c1>'2003-04-01 00:00:00' AND c2>='HHHH');

--3.7 expression(c2,c4)
--3.7.1 ITEM(c2>) AND ITEM(c4>)
--expect: 1 / 19 / 21-22 / 24-25 / 27-28 / 43 / 45-46 / 48-50 / 59 / 61-62 / 64-65 / 67-69 / 87 / 89-90 / 92-93 / 95-96
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c2>'DDDD' AND c4>200;

--3.7.2 ITEM(c2<= AND ITEM(c4<=)
--expect: 1-19 / 28-43 / 50-59 / 69-87
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from partition_pruning_multikey_t1 where c2<='DDDD' AND c4<=200;

drop table partition_pruning_multikey_t1;