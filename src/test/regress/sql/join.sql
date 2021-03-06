--
-- JOIN
-- Test JOIN clauses
--

CREATE TABLE J1_TBL (
  i integer,
  j integer,
  t text
);

CREATE TABLE J2_TBL (
  i integer,
  k integer
);


INSERT INTO J1_TBL VALUES (1, 4, 'one');
INSERT INTO J1_TBL VALUES (2, 3, 'two');
INSERT INTO J1_TBL VALUES (3, 2, 'three');
INSERT INTO J1_TBL VALUES (4, 1, 'four');
INSERT INTO J1_TBL VALUES (5, 0, 'five');
INSERT INTO J1_TBL VALUES (6, 6, 'six');
INSERT INTO J1_TBL VALUES (7, 7, 'seven');
INSERT INTO J1_TBL VALUES (8, 8, 'eight');
INSERT INTO J1_TBL VALUES (0, NULL, 'zero');
INSERT INTO J1_TBL VALUES (NULL, NULL, 'null');
INSERT INTO J1_TBL VALUES (NULL, 0, 'zero');

INSERT INTO J2_TBL VALUES (1, -1);
INSERT INTO J2_TBL VALUES (2, 2);
INSERT INTO J2_TBL VALUES (3, -3);
INSERT INTO J2_TBL VALUES (2, 4);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (0, NULL);
INSERT INTO J2_TBL VALUES (NULL, NULL);
INSERT INTO J2_TBL VALUES (NULL, 0);

--
-- CORRELATION NAMES
-- Make sure that table/column aliases are supported
-- before diving into more complex join syntax.
--

SELECT '' AS "xxx", *
  FROM J1_TBL AS tx 
  ORDER BY i, j, t;

SELECT '' AS "xxx", *
  FROM J1_TBL tx 
  ORDER BY i, j, t;

SELECT '' AS "xxx", *
  FROM J1_TBL AS t1 (a, b, c) 
  ORDER BY a, b, c;

SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c)
  ORDER BY a, b, c;

SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c), J2_TBL t2 (d, e) 
  ORDER BY a, b, c, d, e;

SELECT '' AS "xxx", t1.a, t2.e
  FROM J1_TBL t1 (a, b, c), J2_TBL t2 (d, e)
  WHERE t1.a = t2.d
  ORDER BY a, e;


--
-- CROSS JOIN
-- Qualifications are not allowed on cross joins,
-- which degenerate into a standard unqualified inner join.
--

SELECT '' AS "xxx", *
  FROM J1_TBL CROSS JOIN J2_TBL
  ORDER BY J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k;

-- ambiguous column
SELECT '' AS "xxx", i, k, t
  FROM J1_TBL CROSS JOIN J2_TBL;

-- resolve previous ambiguity by specifying the table name
SELECT '' AS "xxx", t1.i, k, t
  FROM J1_TBL t1 CROSS JOIN J2_TBL t2
  ORDER BY i, k, t;

SELECT '' AS "xxx", ii, tt, kk
  FROM (J1_TBL CROSS JOIN J2_TBL)
    AS tx (ii, jj, tt, ii2, kk)
    ORDER BY ii, tt, kk;

SELECT '' AS "xxx", tx.ii, tx.jj, tx.kk
  FROM (J1_TBL t1 (a, b, c) CROSS JOIN J2_TBL t2 (d, e))
    AS tx (ii, jj, tt, ii2, kk)
    ORDER BY ii, jj, kk;

SELECT '' AS "xxx", *
  FROM J1_TBL CROSS JOIN J2_TBL a CROSS JOIN J2_TBL b
  ORDER BY J1_TBL.i,J1_TBL.j,J1_TBL.t,a.i,a.k,b.i,b.k;


--
--
-- Inner joins (equi-joins)
--
--

--
-- Inner joins (equi-joins) with USING clause
-- The USING syntax changes the shape of the resulting table
-- by including a column in the USING clause only once in the result.
--

-- Inner equi-join on specified column
SELECT '' AS "xxx", *
  FROM J1_TBL INNER JOIN J2_TBL USING (i)
  ORDER BY i, j, k, t;

-- Same as above, slightly different syntax
SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL USING (i)
  ORDER BY i, j, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c) JOIN J2_TBL t2 (a, d) USING (a)
  ORDER BY a, d;

SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c) JOIN J2_TBL t2 (a, b) USING (b)
  ORDER BY b, t1.a;


--
-- NATURAL JOIN
-- Inner equi-join on all columns with the same name
--

SELECT '' AS "xxx", *
  FROM J1_TBL NATURAL JOIN J2_TBL
  ORDER BY i, j, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c) NATURAL JOIN J2_TBL t2 (a, d)
  ORDER BY a, b, c, d;

SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c) NATURAL JOIN J2_TBL t2 (d, a)
  ORDER BY a, b, c, d;

-- mismatch number of columns
-- currently, Postgres will fill in with underlying names
SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b) NATURAL JOIN J2_TBL t2 (a)
  ORDER BY a, b, t, k;


--
-- Inner joins (equi-joins)
--

SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.i)
  ORDER BY J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k;

SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.k)
  ORDER BY J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k;


--
-- Non-equi-joins
--

SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i <= J2_TBL.k)
  ORDER BY J1_TBL.i, J1_TBL.j, J1_TBL.t, J2_TBL.i, J2_TBL.k;


--
-- Outer joins
-- Note that OUTER is a noise word
--

SELECT '' AS "xxx", *
  FROM J1_TBL LEFT OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL RIGHT OUTER JOIN J2_TBL USING (i)
  ORDER BY i, j, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL RIGHT JOIN J2_TBL USING (i)
  ORDER BY i, j, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL FULL OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL FULL JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT '' AS "xxx", *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (k = 1);

SELECT '' AS "xxx", *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (i = 1);


--
-- More complicated constructs
--

--
-- Multiway full join
--

CREATE TABLE t1 (name TEXT, n INTEGER);
CREATE TABLE t2 (name TEXT, n INTEGER);
CREATE TABLE t3 (name TEXT, n INTEGER);

INSERT INTO t1 VALUES ( 'bb', 11 );
INSERT INTO t2 VALUES ( 'bb', 12 );
INSERT INTO t2 VALUES ( 'cc', 22 );
INSERT INTO t2 VALUES ( 'ee', 42 );
INSERT INTO t3 VALUES ( 'bb', 13 );
INSERT INTO t3 VALUES ( 'cc', 23 );
INSERT INTO t3 VALUES ( 'dd', 33 );

SELECT * FROM t1 FULL JOIN t2 USING (name) FULL JOIN t3 USING (name) 
ORDER BY name,t1.n, t2.n, t3.n;

--
-- Test interactions of join syntax and subqueries
--

-- Basic cases (we expect planner to pull up the subquery here)
SELECT * FROM
(SELECT * FROM t2) as s2
INNER JOIN
(SELECT * FROM t3) s3
USING (name)
ORDER BY name, s2.n, s3.n;

SELECT * FROM
(SELECT * FROM t2) as s2
LEFT JOIN
(SELECT * FROM t3) s3
USING (name)
ORDER BY name, s2.n, s3.n;

SELECT * FROM
(SELECT * FROM t2) as s2
FULL JOIN
(SELECT * FROM t3) s3
USING (name)
ORDER BY name, s2.n, s3.n;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3
ORDER BY name, s2_n, s3_n;

SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL LEFT JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3
ORDER BY name, s2_n, s3_n;

SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3
ORDER BY name, s2_n, s3_n;

SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t1) as s1
NATURAL INNER JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3
ORDER BY name, s1_n, s2_n, s3_n;

SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t1) as s1
NATURAL FULL JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3
ORDER BY name, s1_n, s2_n, s3_n;

SELECT * FROM
(SELECT name, n as s1_n FROM t1) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n FROM t2) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t3) as s3
  ) ss2
  ORDER BY name, s1_n, s2_n, s3_n;

SELECT * FROM
(SELECT name, n as s1_n FROM t1) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t3) as s3
  ) ss2
  ORDER BY name, s1_n, s2_n, s3_n;


-- Test for propagation of nullability constraints into sub-joins

SET enforce_two_phase_commit TO off;

create temp table x (x1 int, x2 int);
insert into x values (1,11);
insert into x values (2,22);
insert into x values (3,null);
insert into x values (4,44);
insert into x values (5,null);

create temp table y (y1 int, y2 int);
insert into y values (1,111);
insert into y values (2,222);
insert into y values (3,333);
insert into y values (4,null);

select * from x ORDER BY x1;
select * from y ORDER BY y1;

select * from x left join y on (x1 = y1 and x2 is not null) ORDER BY x1, x2, y1, y2;
select * from x left join y on (x1 = y1 and y2 is not null) ORDER BY x1, x2, y1, y2;

select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) ORDER BY x1, x2, y1, y2;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and x2 is not null) ORDER BY x1, x2, y1, y2;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and y2 is not null) ORDER BY x1, x2, y1, y2;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and xx2 is not null) ORDER BY x1, x2, y1, y2;
-- these should NOT give the same answers as above
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (x2 is not null)
ORDER BY x1, x2, y1, y2;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (y2 is not null)
ORDER BY x1, x2, y1, y2;
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (xx2 is not null)
ORDER BY x1, x2, y1, y2;

--
-- regression test: check for bug with propagation of implied equality
-- to outside an IN
--
analyze tenk1;		-- ensure we get consistent plans here

select count(*) from tenk1 a where unique1 in
  (select unique1 from tenk1 b join tenk1 c using (unique1)
   where b.unique2 = 42);

--
-- regression test: check for failure to generate a plan with multiple
-- degenerate IN clauses
--
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);

-- try that with GEQO too
start transaction;
set geqo = on;
set geqo_threshold = 2;
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);
rollback;

--
-- regression test: be sure we cope with proven-dummy append rels
--
explain (costs off)
select x1, x2, unique1, unique1
  from tenk1 right join (select x1, x2 from x union all select x1, x2 from x)x on x2 = unique1
  where x1 < x1 and x1 is null;

select x1, x2, unique1, unique1
  from tenk1 right join (select x1, x2 from x union all select x1, x2 from x)x on x2 = unique1
  where x1 < x1 and x1 is null;

--
-- regression test: check a case where join_clause_is_movable_into() gives
-- an imprecise result
--
select anname, outname
from
  (select pa.proname as anname, coalesce(po.proname, typname) as outname
   from pg_type t
     left join pg_proc po on po.oid = t.typoutput
     join pg_proc pa on pa.oid = t.typanalyze) ss,
  pgxc_node,
  pg_type t2
where anname = node_host and outname = t2.typname and node_id = t2.oid;

--
-- regression test: check a case where we formerly missed including an EC
-- enforcement clause because it was expected to be handled at scan level
--
create table int4_tbl_rep   as select * from int4_tbl;
analyze int4_tbl_rep;
create index unique12 on tenk1(unique1, unique2);
explain verbose 
select a.f1, b.f1, t.unique1, t.unique2 from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl_rep i4a) a,
  (select sum(f1) as f1 from int4_tbl_rep i4b) b
where b.f1 = t.unique2 and a.f1 = b.f1 and (a.f1+b.f1+999) = t.unique1;

select a.f1, b.f1, t.unique1, t.unique2 from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl_rep i4a) a,
  (select sum(f1) as f1 from int4_tbl_rep i4b) b
where b.f1 = t.unique2 and a.f1 = b.f1 and (a.f1+b.f1+999) = t.unique1;

--
-- check a case where we formerly got confused by conflicting sort orders
-- in redundant merge join path keys
--
explain (costs off)
select * from
  j1_tbl full join
  (select * from j2_tbl order by j2_tbl.i desc, j2_tbl.k asc) j2_tbl
  on j1_tbl.i = j2_tbl.i and j1_tbl.i = j2_tbl.k;

--
-- a different check for handling of redundant sort keys in merge joins
--
explain (costs off)
select count(*) from
  (select * from tenk1 x order by x.thousand, x.twothousand, x.fivethous) x
  left join
  (select * from tenk1 y order by y.unique2) y
  on x.thousand = y.unique2 and x.twothousand = y.hundred and x.fivethous = y.unique2;

select count(*) from
  (select * from tenk1 x order by x.thousand, x.twothousand, x.fivethous) x
  left join
  (select * from tenk1 y order by y.unique2) y
  on x.thousand = y.unique2 and x.twothousand = y.hundred and x.fivethous = y.unique2;

drop index unique12;
drop table int4_tbl_rep;

--
-- Clean up
--

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

DROP TABLE J1_TBL;
DROP TABLE J2_TBL;

-- Both DELETE and UPDATE allow the specification of additional tables
-- to "join" against to determine which rows should be modified.

CREATE TEMP TABLE t1 (a int, b int);
CREATE TEMP TABLE t2 (a int, b int);
CREATE TEMP TABLE t3 (x int, y int);

INSERT INTO t1 VALUES (5, 10);
INSERT INTO t1 VALUES (15, 20);
INSERT INTO t1 VALUES (100, 100);
INSERT INTO t1 VALUES (200, 1000);
INSERT INTO t2 VALUES (200, 2000);
INSERT INTO t3 VALUES (5, 20);
INSERT INTO t3 VALUES (6, 7);
INSERT INTO t3 VALUES (7, 8);
INSERT INTO t3 VALUES (500, 100);

DELETE FROM t3 USING t1 table1 WHERE t3.x = table1.a;
SELECT * FROM t3 ORDER By x, y;
DELETE FROM t3 USING t1 JOIN t2 USING (a) WHERE t3.x > t1.a;
SELECT * FROM t3 ORDER By x, y;
DELETE FROM t3 USING t3 t3_other WHERE t3.x = t3_other.x AND t3.y = t3_other.y;
SELECT * FROM t3 ORDER By x, y;

-- Test join against inheritance tree

create temp table t2a () inherits (t2);

insert into t2a values (200, 2001);

select * from t1 left join t2 on (t1.a = t2.a) order by 1,2,3,4;

--
-- regression test for 8.1 merge right join bug
--

CREATE TEMP TABLE tt1 ( tt1_id int4, joincol int4 );
INSERT INTO tt1 VALUES (1, 11);
INSERT INTO tt1 VALUES (2, NULL);

CREATE TEMP TABLE tt2 ( tt2_id int4, joincol int4 );
INSERT INTO tt2 VALUES (21, 11);
INSERT INTO tt2 VALUES (22, 11);

set enable_hashjoin to off;
set enable_nestloop to off;

-- these should give the same results

select tt1.*, tt2.* from tt1 left join tt2 on tt1.joincol = tt2.joincol 
      ORDER BY tt1_id, tt2_id; 

select tt1.*, tt2.* from tt2 right join tt1 on tt1.joincol = tt2.joincol 
      ORDER BY tt1_id, tt2_id; 

reset enable_hashjoin;
reset enable_nestloop;

--
-- regression test for 8.2 bug with improper re-ordering of left joins
--

create temp table tt3(f1 int, f2 text);
insert into tt3 select x, repeat('xyzzy', 100) from generate_series(1,1000) x;
create index tt3i on tt3(f1);
analyze tt3;

create temp table tt4(f1 int);
insert into tt4 values (0),(1),(999);
analyze tt4;

SELECT a.f1
FROM tt4 a
LEFT JOIN (
        SELECT b.f1
        FROM tt3 b LEFT JOIN tt3 c ON (b.f1 = c.f1)
        WHERE c.f1 IS NULL
) AS d ON (a.f1 = d.f1)
WHERE d.f1 IS NULL ORDER BY f1;

--
-- regression test for problems of the sort depicted in bug #3494
--

create temp table tt5(f1 int, f2 int);
create temp table tt6(f1 int, f2 int);

insert into tt5 values(1, 10);
insert into tt5 values(1, 11);

insert into tt6 values(1, 9);
insert into tt6 values(1, 2);
insert into tt6 values(2, 9);

select * from tt5,tt6 where tt5.f1 = tt6.f1 and tt5.f1 = tt5.f2 - tt6.f2 
      ORDER BY tt5.f1, tt5.f2, tt6.f1, tt6.f2;

--
-- regression test for problems of the sort depicted in bug #3588
--

create temp table xx (pkxx int);
create temp table yy (pkyy int, pkxx int);

insert into xx values (1);
insert into xx values (2);
insert into xx values (3);

insert into yy values (101, 1);
insert into yy values (201, 2);
insert into yy values (301, NULL);

select yy.pkyy as yy_pkyy, yy.pkxx as yy_pkxx, yya.pkyy as yya_pkyy,
       xxa.pkxx as xxa_pkxx, xxb.pkxx as xxb_pkxx
from yy
     left join (SELECT * FROM yy where pkyy = 101) as yya ON yy.pkyy = yya.pkyy
     left join xx xxa on yya.pkxx = xxa.pkxx
     left join xx xxb on coalesce (xxa.pkxx, 1) = xxb.pkxx 
     ORDER BY yy_pkyy, yy_pkxx, yya_pkyy, xxa_pkxx, xxb_pkxx;

--
-- regression test for improper pushing of constants across outer-join clauses
-- (as seen in early 8.2.x releases)
--

create temp table zt1 (f1 int primary key);
create temp table zt2 (f2 int primary key);
create temp table zt3 (f3 int primary key);
insert into zt1 values(53);
insert into zt2 values(53);

select * from
  zt2 left join zt3 on (f2 = f3)
      left join zt1 on (f3 = f1)
where f2 = 53 
ORDER BY f1, f2, f3;

create temp view zv1 as select *,'dummy'::text AS junk from zt1;

select * from
  zt2 left join zt3 on (f2 = f3)
      left join zv1 on (f3 = f1)
where f2 = 53 
ORDER BY f1, f2, f3;

--
-- regression test for improper extraction of OR indexqual conditions
-- (as seen in early 8.3.x releases)
--

select a.unique2, a.ten, b.tenthous, b.unique2, b.hundred
from tenk1 a left join tenk1 b on a.unique2 = b.tenthous
where a.unique1 = 42 and
      ((b.unique2 is null and a.ten = 2) or b.hundred = 3);

--
-- test proper positioning of one-time quals in EXISTS (8.4devel bug)
--
prepare foo(bool) as
  select count(*) from tenk1 a left join tenk1 b
    on (a.unique2 = b.unique1 and exists
        (select 1 from tenk1 c where c.thousand = b.unique2 and $1));
execute foo(true);
execute foo(false);

--
-- test for sane behavior with noncanonical merge clauses, per bug #4926
--

start transaction;

set enable_mergejoin = 1;
set enable_hashjoin = 0;
set enable_nestloop = 0;

create temp table a (i integer);
create temp table b (x integer, y integer);

select * from a left join b on i = x and i = y and x = i;

rollback;

--
-- test NULL behavior of whole-row Vars, per bug #5025
--
select t1.q2, count(t2.*)
from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl offset 0) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

select t1.q2, count(t2.*)
from int8_tbl t1 left join
  (select q1, case when q2=1 then 1 else q2 end as q2 from int8_tbl) t2
  on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--
-- test incorrect failure to NULL pulled-up subexpressions
--
start transaction;

create temp table a (
     code char not null,
     constraint a_pk primary key (code)
);
create temp table b (
     a char not null,
     num integer not null,
     constraint b_pk primary key (a, num)
);
create temp table c (
     name char not null,
     a char,
     constraint c_pk primary key (name)
);

insert into a (code) values ('p');
insert into a (code) values ('q');
insert into b (a, num) values ('p', 1);
insert into b (a, num) values ('p', 2);
insert into c (name, a) values ('A', 'p');
insert into c (name, a) values ('B', 'q');
insert into c (name, a) values ('C', null);

select c.name, ss.code, ss.b_cnt, ss.const
from c left join
  (select a.code, coalesce(b_grp.cnt, 0) as b_cnt, -1 as const
   from a left join
     (select count(1) as cnt, b.a from b group by b.a) as b_grp
     on a.code = b_grp.a
  ) as ss
  on (c.a = ss.code)
order by c.name;

rollback;

--
-- test incorrect handling of placeholders that only appear in targetlists,
-- per bug #6154
--
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, sub4.value2, COALESCE(sub4.value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

-- test the path using join aliases, too
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, value2, COALESCE(value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

--
-- test case where a PlaceHolderVar is used as a nestloop parameter
--

EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
SELECT qq, unique1
  FROM
  ( SELECT COALESCE(q1, 0) AS qq FROM int8_tbl a ) AS ss1
  FULL OUTER JOIN
  ( SELECT COALESCE(q2, -1) AS qq FROM int8_tbl b ) AS ss2
  USING (qq)
  INNER JOIN tenk1 c ON qq = unique2;

SELECT qq, unique1
  FROM
  ( SELECT COALESCE(q1, 0) AS qq FROM int8_tbl a ) AS ss1
  FULL OUTER JOIN
  ( SELECT COALESCE(q2, -1) AS qq FROM int8_tbl b ) AS ss2
  USING (qq)
  INNER JOIN tenk1 c ON qq = unique2 order by qq;

--
-- test case where a PlaceHolderVar is propagated into a subquery
--

explain (num_costs off)
select * from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 1,2;

select * from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 1,2;

--
-- test the corner cases FULL JOIN ON TRUE and FULL JOIN ON FALSE
--
select * from int4_tbl a full join int4_tbl b on true order by 1,2;
select * from int4_tbl a full join int4_tbl b on false order by 1,2;

--
-- test for ability to use a cartesian join when necessary
--

explain (num_costs off)
select * from
  tenk1 join int4_tbl on f1 = twothousand,
  int4(sin(1)) q1,
  int4(sin(0)) q2
where q1 = thousand or q2 = thousand;

explain (num_costs off)
select * from
  tenk1 join int4_tbl on f1 = twothousand,
  int4(sin(1)) q1,
  int4(sin(0)) q2
where thousand = (q1 + q2);

--
-- test ability to generate a suitable plan for a star-schema query
--
explain (costs off)
select * from
  tenk1, int8_tbl a, int8_tbl b
where thousand = a.q1 and tenthous = b.q1 and a.q2 = 1 and b.q2 = 2;

--
-- test a corner case in which we shouldn't apply the star-schema optimization
--
explain (costs off)
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (values(1,0)) v1(x1,x2)
               left join (values(3,1)) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (values(1,0)) v1(x1,x2)
               left join (values(3,1)) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

--
-- test placement of movable quals in a parameterized join tree
--

explain (num_costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten = t3.ten
where t1.unique1 = 1;

explain (num_costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten + t2.ten = t3.ten
where t1.unique1 = 1;

explain (num_costs off)
select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

explain (num_costs off)
select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

--
-- test handling of potential equivalence clauses above outer joins
--

explain (num_costs off)
select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

explain (num_costs off)
select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

--
-- test successful handling of full join underneath left join (bug #14105)
--

explain (costs off)
select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

--
-- test ability to push constants through outer join clauses
--

explain (num_costs off)
  select * from int4_tbl a left join tenk1 b on f1 = unique2 where f1 = 0;

explain (num_costs off)
  select * from tenk1 a full join tenk1 b using(unique2) where unique2 = 42;

--
-- test join removal
--

start transaction;

CREATE TEMP TABLE a (id int PRIMARY KEY, b_id int);
CREATE TEMP TABLE b (id int PRIMARY KEY, c_id int);
CREATE TEMP TABLE c (id int PRIMARY KEY);
CREATE TEMP TABLE d (a int, b int);
INSERT INTO a VALUES (0, 0), (1, NULL);
INSERT INTO b VALUES (0, 0), (1, NULL);
INSERT INTO c VALUES (0), (1);
INSERT INTO d VALUES (1,3), (2,2), (3,1);

-- all three cases should be optimizable into a simple seqscan
explain (verbose on, costs off) SELECT a.* FROM a LEFT JOIN b ON a.b_id = b.id;
explain (verbose on, costs off) SELECT b.* FROM b LEFT JOIN c ON b.c_id = c.id;
explain (verbose on, costs off)
  SELECT a.* FROM a LEFT JOIN (b left join c on b.c_id = c.id)
  ON (a.b_id = b.id);

-- check optimization of outer join within another special join
explain (verbose on, costs off)
select id from a where id in (
	select b.id from b left join c on b.id = c.id
);

-- check that join removal works for a left join when joining a subquery
-- that is guaranteed to be unique by its GROUP BY clause
explain (verbose on, costs off)
select d.* from d left join (select * from b group by b.id, b.c_id) s
  on d.a = s.id and d.b = s.c_id;

-- similarly, but keying off a DISTINCT clause
explain (verbose on, costs off)
select d.* from d left join (select distinct * from b) s
  on d.a = s.id and d.b = s.c_id;

-- join removal is not possible when the GROUP BY contains a column that is
-- not in the join condition
explain (verbose on, costs off)
select d.* from d left join (select * from b group by b.id, b.c_id) s
  on d.a = s.id;

-- similarly, but keying off a DISTINCT clause
explain (verbose on, costs off)
select d.* from d left join (select distinct * from b) s
  on d.a = s.id;

-- check join removal works when uniqueness of the join condition is enforced
-- by a UNION
explain (verbose on, costs off)
select d.* from d left join (select id from a union select id from b) s
  on d.a = s.id;

-- check join removal with a cross-type comparison operator
explain (verbose on, costs off)
select i8.* from int8_tbl i8 left join (select f1 from int4_tbl group by f1) i4
  on i8.q1 = i4.f1;


rollback;

create temp table parent (k int primary key, pd int);
create temp table child (k int unique, cd int);
insert into parent values (1, 10), (2, 20), (3, 30);
insert into child values (1, 100), (4, 400);

-- this case is optimizable
select p.* from parent p left join child c on (p.k = c.k) order by 1,2;
explain (verbose on, costs off)
  select p.* from parent p left join child c on (p.k = c.k) order by 1,2;

-- this case is not
select p.*, linked from parent p
  left join (select c.*, true as linked from child c) as ss
  on (p.k = ss.k) order by p.k;
explain (verbose on, costs off)
  select p.*, linked from parent p
    left join (select c.*, true as linked from child c) as ss
    on (p.k = ss.k) order by p.k;

-- check for a 9.0rc1 bug: join removal breaks pseudoconstant qual handling
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;
explain (verbose on, costs off)
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;

select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;
explain (verbose on, costs off)
select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;

-- bug 5255: this is not optimizable by join removal
start transaction;

CREATE TEMP TABLE a (id int PRIMARY KEY);
CREATE TEMP TABLE b (id int PRIMARY KEY, a_id int);
INSERT INTO a VALUES (0), (1);
INSERT INTO b VALUES (0, 0), (1, NULL);

SELECT * FROM b LEFT JOIN a ON (b.a_id = a.id) WHERE (a.id IS NULL OR a.id > 0);
SELECT b.* FROM b LEFT JOIN a ON (b.a_id = a.id) WHERE (a.id IS NULL OR a.id > 0);

rollback;

-- another join removal bug: this is not optimizable, either
start transaction;

create temp table innertab (id int8 primary key, dat1 int8);
insert into innertab values(123, 42);

SELECT * FROM
    (SELECT 1 AS x) ss1
  LEFT JOIN
    (SELECT q1, q2, COALESCE(dat1, q1) AS y
     FROM int8_tbl LEFT JOIN innertab ON q2 = id) ss2
  ON true order by 1, 2, 3, 4;

rollback;

--
-- test successful handling of nested outer joins with degenerate join quals
--
reset enable_mergejoin;
reset enable_hashjoin;
reset enable_nestloop;
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1) order by 1;

explain (verbose, costs off) 
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

select t1.* from 
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1) order by 1;
  
explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2
                 where q1 = f1) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);
  
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2
                 where q1 = f1) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1) order by 1;

explain (verbose, costs off)
select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1 order by 1,2,3,4,5;

CREATE TABLE mj_t1 (a int,c char(10));
insert into mj_t1 values(11,'abcde');
insert into mj_t1 values(11,'efghi');
CREATE TABLE mj_t2 (a int,c char(10));
insert into mj_t2 values(12,'abcde');
insert into mj_t2 values(12,'efghi');
explain (costs off) select * from mj_t1 full outer join mj_t2 on 1=1;
create table mj_t3 (a int, b char(10))  ;
create table mj_t4 (a int, b char(10))  ;
insert into mj_t3 select * from mj_t1;
insert into mj_t4 select * from mj_t2;
explain (costs off) select * from mj_t3 full outer join mj_t4 on 1=1; 
drop table mj_t1;
drop table mj_t2;
drop table mj_t3;
drop table mj_t4;
create table aaa(id varchar(3),data text) ;
create table bbb(id varchar(3),data text) ;
insert into aaa values ('1','a');
insert into aaa values ('2','b');
insert into aaa values ('3','c');
insert into bbb values ('1','a');
insert into bbb values ('2','b');
insert into bbb values ('4','d');
explain (verbose on, costs off) select * from aaa inner join bbb on (aaa.id = bbb.id);
drop table aaa;
drop table bbb;

-- test Vector Hash Right Anti Join
create table hash_right_anti_x(x int, y int, z int) with(orientation=column);
create table hash_right_anti_y(x int, y int, z int) with(orientation=column);

insert into hash_right_anti_x select v, v*10, v*100 from generate_series(1, 1000) as v;
insert into hash_right_anti_y select v, v*10, v*100 from generate_series(1, 100) as v;

analyze hash_right_anti_x;
analyze hash_right_anti_y;

select * from hash_right_anti_y where not exists (select hash_right_anti_x.y from hash_right_anti_x where hash_right_anti_x.z=hash_right_anti_y.z);

drop table hash_right_anti_x;
drop table hash_right_anti_y;

-- test rownum in outer join
create table t_a (id int, name varchar(10), code int);
create table t_b (id int, name varchar(10), code int);
insert into t_a values (1, 'tom', 3);
insert into t_b values (1, 'bat', 6);
select * from t_a a, t_b b where a.id(+) = b.id and rownum = 1;
drop table t_a;
drop table t_b;