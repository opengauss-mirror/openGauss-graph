--
-- JOIN
-- Test JOIN clauses
--

drop table J1_TBL;
drop foreign table J2_TBL;
CREATE  TABLE J1_TBL (
  i integer,
  j integer,
  t text
);

CREATE foreign TABLE J2_TBL (
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
drop table t1;
drop foreign table t2;
drop table t3;
CREATE TABLE t1 (name TEXT, n INTEGER);
CREATE foreign TABLE t2 (name char(2), n INTEGER);
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

drop foreign table x;
drop table y;

create foreign table x (x1 int, x2 int);
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
-- Clean up
--

DROP TABLE t1;
DROP foreign TABLE t2;
DROP TABLE t3;



-- Both DELETE and UPDATE allow the specification of additional tables
-- to "join" against to determine which rows should be modified.

CREATE  TABLE t1 (a int, b int);
CREATE foreign TABLE t2 (a int, b int);
CREATE  TABLE t3 (x int, y int);

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


select * from (select * from t1 where t1.a in (1,4,11) union all select * from t2) as dt order by 1;

update t1 set a =1 where b in (select b from t1 where t1.a in (1,4,11) union all select b from t2);
update t1 set a =1 where b in (select b from t1 where t1.a in (1,4,11) union all select b from t3);
delete t1 where b in (select b from t1 where t1.a in (1,4,11) union all select b from t2);
delete t1 where b in (select b from t2 where t2.a in (1,4,11) union all select b from t3);
delete t1 where b in (select b from t1 where t1.a in (1,4,11) union all select b from t3);

select a from (select a from t2);
select a , (select a from t3) from (select a from t1);
select a , (select a from t3) from (select a from t2);
select a , (select a from t2) from (select a from t2);

-- test deep nested cases

drop table y1;
drop table y2;
drop table y3;
drop foreign table y4;
drop table y5;

CREATE  TABLE y1 (a int, b int);
CREATE  TABLE y2 (a int, b int);
CREATE  TABLE y3 (a int, b int);
CREATE  foreign TABLE y4 (a int, y int);
CREATE  TABLE y5 (a int, y int);


select y5.a ,
(select (select y1.b from y1, y2) from (select y3.a from y3,y4))
from y5;

select y5.a ,
(select (select y1.b from y1, y2) from (select y3.a from y3,y2))
from y5;

select y5.a ,
(select (select y1.b from y1, y2 ) from (select y3.a from y3,y2))
from y5;

select y5.a ,(select (select y1.b from y1, y2 where y1.a in (select y4.a from y4 )) from (select y3.a from y3,y2)) 
from y5;


-- cross transaction check

begin;
insert into y1 values (1);
insert into y4 values (1);
end;

begin;
delete from y1 ;
delete from y4;
end;

begin;
update y1 set a =1;
update y4 set a =2;
end;

begin;
update y1 set a =1 where a in (select a from y4);
update y4 set a =2;
end;


