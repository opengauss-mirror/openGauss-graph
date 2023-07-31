--
-- SUBSELECT
--

SELECT 1 AS one WHERE 1 IN (SELECT 1);

SELECT 1 AS zero WHERE 1 NOT IN (SELECT 1);

SELECT 1 AS zero WHERE 1 IN (SELECT 2);

-- Check grammar's handling of extra parens in assorted contexts

SELECT * FROM (SELECT 1 AS x) ss;
SELECT * FROM ((SELECT 1 AS x)) ss;

(SELECT 2) UNION SELECT 2;
((SELECT 2)) UNION SELECT 2;

SELECT ((SELECT 2) UNION SELECT 2);
SELECT (((SELECT 2)) UNION SELECT 2);

SELECT (SELECT ARRAY[1,2,3])[1];
SELECT ((SELECT ARRAY[1,2,3]))[2];
SELECT (((SELECT ARRAY[1,2,3])))[3];

-- Set up some simple test tables

CREATE TABLE SUBSELECT_TBL (
  f1 integer,
  f2 integer,
  f3 float
);

INSERT INTO SUBSELECT_TBL VALUES (1, 2, 3);
INSERT INTO SUBSELECT_TBL VALUES (2, 3, 4);
INSERT INTO SUBSELECT_TBL VALUES (3, 4, 5);
INSERT INTO SUBSELECT_TBL VALUES (1, 1, 1);
INSERT INTO SUBSELECT_TBL VALUES (2, 2, 2);
INSERT INTO SUBSELECT_TBL VALUES (3, 3, 3);
INSERT INTO SUBSELECT_TBL VALUES (6, 7, 8);
INSERT INTO SUBSELECT_TBL VALUES (8, 9, NULL);

SELECT '' AS eight, * FROM SUBSELECT_TBL ORDER BY f1, f2, f3;

-- Uncorrelated subselects

SELECT '' AS two, f1 AS "Constant Select" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT 1) ORDER BY 2;

SELECT '' AS six, f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL) 
  ORDER BY 2;

SELECT '' AS six, f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE
    f2 IN (SELECT f1 FROM SUBSELECT_TBL)) 
    ORDER BY 2;

SELECT '' AS three, f1, f2
  FROM SUBSELECT_TBL
  WHERE (f1, f2) NOT IN (SELECT f2, CAST(f3 AS int4) FROM SUBSELECT_TBL
                         WHERE f3 IS NOT NULL) 
                         ORDER BY f1, f2;
-- Correlated subselects

SELECT '' AS six, f1 AS "Correlated Field", f2 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE f1 = upper.f1) 
  ORDER BY f1, f2;

SELECT '' AS six, f1 AS "Correlated Field", f3 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN
    (SELECT f2 FROM SUBSELECT_TBL WHERE CAST(upper.f2 AS float) = f3)
    ORDER BY 2, 3;

SELECT '' AS six, f1 AS "Correlated Field", f3 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f3 IN (SELECT upper.f1 + f2 FROM SUBSELECT_TBL
               WHERE f2 = CAST(f3 AS integer)) 
               ORDER BY 2, 3;

SELECT '' AS five, f1 AS "Correlated Field"
  FROM SUBSELECT_TBL
  WHERE (f1, f2) IN (SELECT f2, CAST(f3 AS int4) FROM SUBSELECT_TBL
                     WHERE f3 IS NOT NULL) 
                     ORDER BY 2;

--
-- Use some existing tables in the regression test
--
SELECT '' AS eight, ss.f1 AS "Correlated Field", ss.f3 AS "Second Field"
  FROM SUBSELECT_TBL ss
  WHERE f1 NOT IN (SELECT f1+1 FROM INT4_TBL
                   WHERE f1 != ss.f1 AND f1 < 2147483647) 
                   ORDER BY 2, 3;

select q1, float8(count(*)) / (select count(*) from int8_tbl)
from int8_tbl group by q1 order by q1;

--
-- Test cases to catch unpleasant interactions between IN-join processing
-- and subquery pullup.
--

-- delelte this sentence, because the output is related with number of nodes
--select count(*) from
--  (select 1 from tenk1 a
--   where unique1 IN (select hundred from tenk1 b)) ss;
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select hundred from tenk1 b)) ss;
select count(*) from
  (select 1 from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;

--
-- Test cases to check for overenthusiastic optimization of
-- "IN (SELECT DISTINCT ...)" and related cases.  Per example from
-- Luca Pireddu and Michael Fuhr.
--

-- Enforce use of COMMIT instead of 2PC for temporary objects

-- CREATE TEMP TABLE subselect_foo (id integer);
CREATE  TABLE subselect_foo (id integer);
-- CREATE TEMP TABLE bar (id1 integer, id2 integer);
CREATE  TABLE subselect_bar (id1 integer, id2 integer);

INSERT INTO subselect_foo VALUES (1);

INSERT INTO subselect_bar VALUES (1, 1);
INSERT INTO subselect_bar VALUES (2, 2);
INSERT INTO subselect_bar VALUES (3, 1);

-- These cases require an extra level of distinct-ing above subquery s
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT DISTINCT id1, id2 FROM subselect_bar) AS s);
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT id1,id2 FROM subselect_bar GROUP BY id1,id2) AS s);
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT id1, id2 FROM subselect_bar UNION
                      SELECT id1, id2 FROM subselect_bar) AS s);

-- These cases do not
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT DISTINCT ON (id2) id1, id2 FROM subselect_bar) AS s);
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT id2 FROM subselect_bar GROUP BY id2) AS s);
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT id2 FROM subselect_bar UNION
                      SELECT id2 FROM subselect_bar) AS s);

--
-- Test case to catch problems with multiply nested sub-SELECTs not getting
-- recalculated properly.  Per bug report from Didier Moens.
--

CREATE TABLE orderstest (
    approver_ref integer,
    po_ref integer,
    ordercanceled boolean
);

INSERT INTO orderstest VALUES (1, 1, false);
INSERT INTO orderstest VALUES (66, 5, false);
INSERT INTO orderstest VALUES (66, 6, false);
INSERT INTO orderstest VALUES (66, 7, false);
INSERT INTO orderstest VALUES (66, 1, true);
INSERT INTO orderstest VALUES (66, 8, false);
INSERT INTO orderstest VALUES (66, 1, false);
INSERT INTO orderstest VALUES (77, 1, false);
INSERT INTO orderstest VALUES (1, 1, false);
INSERT INTO orderstest VALUES (66, 1, false);
INSERT INTO orderstest VALUES (1, 1, false);

CREATE VIEW orders_view AS
SELECT *,
(SELECT CASE
   WHEN ord.approver_ref=1 THEN '---' ELSE 'Approved'
 END) AS "Approved",
(SELECT CASE
 WHEN ord.ordercanceled
 THEN 'Canceled'
 ELSE
  (SELECT CASE
		WHEN ord.po_ref=1
		THEN
		 (SELECT CASE
				WHEN ord.approver_ref=1
				THEN '---'
				ELSE 'Approved'
			END)
		ELSE 'PO'
	END)
END) AS "Status",
(CASE
 WHEN ord.ordercanceled
 THEN 'Canceled'
 ELSE
  (CASE
		WHEN ord.po_ref=1
		THEN
		 (CASE
				WHEN ord.approver_ref=1
				THEN '---'
				ELSE 'Approved'
			END)
		ELSE 'PO'
	END)
END) AS "Status_OK"
FROM orderstest ord;

SELECT * FROM orders_view 
ORDER BY approver_ref, po_ref, ordercanceled;

DROP TABLE orderstest cascade;

--
-- Test cases to catch situations where rule rewriter fails to propagate
-- hasSubLinks flag correctly.  Per example from Kyle Bateman.
--

-- create temp table parts (
create  table parts (
    partnum     text,
    cost        float8
);

-- create temp table shipped (
create  table shipped (
    ttype       char(2),
    ordnum      int4,
    partnum     text,
    value       float8
);

-- create temp view shipped_view as
create  view shipped_view as
    select * from shipped where ttype = 'wt';

create rule shipped_view_insert as on insert to shipped_view do instead
    insert into shipped values('wt', new.ordnum, new.partnum, new.value);

insert into parts (partnum, cost) values (1, 1234.56);

insert into shipped_view (ordnum, partnum, value)
    values (0, 1, (select cost from parts where partnum = '1'));

select * from shipped_view;

create rule shipped_view_update as on update to shipped_view do instead
    update shipped set partnum = new.partnum, value = new.value
        where ttype = new.ttype and ordnum = new.ordnum;

update shipped_view set value = 11
    from int4_tbl a join int4_tbl b
      on (a.f1 = (select f1 from int4_tbl c where c.f1=b.f1))
    where ordnum = a.f1;

select * from shipped_view;

select f1, ss1 as relabel from
    (select *, (select sum(f1) from int4_tbl b where f1 >= a.f1) as ss1
     from int4_tbl a) ss 
     ORDER BY f1, relabel;

--
-- Test cases involving PARAM_EXEC parameters and min/max index optimizations.
-- Per bug report from David Sanchez i Gregori.
--
explain (verbose on, costs off)
select * from (
  select max(unique1) from tenk1 as a
  where exists (select 1 from tenk1 as b where b.thousand = a.unique2)
) ss;

select * from (
  select max(unique1) from tenk1 as a
  where exists (select 1 from tenk1 as b where b.thousand = a.unique2)
) ss;

explain (verbose on, costs off)
select * from (
  select min(unique1) from tenk1 as a
  where not exists (select 1 from tenk1 as b where b.unique2 = 10000)
) ss;

select * from (
  select min(unique1) from tenk1 as a
  where not exists (select 1 from tenk1 as b where b.unique2 = 10000)
) ss;

explain (verbose on, costs off)
select * from (
  select min(unique1) as b from tenk1 as a
  where not exists (select 1 from tenk1 as b where b.unique2 = 10000)
) ss where b>5;

select * from (
  select min(unique1) as b from tenk1 as a
  where not exists (select 1 from tenk1 as b where b.unique2 = 10000)
) ss where b>5;

explain (verbose on, costs off)
select * from (
  select min(unique1) from tenk1 as a
  where exists (select * from generate_series(1,1))
) ss;

select * from (
  select min(unique1) from tenk1 as a
  where exists (select * from generate_series(1,1))
) ss;

explain (verbose on, costs off)
select * from (
  select min(unique1) from tenk1 as a
  where exists (select * from generate_series(1,1))
) ss where exists(select * from generate_series(1,1));

select * from (
  select min(unique1) from tenk1 as a
  where exists (select * from generate_series(1,1))
) ss where exists(select * from generate_series(1,1));

DROP TABLE subselect_foo CASCADE;
DROP TABLE subselect_bar CASCADE;
DROP table parts CASCADE;
DROP table shipped CASCADE;
