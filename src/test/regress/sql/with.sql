--
--FOR BLACKLIST FEATURE: RULE、TRIGGER、INHERITS is not supported.
--

--
-- Tests for common table expressions (WITH query, ... SELECT ...)
--

-- Basic WITH
WITH q1(x,y) AS (SELECT 1,2)
SELECT * FROM q1, q1 AS q2;

-- Multiple uses are evaluated only once
SELECT count(*) FROM (
  WITH q1(x) AS (SELECT random() FROM generate_series(1, 5))
    SELECT * FROM q1
  UNION
    SELECT * FROM q1
) ss;

-- WITH RECURSIVE

-- sum of 1..100
WITH RECURSIVE t(n) AS (
    VALUES (1)
UNION ALL
    SELECT n+1 FROM t WHERE n < 100
)
SELECT sum(n) FROM t;

WITH RECURSIVE t(n) AS (
    SELECT (VALUES(1))
UNION ALL
    SELECT n+1 FROM t WHERE n < 5
)
SELECT * FROM t ORDER BY n;

-- This is an infinite loop with UNION ALL, but not with UNION
WITH RECURSIVE t(n) AS (
    SELECT 1
UNION
    SELECT 10-n FROM t)
SELECT * FROM t ORDER BY n;

-- This'd be an infinite loop, but outside query reads only as much as needed
WITH RECURSIVE t(n) AS (
    VALUES (1)
UNION ALL
    SELECT n+1 FROM t)
SELECT * FROM t LIMIT 10;

-- UNION case should have same property
WITH RECURSIVE t(n) AS (
    SELECT 1
UNION
    SELECT n+1 FROM t)
SELECT * FROM t LIMIT 10;

-- Test behavior with an unknown-type literal in the WITH
WITH q AS (SELECT 'foo' AS x)
SELECT x, x IS OF (unknown) as is_unknown FROM q;

WITH RECURSIVE t(n) AS (
    SELECT 'foo'
UNION ALL
    SELECT n || ' bar' FROM t WHERE length(n) < 20
)
SELECT n, n IS OF (text) as is_text FROM t ORDER BY n;

--
-- Some examples with a tree
--
-- department structure represented here is as follows:
--
-- ROOT-+->A-+->B-+->C
--      |         |
--      |         +->D-+->F
--      +->E-+->G

-- Enforce use of COMMIT instead of 2PC for temporary objects
-- SET enforce_two_phase_commit TO off;

CREATE TABLE department (
	id INTEGER PRIMARY KEY,  -- department ID
	parent_department INTEGER ,
	name TEXT -- department name
);

INSERT INTO department VALUES (0, NULL, 'ROOT');
INSERT INTO department VALUES (1, 0, 'A');
INSERT INTO department VALUES (2, 1, 'B');
INSERT INTO department VALUES (3, 2, 'C');
INSERT INTO department VALUES (4, 2, 'D');
INSERT INTO department VALUES (5, 0, 'E');
INSERT INTO department VALUES (6, 4, 'F');
INSERT INTO department VALUES (7, 5, 'G');


-- extract all departments under 'A'. Result should be A, B, C, D and F
WITH RECURSIVE subdepartment AS
(
	-- non recursive term
	SELECT name as root_name, * FROM department WHERE name = 'A'

	UNION ALL

	-- recursive term
	SELECT sd.root_name, d.* FROM department AS d, subdepartment AS sd
		WHERE d.parent_department = sd.id
)
SELECT * FROM subdepartment ORDER BY name;

-- extract all departments under 'A' with "level" number
WITH RECURSIVE subdepartment(level, id, parent_department, name) AS
(
	-- non recursive term
	SELECT 1, * FROM department WHERE name = 'A'

	UNION ALL

	-- recursive term
	SELECT sd.level + 1, d.* FROM department AS d, subdepartment AS sd
		WHERE d.parent_department = sd.id
)
SELECT * FROM subdepartment ORDER BY name;

-- extract all departments under 'A' with "level" number.
-- Only shows level 2 or more
WITH RECURSIVE subdepartment(level, id, parent_department, name) AS
(
	-- non recursive term
	SELECT 1, * FROM department WHERE name = 'A'

	UNION ALL

	-- recursive term
	SELECT sd.level + 1, d.* FROM department AS d, subdepartment AS sd
		WHERE d.parent_department = sd.id
)
SELECT * FROM subdepartment WHERE level >= 2 ORDER BY name;

-- "RECURSIVE" is ignored if the query has no self-reference
WITH RECURSIVE subdepartment AS
(
	-- note lack of recursive UNION structure
	SELECT * FROM department WHERE name = 'A'
)
SELECT * FROM subdepartment ORDER BY name;

-- inside subqueries
SELECT count(*) FROM (
    WITH RECURSIVE t(n) AS (
        SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 500
    )
    SELECT * FROM t) AS t WHERE n < (
        SELECT count(*) FROM (
            WITH RECURSIVE t(n) AS (
                   SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 100
                )
            SELECT * FROM t WHERE n < 50000
         ) AS t WHERE n < 100);

-- use same CTE twice at different subquery levels
WITH q1(x,y) AS (
    SELECT hundred, sum(ten) FROM tenk1 GROUP BY hundred
  )
SELECT count(*) FROM q1 WHERE y > (SELECT sum(y)/100 FROM q1 qsub);

-- via a VIEW
CREATE VIEW vsubdepartment AS
	WITH RECURSIVE subdepartment AS
	(
		 -- non recursive term
		SELECT * FROM department WHERE name = 'A'
		UNION ALL
		-- recursive term
		SELECT d.* FROM department AS d, subdepartment AS sd
			WHERE d.parent_department = sd.id
	)
	SELECT * FROM subdepartment;

SELECT * FROM vsubdepartment ORDER BY name;

-- Check reverse listing
SELECT pg_get_viewdef('vsubdepartment'::regclass);
SELECT pg_get_viewdef('vsubdepartment'::regclass, true);

-- corner case in which sub-WITH gets initialized first
select count(*) from (with recursive q as (
      (select * from department order by id)
    union all
      (with x as (select * from q)
       select * from x)
    )
select * from q limit 24) rel_alias;

select count(*) from (with recursive q as (
      (select * from department order by id)
    union all
      (with recursive x as (
           select * from department
         union all
           (select * from q union all select * from x)
        )
       select * from x)
    )
select * from q limit 32);

-- recursive term has sub-UNION
WITH RECURSIVE t(i,j) AS (
	VALUES (1,2)
	UNION ALL
	SELECT t2.i, t.j+1 FROM
		(SELECT 2 AS i UNION ALL SELECT 3 AS i) AS t2
		JOIN t ON (t2.i = t.i+1))

	SELECT * FROM t order by i;

--
-- different tree example
--
CREATE TABLE tree(
    id INTEGER PRIMARY KEY,
    parent_id INTEGER 
);

INSERT INTO tree
VALUES (1, NULL), (2, 1), (3,1), (4,2), (5,2), (6,2), (7,3), (8,3),
       (9,4), (10,4), (11,7), (12,7), (13,7), (14, 9), (15,11), (16,11);

--
-- get all paths from "second level" nodes to leaf nodes
--
WITH RECURSIVE t(id, path) AS (
    VALUES(1,ARRAY[]::integer[])
UNION ALL
    SELECT tree.id, t.path || tree.id
    FROM tree JOIN t ON (tree.parent_id = t.id)
)
SELECT t1.*, t2.* FROM t AS t1 JOIN t AS t2 ON
	(t1.path[1] = t2.path[1] AND
	array_upper(t1.path,1) = 1 AND
	array_upper(t2.path,1) > 1)
	ORDER BY t1.id, t2.id;

-- just count 'em
WITH RECURSIVE t(id, path) AS (
    VALUES(1,ARRAY[]::integer[])
UNION ALL
    SELECT tree.id, t.path || tree.id
    FROM tree JOIN t ON (tree.parent_id = t.id)
)
SELECT t1.id, count(t2.*) FROM t AS t1 JOIN t AS t2 ON
	(t1.path[1] = t2.path[1] AND
	array_upper(t1.path,1) = 1 AND
	array_upper(t2.path,1) > 1)
	GROUP BY t1.id
	ORDER BY t1.id;

-- this variant tickled a whole-row-variable bug in 8.4devel
WITH RECURSIVE t(id, path) AS (
    VALUES(1,ARRAY[]::integer[])
UNION ALL
    SELECT tree.id, t.path || tree.id
    FROM tree JOIN t ON (tree.parent_id = t.id)
)
SELECT t1.id, t2.path, t2 FROM t AS t1 JOIN t AS t2 ON
(t1.id=t2.id) ORDER BY id;

--
-- test cycle detection
--
create table graph( f int, t int, label text );

insert into graph values
	(1, 2, 'arc 1 -> 2'),
	(1, 3, 'arc 1 -> 3'),
	(2, 3, 'arc 2 -> 3'),
	(1, 4, 'arc 1 -> 4'),
	(4, 5, 'arc 4 -> 5'),
	(5, 1, 'arc 5 -> 1');

with recursive search_graph(f, t, label, path, cycle) as (
	select *, array[row(g.f, g.t)], false from graph g
	union all
	select g.*, path || row(g.f, g.t), row(g.f, g.t) = any(path)
	from graph g, search_graph sg
	where g.f = sg.t and not cycle
)
select * from search_graph order by path;

-- ordering by the path column has same effect as SEARCH DEPTH FIRST
with recursive search_graph(f, t, label, path, cycle) as (
	select *, array[row(g.f, g.t)], false from graph g
	union all
	select g.*, path || row(g.f, g.t), row(g.f, g.t) = any(path)
	from graph g, search_graph sg
	where g.f = sg.t and not cycle
)
select * from search_graph order by path;

--
-- test multiple WITH queries
--
WITH RECURSIVE
  y (id) AS (VALUES (1)),
  x (id) AS (SELECT * FROM y UNION ALL SELECT id+1 FROM x WHERE id < 5)
SELECT * FROM x ORDER BY id;

-- forward reference OK
WITH RECURSIVE
    x(id) AS (SELECT * FROM y UNION ALL SELECT id+1 FROM x WHERE id < 5),
    y(id) AS (values (1))
 SELECT * FROM x ORDER BY id;

WITH RECURSIVE
   x(id) AS
     (VALUES (1) UNION ALL SELECT id+1 FROM x WHERE id < 5),
   y(id) AS
     (VALUES (1) UNION ALL SELECT id+1 FROM y WHERE id < 10)
 SELECT y.*, x.* FROM y LEFT JOIN x USING (id) ORDER BY 1;

WITH RECURSIVE
   x(id) AS
     (VALUES (1) UNION ALL SELECT id+1 FROM x WHERE id < 5),
   y(id) AS
     (VALUES (1) UNION ALL SELECT id+1 FROM x WHERE id < 10)
 SELECT y.*, x.* FROM y LEFT JOIN x USING (id) ORDER BY 1;

WITH RECURSIVE
   x(id) AS
     (SELECT 1 UNION ALL SELECT id+1 FROM x WHERE id < 3 ),
   y(id) AS
     (SELECT * FROM x UNION ALL SELECT * FROM x),
   z(id) AS
     (SELECT * FROM x UNION ALL SELECT id+1 FROM z WHERE id < 10)
 SELECT * FROM z ORDER BY id;

WITH RECURSIVE
   x(id) AS
     (SELECT 1 UNION ALL SELECT id+1 FROM x WHERE id < 3 ),
   y(id) AS
     (SELECT * FROM x UNION ALL SELECT * FROM x),
   z(id) AS
     (SELECT * FROM y UNION ALL SELECT id+1 FROM z WHERE id < 10)
 SELECT * FROM z ORDER BY id;

--
-- Test WITH attached to a data-modifying statement
--

CREATE TABLE y (a INTEGER PRIMARY KEY INITIALLY DEFERRED)  ;
INSERT INTO y SELECT generate_series(1, 10);

WITH t AS (
	SELECT a FROM y
)
INSERT INTO y
SELECT a+20 FROM t order by 1 RETURNING *;

SELECT * FROM y order by 1;

WITH t AS (
	SELECT a FROM y
)
UPDATE y SET a = y.a-10 FROM t WHERE y.a > 20 AND t.a = y.a RETURNING y.a;

SELECT * FROM y order by 1;

WITH RECURSIVE t(a) AS (
	SELECT 11
	UNION ALL
	SELECT a+1 FROM t WHERE a < 50
)
DELETE FROM y USING t WHERE t.a = y.a RETURNING y.a;

SELECT * FROM y order by 1;

UPDATE y SET a = a + 1 RETURNING *;

DROP TABLE y;

--
-- error cases
--

-- INTERSECT
WITH RECURSIVE x(n) AS (SELECT 1 INTERSECT SELECT n+1 FROM x)
	SELECT * FROM x;

WITH RECURSIVE x(n) AS (SELECT 1 INTERSECT ALL SELECT n+1 FROM x)
	SELECT * FROM x;

-- EXCEPT
WITH RECURSIVE x(n) AS (SELECT 1 EXCEPT SELECT n+1 FROM x)
	SELECT * FROM x;

WITH RECURSIVE x(n) AS (SELECT 1 EXCEPT ALL SELECT n+1 FROM x)
	SELECT * FROM x;

-- no non-recursive term
WITH RECURSIVE x(n) AS (SELECT n FROM x)
	SELECT * FROM x;

-- recursive term in the left hand side (strictly speaking, should allow this)
WITH RECURSIVE x(n) AS (SELECT n FROM x UNION ALL SELECT 1)
	SELECT * FROM x;

CREATE TABLE y (a INTEGER PRIMARY KEY)  ;
INSERT INTO y SELECT generate_series(1, 10);

-- LEFT JOIN

WITH RECURSIVE x(n) AS (SELECT a FROM y WHERE a = 1
	UNION ALL
	SELECT x.n+1 FROM y LEFT JOIN x ON x.n = y.a WHERE n < 10)
SELECT * FROM x;

-- RIGHT JOIN
WITH RECURSIVE x(n) AS (SELECT a FROM y WHERE a = 1
	UNION ALL
	SELECT x.n+1 FROM x RIGHT JOIN y ON x.n = y.a WHERE n < 10)
SELECT * FROM x;

-- FULL JOIN
WITH RECURSIVE x(n) AS (SELECT a FROM y WHERE a = 1
	UNION ALL
	SELECT x.n+1 FROM x FULL JOIN y ON x.n = y.a WHERE n < 10)
SELECT * FROM x;

-- subquery
WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x
                          WHERE n IN (SELECT * FROM x))
  SELECT * FROM x;

-- aggregate functions
WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT count(*) FROM x)
  SELECT * FROM x;

WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT sum(n) FROM x)
  SELECT * FROM x;

-- ORDER BY
WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x ORDER BY 1)
  SELECT * FROM x;

-- LIMIT/OFFSET
WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x LIMIT 10 OFFSET 1)
  SELECT * FROM x;

-- FOR UPDATE
WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x FOR UPDATE)
  SELECT * FROM x;

-- target list has a recursive query name
WITH RECURSIVE x(id) AS (values (1)
    UNION ALL
    SELECT (SELECT * FROM x) FROM x WHERE id < 5
) SELECT * FROM x;

-- mutual recursive query (not implemented)
WITH RECURSIVE
  x (id) AS (SELECT 1 UNION ALL SELECT id+1 FROM y WHERE id < 5),
  y (id) AS (SELECT 1 UNION ALL SELECT id+1 FROM x WHERE id < 5)
SELECT * FROM x;

-- non-linear recursion is not allowed
WITH RECURSIVE foo(i) AS
    (values (1)
    UNION ALL
       (SELECT i+1 FROM foo WHERE i < 10
          UNION ALL
       SELECT i+1 FROM foo WHERE i < 5)
) SELECT * FROM foo;

WITH RECURSIVE foo(i) AS
    (values (1)
    UNION ALL
	   SELECT * FROM
       (SELECT i+1 FROM foo WHERE i < 10
          UNION ALL
       SELECT i+1 FROM foo WHERE i < 5) AS t
) SELECT * FROM foo;

WITH RECURSIVE foo(i) AS
    (values (1)
    UNION ALL
       (SELECT i+1 FROM foo WHERE i < 10
          EXCEPT
       SELECT i+1 FROM foo WHERE i < 5)
) SELECT * FROM foo;

WITH RECURSIVE foo(i) AS
    (values (1)
    UNION ALL
       (SELECT i+1 FROM foo WHERE i < 10
          INTERSECT
       SELECT i+1 FROM foo WHERE i < 5)
) SELECT * FROM foo;

-- Wrong type induced from non-recursive term
WITH RECURSIVE foo(i) AS
   (SELECT i FROM (VALUES(1),(2)) t(i)
   UNION ALL
   SELECT (i+1)::numeric(10,0) FROM foo WHERE i < 10)
SELECT * FROM foo;

-- rejects different typmod, too (should we allow this?)
WITH RECURSIVE foo(i) AS
   (SELECT i::numeric(3,0) FROM (VALUES(1),(2)) t(i)
   UNION ALL
   SELECT (i+1)::numeric(10,0) FROM foo WHERE i < 10)
SELECT * FROM foo;

-- disallow OLD/NEW reference in CTE
CREATE TABLE x (n integer);
CREATE RULE r2 AS ON UPDATE TO x DO INSTEAD
    WITH t AS (SELECT OLD.*) UPDATE y SET a = t.n FROM t;

--
-- test for bug #4902
--
with cte(foo) as ( values(42) ) values((select foo from cte));
with cte(foo) as ( select 42 ) select * from ((select foo from cte)) q;

-- test CTE referencing an outer-level variable (to see that changed-parameter
-- signaling still works properly after fixing this bug)
select ( with cte(foo) as ( values(f1) )
         select (select foo from cte) )
from int4_tbl order by 1;

select ( with cte(foo) as ( values(f1) )
          values((select foo from cte)) )
from int4_tbl order by 1;

--
-- test for nested-recursive-WITH bug
--
WITH RECURSIVE t(j) AS (
    WITH RECURSIVE s(i) AS (
        VALUES (1)
        UNION ALL
        SELECT i+1 FROM s WHERE i < 10
    )
    SELECT i FROM s
    UNION ALL
    SELECT j+1 FROM t WHERE j < 10
)
SELECT * FROM t order by 1;

--
-- test WITH attached to intermediate-level set operation
--

WITH outermost(x) AS (
  SELECT 1
  UNION (WITH innermost as (SELECT 2)
         SELECT * FROM innermost
         UNION SELECT 3)
)
SELECT * FROM outermost ORDER BY 1;

WITH outermost(x) AS (
  SELECT 1
  UNION (WITH innermost as (SELECT 2)
         SELECT * FROM outermost  -- fail
         UNION SELECT * FROM innermost)
)
SELECT * FROM outermost;

WITH RECURSIVE outermost(x) AS (
  SELECT 1
  UNION (WITH innermost as (SELECT 2)
         SELECT * FROM outermost
         UNION SELECT * FROM innermost)
)
SELECT * FROM outermost ORDER BY 1;

WITH RECURSIVE outermost(x) AS (
  WITH innermost as (SELECT 2 FROM outermost) -- fail
    SELECT * FROM innermost
    UNION SELECT * from outermost
)
SELECT * FROM outermost;

--
-- This test will fail with the old implementation of PARAM_EXEC parameter
-- assignment, because the "q1" Var passed down to A's targetlist subselect
-- looks exactly like the "A.id" Var passed down to C's subselect, causing
-- the old code to give them the same runtime PARAM_EXEC slot.  But the
-- lifespans of the two parameters overlap, thanks to B also reading A.
--

with
A as ( select q2 as id, (select q1) as x from int8_tbl ),
B as ( select id, row_number() over (partition by id) as r from A ),
C as ( select A.id, array(select B.id from B where B.id = A.id) from A )
select * from C order by id;

--
-- Test CTEs read in non-initialization orders
--

WITH RECURSIVE
  tab(id_key,link) AS (VALUES (1,17), (2,17), (3,17), (4,17), (6,17), (5,17)),
  iter (id_key, row_type, link) AS (
      SELECT 0, 'base', 17
    UNION ALL (
      WITH remaining(id_key, row_type, link, min) AS (
        SELECT tab.id_key, 'true'::text, iter.link, MIN(tab.id_key) OVER ()
        FROM tab INNER JOIN iter USING (link)
        WHERE tab.id_key > iter.id_key
      ),
      first_remaining AS (
        SELECT id_key, row_type, link
        FROM remaining
        WHERE id_key=min
      ),
      effect AS (
        SELECT tab.id_key, 'new'::text, tab.link
        FROM first_remaining e INNER JOIN tab ON e.id_key=tab.id_key
        WHERE e.row_type = 'false'
      )
      SELECT * FROM first_remaining
      UNION ALL SELECT * FROM effect
    )
  )
SELECT * FROM iter;

WITH RECURSIVE
  tab(id_key,link) AS (VALUES (1,17), (2,17), (3,17), (4,17), (6,17), (5,17)),
  iter (id_key, row_type, link) AS (
      SELECT 0, 'base', 17
    UNION (
      WITH remaining(id_key, row_type, link, min) AS (
        SELECT tab.id_key, 'true'::text, iter.link, MIN(tab.id_key) OVER ()
        FROM tab INNER JOIN iter USING (link)
        WHERE tab.id_key > iter.id_key
      ),
      first_remaining AS (
        SELECT id_key, row_type, link
        FROM remaining
        WHERE id_key=min
      ),
      effect AS (
        SELECT tab.id_key, 'new'::text, tab.link
        FROM first_remaining e INNER JOIN tab ON e.id_key=tab.id_key
        WHERE e.row_type = 'false'
      )
      SELECT * FROM first_remaining
      UNION ALL SELECT * FROM effect
    )
  )
SELECT * FROM iter;

--
-- Data-modifying statements in WITH
--

-- INSERT ... RETURNING
WITH t AS (
    INSERT INTO y
    VALUES
        (11),
        (12),
        (13),
        (14),
        (15),
        (16),
        (17),
        (18),
        (19),
        (20)
    RETURNING *
)
SELECT * FROM t order by 1;

SELECT * FROM y order by 1;

-- UPDATE ... RETURNING
WITH t AS (
    UPDATE y
    SET a=a+1
    RETURNING *
)
SELECT * FROM t order by 1;

SELECT * FROM y order by 1;

-- DELETE ... RETURNING
WITH t AS (
    DELETE FROM y
    WHERE a <= 10
    RETURNING *
)
SELECT * FROM t order by 1;

SELECT * FROM y order by 1;

-- forward reference
WITH RECURSIVE t AS (
	INSERT INTO y
		SELECT a+5 FROM t2 WHERE a > 5
	RETURNING *
), t2 AS (
	UPDATE y SET a=a-11 RETURNING *
)
SELECT * FROM t
UNION ALL
SELECT * FROM t2 order by 1;

SELECT * FROM y order by 1;

-- unconditional DO INSTEAD rule
CREATE RULE y_rule AS ON DELETE TO y DO INSTEAD
  INSERT INTO y VALUES(42) RETURNING *;

WITH t AS (
	DELETE FROM y RETURNING *
)
SELECT * FROM t;

SELECT * FROM y order by 1;

DROP RULE y_rule ON y;

-- check merging of outer CTE with CTE in a rule action
CREATE TABLE bug6051 AS
  select i from generate_series(1,3) as t(i);

SELECT * FROM bug6051 ORDER BY 1;

WITH t1 AS ( DELETE FROM bug6051 RETURNING * )
INSERT INTO bug6051 SELECT * FROM t1;

SELECT * FROM bug6051 ORDER BY 1;

CREATE TABLE bug6051_2 (i int);

CREATE RULE bug6051_ins AS ON INSERT TO bug6051 DO INSTEAD
 INSERT INTO bug6051_2
 SELECT NEW.i;

WITH t1 AS ( DELETE FROM bug6051 RETURNING * )
INSERT INTO bug6051 SELECT * FROM t1;

SELECT * FROM bug6051 ORDER BY 1;
SELECT * FROM bug6051_2 ORDER BY 1;

-- a truly recursive CTE in the same list
WITH RECURSIVE t(a) AS (
	SELECT 0
		UNION ALL
	SELECT a+1 FROM t WHERE a+1 < 5
), t2 as (
	INSERT INTO y
		SELECT * FROM t RETURNING *
)
SELECT * FROM t2 JOIN y USING (a) ORDER BY a;

SELECT * FROM y order by 1;

-- data-modifying WITH in a modifying statement
WITH t AS (
    DELETE FROM y
    WHERE a <= 10
    RETURNING *
)
INSERT INTO y SELECT -a FROM t ORDER BY 1 RETURNING *;

SELECT * FROM y order by 1;

-- check that WITH query is run to completion even if outer query isn't
WITH t AS (
    UPDATE y SET a = a * 100 RETURNING *
)
SELECT * FROM t ORDER BY 1 LIMIT 10;

SELECT * FROM y order by 1;

-- check that run to completion happens in proper ordering

TRUNCATE TABLE y;
INSERT INTO y SELECT generate_series(1, 3);
CREATE TABLE yy (a INTEGER);

WITH RECURSIVE t1 AS (
  INSERT INTO y SELECT * FROM y RETURNING *
), t2 AS (
  INSERT INTO yy SELECT * FROM t1 RETURNING *
)
SELECT 1;

SELECT * FROM y order by 1;
SELECT * FROM yy order by 1;

WITH RECURSIVE t1 AS (
  INSERT INTO yy SELECT * FROM t2 RETURNING *
), t2 AS (
  INSERT INTO y SELECT * FROM y RETURNING *
)
SELECT 1;

SELECT * FROM y order by 1;
SELECT * FROM yy order by 1;

-- triggers

TRUNCATE TABLE y;
INSERT INTO y SELECT generate_series(1, 10);

CREATE FUNCTION y_trigger() RETURNS trigger AS $$
begin
  raise notice 'y_trigger: a = %', new.a;
  return new;
end;
$$ LANGUAGE plpgsql;

CREATE TRIGGER y_trig BEFORE INSERT ON y FOR EACH ROW
    EXECUTE PROCEDURE y_trigger();

WITH t AS (
    INSERT INTO y
    VALUES
        (21),
        (22),
        (23)
    RETURNING *
)
SELECT * FROM t;

SELECT * FROM y order by 1;

DROP TRIGGER y_trig ON y;

CREATE TRIGGER y_trig AFTER INSERT ON y FOR EACH ROW
    EXECUTE PROCEDURE y_trigger();

WITH t AS (
    INSERT INTO y
    VALUES
        (31),
        (32),
        (33)
    RETURNING *
)
SELECT * FROM t LIMIT 1;

SELECT * FROM y order by 1;

DROP TRIGGER y_trig ON y;

CREATE OR REPLACE FUNCTION y_trigger() RETURNS trigger AS $$
begin
  raise notice 'y_trigger';
  return null;
end;
$$ LANGUAGE plpgsql;

CREATE TRIGGER y_trig AFTER INSERT ON y FOR EACH STATEMENT
    EXECUTE PROCEDURE y_trigger();

WITH t AS (
    INSERT INTO y
    VALUES
        (41),
        (42),
        (43)
    RETURNING *
)
SELECT * FROM t;

SELECT * FROM y order by 1;

DROP TRIGGER y_trig ON y;
DROP FUNCTION y_trigger();

-- WITH attached to inherited UPDATE or DELETE

CREATE TABLE parent ( id int PRIMARY KEY, val text )  ;
CREATE TABLE child1 ( ) INHERITS ( parent )  ;
ALTER TABLE child1 ADD PRIMARY KEY(id);
CREATE TABLE child2 ( ) INHERITS ( parent )  ;
ALTER TABLE child2 ADD PRIMARY KEY(id);


INSERT INTO parent VALUES ( 1, 'p1' );
INSERT INTO child1 VALUES ( 11, 'c11' ),( 12, 'c12' );
INSERT INTO child2 VALUES ( 23, 'c21' ),( 24, 'c22' );

WITH rcte AS ( SELECT sum(id) AS totalid FROM parent )
UPDATE parent SET id = id + totalid FROM rcte;

SELECT * FROM parent ORDER BY id;

WITH wcte AS ( INSERT INTO child1 VALUES ( 42, 'new' ) RETURNING id AS newid )
UPDATE parent SET id = id + newid FROM wcte;

SELECT * FROM parent ORDER BY id;

WITH rcte AS ( SELECT max(id) AS maxid FROM parent )
DELETE FROM parent USING rcte WHERE id = maxid;

SELECT * FROM parent ORDER BY id;

WITH wcte AS ( INSERT INTO child2 VALUES ( 42, 'new2' ) RETURNING id AS newid )
DELETE FROM parent USING wcte WHERE id = newid;

SELECT * FROM parent ORDER BY id;

drop table child1;
drop table child2;
drop table parent;

-- check EXPLAIN VERBOSE for a wCTE with RETURNING
SET ENABLE_NESTLOOP = FALSE;
SET ENABLE_HASHJOIN = FALSE;
EXPLAIN (VERBOSE, COSTS OFF, NODES OFF, NUM_NODES OFF)
WITH wcte AS ( INSERT INTO int8_tbl VALUES ( 42, 47 ) RETURNING q2 )
DELETE FROM a USING wcte WHERE aa = q2;
RESET ENABLE_NESTLOOP;
RESET ENABLE_HASHJOIN;
-- error cases

-- data-modifying WITH tries to use its own output
WITH RECURSIVE t AS (
	INSERT INTO y
		SELECT * FROM t
)
VALUES(FALSE);

-- no RETURNING in a referenced data-modifying WITH
WITH t AS (
	INSERT INTO y VALUES(0)
)
SELECT * FROM t;

-- data-modifying WITH allowed only at the top level
SELECT * FROM (
	WITH t AS (UPDATE y SET a=a+1 RETURNING *)
	SELECT * FROM t
) ss;

-- most variants of rules aren't allowed
CREATE RULE y_rule AS ON INSERT TO y WHERE a=0 DO INSTEAD DELETE FROM y;
WITH t AS (
	INSERT INTO y VALUES(0)
)
VALUES(FALSE);
DROP RULE y_rule ON y;

-- check cte replace when pulling up
-- case 1: exists/any sublink
create table t_cte(a int, b int);
insert into t_cte select generate_series(1,5), generate_series(1,5);
analyze t_cte;
explain (verbose on, costs off)
with x as
(select a, b from t_cte)
select 1 from t_cte t
where exists
(select 1 from x where x.a=t.a);

with x as
(select a, b from t_cte)
select 1 from t_cte t
where exists
(select 1 from x where x.a=t.a);

-- case 2: not exists/any sublink
explain (verbose on, costs off)
with x as
(select a, b from t_cte)
select 1 from t_cte t 
where t.a not in 
(select a from x);

with x as
(select a, b from t_cte)
select 1 from t_cte t 
where t.a not in 
(select a from x);

-- case 3: expr sublink 
explain (verbose on, costs off)
with x as
(select a, b from t_cte)
select 1 from t_cte t 
where t.a > 
(select avg(a) from x where x.b=t.b);

with x as
(select a, b from t_cte)
select 1 from t_cte t 
where t.a > 
(select avg(a) from x where x.b=t.b);

-- case 4: exists/any/expr sublink in OR clause
explain (verbose on, costs off)
with x as
(select a, b from t_cte)
select 1 from t_cte t 
where t.a not in 
(select a from x)
or exists
(select 1 from x where x.a=t.a)
or t.a >     
(select avg(a) from x where x.b=t.b);

with x as
(select a, b from t_cte)
select 1 from t_cte t 
where t.a not in 
(select a from x)
or exists
(select 1 from x where x.a=t.a)
or t.a >     
(select avg(a) from x where x.b=t.b);

-- case 5: non-pull up sublink 
explain (verbose on, costs off)
with x as
(select a, b from t_cte)
select 1 from t_cte t 
where t.a not in 
(select a from x where x.b=t.b);

with x as
(select a, b from t_cte)
select 1 from t_cte t 
where t.a not in 
(select a from x where x.b=t.b);

-- case 6: pullup simplequery
explain (verbose on, costs off)
with x as
(select a,b from t_cte)
select sum(b) from
(select a*b b from t_cte where a in (select b from x));

with x as
(select a,b from t_cte)
select sum(b) from
(select a*b b from t_cte where a in (select b from x));

-- case 7: same cte alias in different level
explain (verbose on, costs off)
with x as
(select a,b from t_cte)
select sum(b) from
(select b from
 (select sum(b) b from
  (with x as
  (select a+b a, a-b b from t_cte)
  select sum(b) b from
   (select a*b b from t_cte where a in (select b from x))
  )
 )
);

with x as
(select a,b from t_cte)
select sum(b) from
(select b from
 (select sum(b) b from
  (with x as
  (select a+b a, a-b b from t_cte)
  select sum(b) b from
   (select a*b b from t_cte where a in (select b from x))
  )
 )
);

-- case 8: cte replace during pull up
explain (verbose on, costs off)
select 1 from t_cte t1 where a = (with tmp as (select b from t_cte t2 where t2.b=t1.a)
select count(b) from tmp tmp1
where b>(select count(*) from tmp tmp2 where tmp2.b=tmp1.b))+1;

select 1 from t_cte t1 where a = (with tmp as (select b from t_cte t2 where t2.b=t1.a)
select count(b) from tmp tmp1
where b>(select count(*) from tmp tmp2 where tmp2.b=tmp1.b))+1;

-- case 9: two level simple sub-query
explain (verbose on, costs off)
with x as
(select a,b from t_cte)
select sum(b) from
 (select a*b b from x
  where a in
  (select b from x 
   where a in (select b from x)
  )
 );

with x as
(select a,b from t_cte)
select sum(b) from
 (select a*b b from x 
  where a in
  (select b from x 
   where a in (select b from x)
  )
 );

-- case 10: cte with same alias referenced by other cte
explain (verbose on, costs off)
with x as (select a,b from t_cte),
x2 as (select * from x)
select * from x x1
where exists(
 with x as (select a+b a, a-b b from t_cte)
 select 1 from x2 where x2.a=x1.a
 )
order by 1,2;

with x as (select a,b from t_cte),
x2 as (select * from x)
select * from x x1
where exists(
 with x as (select a+b a, a-b b from t_cte)
 select 1 from x2 where x2.a=x1.a
 )
order by 1,2;

-- case 11: correlated cte with same alias referenced by other cte
explain (verbose on, costs off)
select * from t_cte t1
where exists (
 with x as (select a,b from t_cte t2 where t1.a=t2.a),
 x2 as (select * from x)
 select * from x x1 where exists(
  with x as (select a+b a, a-b b from t_cte)
  select 1 from x2 where x2.a=x1.a
  )
 )
order by 1,2;

select * from t_cte t1
where exists (
 with x as (select a,b from t_cte t2 where t1.a=t2.a),
 x2 as (select * from x)
 select * from x x1 where exists(
  with x as (select a+b a, a-b b from t_cte)
  select 1 from x2 where x2.a=x1.a
  )
 )
order by 1,2;

-- case 12: cte referencing correlated cte referenced in different level 
explain (verbose on, costs off)
select * from t_cte t1
where exists (
 with x as (select a,b from t_cte t2 where t1.a=t2.a),
 x2 as (select * from x)
 select * from x2 x1 where exists(
  with x as (select a+b a, a-b b from t_cte)
  select 1 from x2 where x2.a=x1.a
  )
 )
order by 1,2;

select * from t_cte t1
where exists (
 with x as (select a,b from t_cte t2 where t1.a=t2.a),
 x2 as (select * from x)
 select * from x2 x1 where exists(
  with x as (select a+b a, a-b b from t_cte)
  select 1 from x2 where x2.a=x1.a
  )
 )
order by 1,2;

-- case 13: cte used in set op
explain (verbose on, costs off)
select * from t_cte t1
where exists (
 with x as (select a,b from t_cte t2 where t1.a=t2.a),
 x2 as (select * from x)
 select * from x2 x1 where exists(
  with x as (select a+b a, a-b b from t_cte)
  select 1 from x2 where x2.a=x1.a
  )
 union all
 select 1,2 from x2
 )
order by 1,2;

select * from t_cte t1
where exists (
 with x as (select a,b from t_cte t2 where t1.a=t2.a),
 x2 as (select * from x)
 select * from x2 x1 where exists(
  with x as (select a+b a, a-b b from t_cte)
  select 1 from x2 where x2.a=x1.a
  )
 union all
 select 1,2 from x2
 )
order by 1,2;

explain (verbose on, costs off)
select * from t_cte t1
where exists (
 with x as (select a,b from t_cte t2 where t1.a=t2.a),
 x2 as (select * from x)
 select * from x2 x1 where exists(
  with x as (select a+b a, a-b b from t_cte)
  select 1 from x2 where x2.a=x1.a
  )
 union all
 select 1,2 from x
 )
order by 1,2;

select * from t_cte t1
where exists (
 with x as (select a,b from t_cte t2 where t1.a=t2.a),
 x2 as (select * from x)
 select * from x2 x1 where exists(
  with x as (select a+b a, a-b b from t_cte)
  select 1 from x2 where x2.a=x1.a
  )
 union all
 select 1,2 from x
 )
order by 1,2;

explain (verbose on, costs off)
select * from t_cte t1
where exists (
 with x as (select a,b from t_cte t2 where t1.a=t2.a),
 x2 as (select * from x)
 select * from x2 x1 where exists(
  with x as (select a+b a, a-b b from t_cte)
  select 1 from x2 where x2.a=x1.a
  )
 union
 select 1,2 from x2
 )
order by 1,2;

select * from t_cte t1
where exists (
 with x as (select a,b from t_cte t2 where t1.a=t2.a),
 x2 as (select * from x)
 select * from x2 x1 where exists(
  with x as (select a+b a, a-b b from t_cte)
  select 1 from x2 where x2.a=x1.a
  )
 union
 select 1,2 from x2
 )
order by 1,2;

explain (verbose on, costs off)
select * from t_cte t1
where exists (
 with x as (select a,b from t_cte t2 where t1.a=t2.a),
 x2 as (select * from x)
 select * from x2 x1 where exists(
  with x as (select a+b a, a-b b from t_cte)
  select 1 from x2 where x2.a=x1.a
  )
 intersect 
 select 1,2 from x
 )
order by 1,2;

select * from t_cte t1
where exists (
 with x as (select a,b from t_cte t2 where t1.a=t2.a),
 x2 as (select * from x)
 select * from x2 x1 where exists(
  with x as (select a+b a, a-b b from t_cte)
  select 1 from x2 where x2.a=x1.a
  )
 intersect 
 select 1,2 from x
 )
order by 1,2;

-- case 14: cte referencing cte with same alias
explain (verbose on, costs off)
with tmp as (select a from t_cte)
select * from tmp t1 where exists
(with tmp as (select a*2 a from tmp)
select a from tmp t2 where t2.a=t1.a);

with tmp as (select a from t_cte)
select * from tmp t1 where exists
(with tmp as (select a*2 a from tmp)
select a from tmp t2 where t2.a=t1.a)
order by 1;

explain (verbose on, costs off)
with tmp as (select a from t_cte)
select sum(a) from
(with tmp as (select a*2 a from tmp)
select a from tmp t1
where exists
(select a from tmp t2 where t2.a=t1.a));

with tmp as (select a from t_cte)
select sum(a) from
(with tmp as (select a*2 a from tmp)
select a from tmp t1
where exists
(select a from tmp t2 where t2.a=t1.a));

explain (verbose on, costs off)
with tmp as (select a from t_cte)
select sum(a) from
(with tmp2 as (select a+2 a from tmp),
 tmp as (select a*2 a from tmp2)
 select a from tmp t1
 where exists
 (select a from tmp t2 where t2.a=t1.a)
);

with tmp as (select a from t_cte)
select sum(a) from
(with tmp2 as (select a+2 a from tmp),
 tmp as (select a*2 a from tmp2)
 select a from tmp t1
 where exists
 (select a from tmp t2 where t2.a=t1.a)
);

explain (verbose on, costs off)
with tmp as (select a from t_cte) 
select sum(a) from 
(with tmp2 as (select a+2 a from tmp t1
 where exists
 (select 1 from tmp t2 where t1.a=t2.a)), 
 tmp as (select a*2 a from tmp2)
 select a from tmp t1
 where exists
 (select a from tmp t2 where t2.a=t1.a)
);

with tmp as (select a from t_cte) 
select sum(a) from 
(with tmp2 as (select a+2 a from tmp t1
 where exists
 (select 1 from tmp t2 where t1.a=t2.a)), 
 tmp as (select a*2 a from tmp2)
 select a from tmp t1
 where exists
 (select a from tmp t2 where t2.a=t1.a)
);

explain (verbose on, costs off)
with tmp as
(select * from t_cte)
select *
from (with tmp as (select a
from t_cte t1
where b in
(select b
from tmp
))
select * from
(select count(*) from tmp));

with tmp as
(select * from t_cte)
select *
from (with tmp as (select a
from t_cte t1
where b in
(select b
from tmp
))
select * from
(select count(*) from tmp));

drop table t_cte;
drop table x cascade;
