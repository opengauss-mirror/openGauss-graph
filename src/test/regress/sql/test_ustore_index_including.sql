/*
 * 1.test CREATE INDEX
 */

-- Regular index with included columns
set enable_default_ustore_table = on;
CREATE TABLE tbl (c1 int, c2 int, c3 int, c4 box);
INSERT INTO tbl SELECT x, 2*x, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
CREATE INDEX tbl_idx ON tbl using ubtree (c1, c2) INCLUDE (c3,c4);
-- must fail because of intersection of key and included columns
CREATE INDEX tbl_idx ON tbl using ubtree (c1, c2) INCLUDE (c1,c3);
SELECT pg_get_indexdef(i.indexrelid)
FROM pg_index i JOIN pg_class c ON i.indexrelid = c.oid
WHERE i.indrelid = 'tbl'::regclass ORDER BY c.relname;
DROP TABLE tbl;

-- Unique index and unique constraint
CREATE TABLE tbl (c1 int, c2 int, c3 int, c4 box);
INSERT INTO tbl SELECT x, 2*x, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
CREATE UNIQUE INDEX tbl_idx_unique ON tbl using ubtree (c1, c2) INCLUDE (c3, c4);
ALTER TABLE tbl add UNIQUE USING INDEX tbl_idx_unique;
ALTER TABLE tbl add UNIQUE (c1, c2) INCLUDE (c3, c4);
SELECT pg_get_indexdef(i.indexrelid)
FROM pg_index i JOIN pg_class c ON i.indexrelid = c.oid
WHERE i.indrelid = 'tbl'::regclass ORDER BY c.relname;
DROP TABLE tbl;

-- Unique index and unique constraint. Both must fail.
CREATE TABLE tbl (c1 int, c2 int, c3 int, c4 box);
INSERT INTO tbl SELECT 1, 2, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
CREATE UNIQUE INDEX tbl_idx_unique ON tbl using ubtree (c1, c2) INCLUDE (c3, c4);
ALTER TABLE tbl add UNIQUE (c1, c2) INCLUDE (c3, c4);
DROP TABLE tbl;

-- PK constraint
CREATE TABLE tbl (c1 int, c2 int, c3 int, c4 box);
INSERT INTO tbl SELECT 1, 2*x, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
ALTER TABLE tbl add PRIMARY KEY (c1, c2) INCLUDE (c3, c4);
SELECT pg_get_indexdef(i.indexrelid)
FROM pg_index i JOIN pg_class c ON i.indexrelid = c.oid
WHERE i.indrelid = 'tbl'::regclass ORDER BY c.relname;
DROP TABLE tbl;

CREATE TABLE tbl (c1 int, c2 int, c3 int, c4 box);
INSERT INTO tbl SELECT 1, 2*x, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
CREATE UNIQUE INDEX tbl_idx_unique ON tbl using ubtree (c1, c2) INCLUDE (c3, c4);
ALTER TABLE tbl add PRIMARY KEY USING INDEX tbl_idx_unique;
SELECT pg_get_indexdef(i.indexrelid)
FROM pg_index i JOIN pg_class c ON i.indexrelid = c.oid
WHERE i.indrelid = 'tbl'::regclass ORDER BY c.relname;
DROP TABLE tbl;

-- PK constraint. Must fail.
CREATE TABLE tbl (c1 int, c2 int, c3 int, c4 box);
INSERT INTO tbl SELECT 1, 2, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
ALTER TABLE tbl add PRIMARY KEY (c1, c2) INCLUDE (c3, c4);
DROP TABLE tbl;

/*
 * 2. Test CREATE TABLE with constraint
 */
CREATE TABLE tbl (c1 int,c2 int, c3 int, c4 box,
				CONSTRAINT covering UNIQUE(c1,c2) INCLUDE(c3,c4));
SELECT indexrelid::regclass, indnatts, indnkeyatts, indisunique, indisprimary, indkey, indclass FROM pg_index WHERE indrelid = 'tbl'::regclass::oid;
SELECT pg_get_constraintdef(oid), conname, conkey, conincluding FROM pg_constraint WHERE conrelid = 'tbl'::regclass::oid;
-- ensure that constraint works
INSERT INTO tbl SELECT 1, 2, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
DROP TABLE tbl;

CREATE TABLE tbl (c1 int,c2 int, c3 int, c4 box,
				CONSTRAINT covering PRIMARY KEY(c1,c2) INCLUDE(c3,c4));
SELECT indexrelid::regclass, indnatts, indnkeyatts, indisunique, indisprimary, indkey, indclass FROM pg_index WHERE indrelid = 'tbl'::regclass::oid;
SELECT pg_get_constraintdef(oid), conname, conkey, conincluding FROM pg_constraint WHERE conrelid = 'tbl'::regclass::oid;
-- ensure that constraint works
INSERT INTO tbl SELECT 1, 2, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
INSERT INTO tbl SELECT 1, NULL, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
INSERT INTO tbl SELECT x, 2*x, NULL, NULL FROM generate_series(1,10) AS x;
DROP TABLE tbl;

CREATE TABLE tbl (c1 int,c2 int, c3 int, c4 box,
				UNIQUE(c1,c2) INCLUDE(c3,c4));
SELECT indexrelid::regclass, indnatts, indnkeyatts, indisunique, indisprimary, indkey, indclass FROM pg_index WHERE indrelid = 'tbl'::regclass::oid;
SELECT pg_get_constraintdef(oid), conname, conkey, conincluding FROM pg_constraint WHERE conrelid = 'tbl'::regclass::oid;
-- ensure that constraint works
INSERT INTO tbl SELECT 1, 2, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
DROP TABLE tbl;

CREATE TABLE tbl (c1 int,c2 int, c3 int, c4 box,
				PRIMARY KEY(c1,c2) INCLUDE(c3,c4));
SELECT indexrelid::regclass, indnatts, indnkeyatts, indisunique, indisprimary, indkey, indclass FROM pg_index WHERE indrelid = 'tbl'::regclass::oid;
SELECT pg_get_constraintdef(oid), conname, conkey, conincluding FROM pg_constraint WHERE conrelid = 'tbl'::regclass::oid;
-- ensure that constraint works
INSERT INTO tbl SELECT 1, 2, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
INSERT INTO tbl SELECT 1, NULL, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
INSERT INTO tbl SELECT x, 2*x, NULL, NULL FROM generate_series(1,10) AS x;
DROP TABLE tbl;

/*
 * 3.0 Test ALTER TABLE DROP COLUMN.
 * Any column deletion leads to index deletion.
 */
CREATE TABLE tbl (c1 int,c2 int, c3 int, c4 int);
CREATE UNIQUE INDEX tbl_idx ON tbl using ubtree(c1, c2, c3, c4);
SELECT indexdef FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;
ALTER TABLE tbl DROP COLUMN c3;
SELECT indexdef FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;
DROP TABLE tbl;

/*
 * 3.1 Test ALTER TABLE DROP COLUMN.
 * Included column deletion leads to the index deletion,
 * AS well AS key columns deletion. It's explained in documentation.
 */
CREATE TABLE tbl (c1 int,c2 int, c3 int, c4 box);
CREATE UNIQUE INDEX tbl_idx ON tbl using ubtree(c1, c2) INCLUDE(c3,c4);
SELECT indexdef FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;
ALTER TABLE tbl DROP COLUMN c3;
SELECT indexdef FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;
DROP TABLE tbl;

/*
 * 3.2 Test ALTER TABLE DROP COLUMN.
 * Included column deletion leads to the index deletion.
 * AS well AS key columns deletion. It's explained in documentation.
 */
CREATE TABLE tbl (c1 int,c2 int, c3 int, c4 box, UNIQUE(c1, c2) INCLUDE(c3,c4));
SELECT indexdef FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;
ALTER TABLE tbl DROP COLUMN c3;
SELECT indexdef FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;
ALTER TABLE tbl DROP COLUMN c1;
SELECT indexdef FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;
DROP TABLE tbl;

/*
 * 4. CREATE INDEX
 */
CREATE TABLE tbl (c1 int,c2 int, c3 int, c4 box, UNIQUE(c1, c2) INCLUDE(c3,c4));
INSERT INTO tbl SELECT x, 2*x, 3*x, box('4,4,4,4') FROM generate_series(1,1000) AS x;
CREATE UNIQUE INDEX on tbl (c1, c2) INCLUDE (c3, c4);
SELECT indexdef FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;
DROP TABLE tbl;

/*
 * 5. REINDEX
 */
CREATE TABLE tbl (c1 int,c2 int, c3 int, c4 box, UNIQUE(c1, c2) INCLUDE(c3,c4));
SELECT indexdef FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;
ALTER TABLE tbl DROP COLUMN c3;
SELECT indexdef FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;
REINDEX INDEX tbl_c1_c2_c3_c4_key;
SELECT indexdef FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;
ALTER TABLE tbl DROP COLUMN c1;
SELECT indexdef FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;
DROP TABLE tbl;

/*
 * 7. Check various AMs. All but ubtree must fail.
 */
CREATE TABLE tbl (c1 int,c2 int, c3 box, c4 box);
CREATE INDEX on tbl USING brin(c1, c2) INCLUDE (c3, c4);
CREATE INDEX on tbl USING gist(c3) INCLUDE (c4);
CREATE INDEX on tbl USING spgist(c3) INCLUDE (c4);
CREATE INDEX on tbl USING gin(c1, c2) INCLUDE (c3, c4);
CREATE INDEX on tbl USING hash(c1, c2) INCLUDE (c3, c4);
CREATE INDEX on tbl USING rtree(c1, c2) INCLUDE (c3, c4);
CREATE INDEX on tbl USING ubtree(c1, c2) INCLUDE (c3, c4);
DROP TABLE tbl;

/*
 * 8. Update, delete values in indexed table.
 */
CREATE TABLE tbl (c1 int, c2 int, c3 int, c4 box);
INSERT INTO tbl SELECT x, 2*x, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
CREATE UNIQUE INDEX tbl_idx_unique ON tbl using ubtree(c1, c2) INCLUDE (c3,c4);
UPDATE tbl SET c1 = 100 WHERE c1 = 2;
UPDATE tbl SET c1 = 1 WHERE c1 = 3;
-- should fail
UPDATE tbl SET c2 = 2 WHERE c1 = 1;
UPDATE tbl SET c3 = 1;
DELETE FROM tbl WHERE c1 = 5 OR c3 = 12;
DROP TABLE tbl;

/*
 * 9. Alter column type.
 */
CREATE TABLE tbl (c1 int,c2 int, c3 int, c4 box, UNIQUE(c1, c2) INCLUDE(c3,c4));
INSERT INTO tbl SELECT x, 2*x, 3*x, box('4,4,4,4') FROM generate_series(1,10) AS x;
ALTER TABLE tbl ALTER c1 TYPE bigint;
ALTER TABLE tbl ALTER c3 TYPE bigint;
\d tbl
DROP TABLE tbl;
set enable_default_ustore_table = off;