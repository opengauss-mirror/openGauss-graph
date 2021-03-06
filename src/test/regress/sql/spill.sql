-- predictability
SET synchronous_commit = on;

execute direct on (datanode1)'SELECT ''init'' FROM pg_create_logical_replication_slot(''regression_slot'', ''test_decoding'');';
CREATE TABLE spill_test(data text);

-- consume DDL
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';
-- spilling main xact
BEGIN;
INSERT INTO spill_test SELECT 'serialize-topbig--1:'||g.i FROM generate_series(1, 5000) g(i);
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

-- spilling subxact, nothing in main
BEGIN;
SAVEPOINT s;
INSERT INTO spill_test SELECT 'serialize-subbig--1:'||g.i FROM generate_series(1, 5000) g(i);
RELEASE SAVEPOINT s;
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

-- spilling subxact, spilling main xact
BEGIN;
SAVEPOINT s;
INSERT INTO spill_test SELECT 'serialize-subbig-topbig--1:'||g.i FROM generate_series(1, 5000) g(i);
RELEASE SAVEPOINT s;
INSERT INTO spill_test SELECT 'serialize-subbig-topbig--2:'||g.i FROM generate_series(5001, 10000) g(i);
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

-- spilling subxact, non-spilling main xact
BEGIN;
SAVEPOINT s;
INSERT INTO spill_test SELECT 'serialize-subbig-topsmall--1:'||g.i FROM generate_series(1, 5000) g(i);
RELEASE SAVEPOINT s;
INSERT INTO spill_test SELECT 'serialize-subbig-topsmall--2:'||g.i FROM generate_series(5001, 5001) g(i);
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

-- not-spilling subxact, spilling main xact
BEGIN;
SAVEPOINT s;
INSERT INTO spill_test SELECT 'serialize-subbig-topbig--1:'||g.i FROM generate_series(1, 5000) g(i);
RELEASE SAVEPOINT s;
INSERT INTO spill_test SELECT 'serialize-subbig-topbig--2:'||g.i FROM generate_series(5001, 10000) g(i);
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

-- spilling main xact, spilling subxact
BEGIN;
INSERT INTO spill_test SELECT 'serialize-topbig-subbig--1:'||g.i FROM generate_series(1, 5000) g(i);
SAVEPOINT s;
INSERT INTO spill_test SELECT 'serialize-topbig-subbig--2:'||g.i FROM generate_series(5001, 10000) g(i);
RELEASE SAVEPOINT s;
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

-- spilling main xact, not spilling subxact
BEGIN;
INSERT INTO spill_test SELECT 'serialize-topbig-subsmall--1:'||g.i FROM generate_series(1, 5000) g(i);
SAVEPOINT s;
INSERT INTO spill_test SELECT 'serialize-topbig-subsmall--2:'||g.i FROM generate_series(5001, 5001) g(i);
RELEASE SAVEPOINT s;
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

-- spilling subxact, followed by another spilling subxact
BEGIN;
SAVEPOINT s1;
INSERT INTO spill_test SELECT 'serialize-subbig-subbig--1:'||g.i FROM generate_series(1, 5000) g(i);
RELEASE SAVEPOINT s1;
SAVEPOINT s2;
INSERT INTO spill_test SELECT 'serialize-subbig-subbig--2:'||g.i FROM generate_series(5001, 10000) g(i);
RELEASE SAVEPOINT s2;
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

-- spilling subxact, followed by not spilling subxact
BEGIN;
SAVEPOINT s1;
INSERT INTO spill_test SELECT 'serialize-subbig-subsmall--1:'||g.i FROM generate_series(1, 5000) g(i);
RELEASE SAVEPOINT s1;
SAVEPOINT s2;
INSERT INTO spill_test SELECT 'serialize-subbig-subsmall--2:'||g.i FROM generate_series(5001, 5001) g(i);
RELEASE SAVEPOINT s2;
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

-- not spilling subxact, followed by spilling subxact
BEGIN;
SAVEPOINT s1;
INSERT INTO spill_test SELECT 'serialize-subsmall-subbig--1:'||g.i FROM generate_series(1, 1) g(i);
RELEASE SAVEPOINT s1;
SAVEPOINT s2;
INSERT INTO spill_test SELECT 'serialize-subsmall-subbig--2:'||g.i FROM generate_series(2, 5001) g(i);
RELEASE SAVEPOINT s2;
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

-- spilling subxact, containing another spilling subxact
BEGIN;
SAVEPOINT s1;
INSERT INTO spill_test SELECT 'serialize-nested-subbig-subbig--1:'||g.i FROM generate_series(1, 5000) g(i);
SAVEPOINT s2;
INSERT INTO spill_test SELECT 'serialize-nested-subbig-subbig--2:'||g.i FROM generate_series(5001, 10000) g(i);
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

-- spilling subxact, containing a not spilling subxact
BEGIN;
SAVEPOINT s1;
INSERT INTO spill_test SELECT 'serialize-nested-subbig-subsmall--1:'||g.i FROM generate_series(1, 5000) g(i);
SAVEPOINT s2;
INSERT INTO spill_test SELECT 'serialize-nested-subbig-subsmall--2:'||g.i FROM generate_series(5001, 5001) g(i);
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

-- not spilling subxact, containing a spilling subxact
BEGIN;
SAVEPOINT s1;
INSERT INTO spill_test SELECT 'serialize-nested-subsmall-subbig--1:'||g.i FROM generate_series(1, 1) g(i);
SAVEPOINT s2;
INSERT INTO spill_test SELECT 'serialize-nested-subsmall-subbig--2:'||g.i FROM generate_series(2, 5001) g(i);
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

-- not spilling subxact, containing a spilling subxact that aborts and one that commits
BEGIN;
SAVEPOINT s1;
INSERT INTO spill_test SELECT 'serialize-nested-subbig-subbigabort--1:'||g.i FROM generate_series(1, 5000) g(i);
SAVEPOINT s2;
INSERT INTO spill_test SELECT 'serialize-nested-subbig-subbigabort--2:'||g.i FROM generate_series(5001, 10000) g(i);
ROLLBACK TO SAVEPOINT s2;
SAVEPOINT s3;
INSERT INTO spill_test SELECT 'serialize-nested-subbig-subbigabort-subbig-3:'||g.i FROM generate_series(5001, 10000) g(i);
RELEASE SAVEPOINT s1;
COMMIT;
execute direct on (datanode1)'SELECT data FROM pg_logical_slot_get_changes(''regression_slot'', NULL, NULL, ''include-xids'', ''0'', ''skip-empty-xacts'', ''1'') limit 10;';

DROP TABLE spill_test;

execute direct on (datanode1)'SELECT pg_drop_replication_slot(''regression_slot'');';
