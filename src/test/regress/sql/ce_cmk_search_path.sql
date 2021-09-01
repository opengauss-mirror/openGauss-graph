\! gs_ktool -d all
\! gs_ktool -g

-- create another user
DROP ROLE IF EXISTS newuser;
CREATE USER newuser PASSWORD 'gauss@123';

-- create schema
DROP SCHEMA IF EXISTS testns CASCADE;
CREATE SCHEMA testns;
SET search_path to testns;

-- grant privileges on schema (ALL = USAGE, CREATE)
GRANT ALL ON SCHEMA testns TO newuser;

-- CREATE CMK
CREATE CLIENT MASTER KEY MyCMK1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);

show search_path;

CREATE SCHEMA testsp;

SET search_path to testsp;

show search_path;

-- CREATE CEK
CREATE COLUMN ENCRYPTION KEY MyCEK1 WITH VALUES (CLIENT_MASTER_KEY = public.MyCMK1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

select count(*), 'count' from gs_client_global_keys;
select count(*), 'count' from gs_column_keys;


-- change search path to testns and create a table
SET search_path to testns;
CREATE COLUMN ENCRYPTION KEY MyCEK1 WITH VALUES (CLIENT_MASTER_KEY = MyCMK1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
-- create TABLE 
CREATE TABLE acltest1 (x int, x2 varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC));

SELECT has_cmk_privilege('newuser', 'testns.MyCMK1', 'USAGE');
SELECT has_cek_privilege('newuser', 'testns.MyCEK1', 'USAGE');
SELECT has_schema_privilege('newuser', 'testns', 'USAGE');
SELECT has_schema_privilege('newuser', 'testns', 'CREATE');
SELECT has_table_privilege('newuser', 'acltest1', 'INSERT, SELECT, UPDATE, DELETE, TRUNCATE, TRIGGER');

RESET SESSION AUTHORIZATION;
DROP TABLE acltest1;
REVOKE USAGE ON COLUMN_ENCRYPTION_KEY MyCEK1 FROM newuser;
REVOKE USAGE ON CLIENT_MASTER_KEY MyCMK1 FROM newuser;
DROP COLUMN ENCRYPTION KEY MyCEK1;
DROP CLIENT MASTER KEY MyCMK1;
DROP SCHEMA IF EXISTS testns CASCADE;
DROP SCHEMA IF EXISTS testsp CASCADE;
DROP SCHEMA IF EXISTS newuser CASCADE;
DROP ROLE IF EXISTS newuser;

\! gs_ktool -d all