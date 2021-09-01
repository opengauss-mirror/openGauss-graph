\! gs_ktool -d all
\! gs_ktool -g
\! gs_ktool -g

CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE CLIENT MASTER KEY MyCMK1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/2" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK1 WITH VALUES (CLIENT_MASTER_KEY = MyCMK1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE IF NOT EXISTS tr1(i1 INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) , i2 INT);
CREATE TABLE IF NOT EXISTS tr2(i1 INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) , i2 INT);
CREATE TABLE IF NOT EXISTS tr3(i1 INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC) , i2 INT);
CREATE TABLE IF NOT EXISTS tr4(i1 INT, i2 INT);
INSERT INTO tr1 (i1, i2) VALUES (7, 8);
INSERT INTO tr1 VALUES (7,8);
INSERT INTO tr2 (i1, i2) VALUES (12, 13);
INSERT INTO tr2 VALUES (15,16);
INSERT INTO tr2 (i1, i2) VALUES (22, 23), (24, 25), (26,27);
INSERT INTO tr2 VALUES (35,36), (36,37), (38,39);
INSERT INTO tr3 VALUES (45,46), (46,47), (48,49);
INSERT INTO tr4 VALUES (51,52), (53,54), (55,56);
select * from tr1 order by i2;
select * from tr2 order by i2;
INSERT INTO tr2 (i1, i2) select * from tr1;
select * from tr2 order by i2;
--unsupport
INSERT INTO tr2 (i1, i2) select * from tr1 where i1 = 7;
select * from tr2 order by i2;
INSERT INTO tr2 (i1, i2) select * from tr3;
select * from tr2 order by i2;

INSERT INTO tr4 select * from tr3;
INSERT INTO tr3 select * from tr4;

CREATE TABLE creditcard_info (id_number    int, name  text ,credit_card  varchar(19));
CREATE TABLE creditcard_info1 (id_number    int, name text encrypted with (column_encryption_key = MyCEK, encryption_type = DETERMINISTIC), credit_card  varchar(19) encrypted with (column_encryption_key = MyCEK1, encryption_type = DETERMINISTIC));
CREATE TABLE creditcard_info2 (id_number    int, name text encrypted with (column_encryption_key = MyCEK, encryption_type = DETERMINISTIC), credit_card  varchar(19) encrypted with (column_encryption_key = MyCEK, encryption_type = DETERMINISTIC));

insert into creditcard_info  select * from creditcard_info1 ;
insert into creditcard_info1  select * from creditcard_info ;
insert into creditcard_info1  select * from creditcard_info2 ;

drop table creditcard_info;
drop table creditcard_info1;
drop table creditcard_info2;
DROP TABLE tr1;
DROP TABLE tr2;
DROP TABLE tr3; 
DROP TABLE tr4; 
DROP COLUMN ENCRYPTION KEY MyCEK;
DROP COLUMN ENCRYPTION KEY MyCEK1;
DROP CLIENT MASTER KEY MyCMK CASCADE;
DROP CLIENT MASTER KEY MyCMK1 CASCADE;

\! gs_ktool -d all