\! gs_ktool -d all
\! gs_ktool -g

CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE ce_customer (
    ce_customer_id integer NOT NULL,
    id integer  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
    first_name character varying(45) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
    last_name character varying(45) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);
insert into ce_customer values (770, 1234, 'Ido''s', 'shemer');
insert into ce_customer (ce_customer_id, id, first_name, last_name) values (771, 1234, 'Eli''s', 'shemer');
select * from ce_customer order by ce_customer_id;
select * from ce_customer where first_name = 'Ido''s';
drop table ce_customer;
DROP CLIENT MASTER KEY mycmk CASCADE;

\! gs_ktool -d all