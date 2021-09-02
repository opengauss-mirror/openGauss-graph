\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY MyCMK CASCADE;
CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE COLUMN ENCRYPTION KEY MyCEK2 WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE so_headers (
   id INTEGER unique ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
   customer_id INTEGER  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK2, ENCRYPTION_TYPE = DETERMINISTIC),
   ship_to VARCHAR (255)
);

CREATE TABLE so_items (
   item_id INTEGER NOT NULL,
   so_id INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
   product_id INTEGER,
   qty INTEGER,
   net_price NUMERIC,
   PRIMARY KEY (item_id, so_id),
   FOREIGN KEY (so_id) REFERENCES so_headers (id)
);

CREATE TABLE so_items_r (
  item_id INTEGER NOT NULL,
  so_id int4  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) REFERENCES so_headers(id) ON DELETE RESTRICT,
  product_id INTEGER,
  qty INTEGER,
  net_price numeric,
  PRIMARY KEY (item_id,so_id)
);
CREATE TABLE so_items_c (
  item_id int4 NOT NULL,   
  so_id int4  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) REFERENCES so_headers(id) ON DELETE CASCADE,
  product_id int4,
  qty int4,
  net_price numeric,
  PRIMARY KEY (item_id,so_id)
);

ALTER TABLE IF EXISTS so_headers ADD CONSTRAINT so_headers_unique1 UNIQUE (id,customer_id);
CREATE TABLE payments (
   pay_id int,
   so_id INTEGER ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
   customer_id INTEGER  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK2, ENCRYPTION_TYPE = DETERMINISTIC),
   FOREIGN KEY (so_id, customer_id) REFERENCES so_headers (id, customer_id)
);

CREATE TABLE so_items_a (
  item_id INTEGER NOT NULL,
  so_id int4  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
  product_id INTEGER,
  qty INTEGER,
  net_price numeric,
  PRIMARY KEY (item_id,so_id)
);

INSERT INTO so_headers VALUES (1,101, 'Vina');
INSERT INTO so_headers VALUES (2,103, 'Paris');

INSERT INTO so_items VALUES (10001, 1, 1001, 100, 37.28);
INSERT INTO so_items VALUES (10002, 6, 1001, 100, 37.28);
INSERT INTO so_items VALUES (10003, 2, 1001, 100, 37.28);
SELECT * from so_items ORDER BY item_id;

INSERT INTO so_items_r VALUES (10001, 1, 1001, 100, 37.28);
INSERT INTO so_items_r VALUES (10002, 6, 1001, 100, 37.28);
SELECT * from so_items_r ORDER BY item_id;
INSERT INTO so_items_a VALUES (10001, 1, 1001, 100, 37.28);
INSERT INTO so_items_a VALUES (10002, 6, 1001, 100, 37.28);
INSERT INTO so_items_a VALUES (10001, 1, 1001, 110, 36.28);
SELECT * from so_items_a ORDER BY item_id;
INSERT INTO so_items_c VALUES (10001, 1, 1001, 100, 37.28);
INSERT INTO so_items_c VALUES (10002, 6, 1001, 100, 37.28);
INSERT INTO so_items_c VALUES (10001, 1, 1011, 101, 36.28);
SELECT * from so_items_c ORDER BY item_id;

DELETE from so_headers where id = 2;

SELECT * from so_items ORDER BY item_id;
SELECT * from so_items_a ORDER BY item_id;
SELECT * from so_items_r ORDER BY item_id;
SELECT * from so_items_c ORDER BY item_id;

INSERT INTO payments VALUES (100001, 1, 101);
INSERT INTO payments VALUES (100002, 1, 102);
SELECT * from payments ORDER BY pay_id;

DROP TABLE so_items;
DROP TABLE so_items_r;
DROP TABLE so_items_a;
DROP TABLE so_items_c;
DROP TABLE payments;
DROP TABLE so_headers;
DROP CLIENT MASTER KEY MyCMK CASCADE;

\! gs_ktool -d all