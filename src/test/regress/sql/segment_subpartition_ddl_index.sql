--
----test index is Ok when use ddl grammer for subpartition----
--
DROP SCHEMA segment_subpartition_ddl_index CASCADE;
CREATE SCHEMA segment_subpartition_ddl_index;
SET CURRENT_SCHEMA TO segment_subpartition_ddl_index;

SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = OFF;

--
--test for add/drop partition/subpartition
--
--1. first, we create subpartitioned table, and index on the table
CREATE TABLE range_list_sales1
(
    product_id     INT4,
    customer_id    INT4,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
) WITH (SEGMENT=ON)
PARTITION BY RANGE (customer_id) SUBPARTITION BY LIST (channel_id)
(
    PARTITION customer1 VALUES LESS THAN (200)
    (
        SUBPARTITION customer1_channel1 VALUES ('0', '1', '2'),
        SUBPARTITION customer1_channel2 VALUES ('3', '4', '5'),
        SUBPARTITION customer1_channel3 VALUES ('6', '7', '8'),
        SUBPARTITION customer1_channel4 VALUES ('9')
    ),
    PARTITION customer2 VALUES LESS THAN (500)
    (
        SUBPARTITION customer2_channel1 VALUES ('0', '1', '2', '3', '4'),
        SUBPARTITION customer2_channel2 VALUES (DEFAULT)
    ),
    PARTITION customer3 VALUES LESS THAN (800),
    PARTITION customer4 VALUES LESS THAN (1200)
    (
        SUBPARTITION customer4_channel1 VALUES ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    )
);
INSERT INTO range_list_sales1 SELECT generate_series(1,1000),
                                     generate_series(1,1000),
                                     date_pli('2008-01-01', generate_series(1,1000)),
                                     generate_series(1,1000)%10,
                                     generate_series(1,1000)%10,
                                     generate_series(1,1000)%1000,
                                     generate_series(1,1000);

CREATE INDEX range_list_sales1_idx1 ON range_list_sales1(product_id, customer_id) GLOBAL;
CREATE INDEX range_list_sales1_idx2 ON range_list_sales1(channel_id) GLOBAL;
CREATE INDEX range_list_sales1_idx3 ON range_list_sales1(customer_id) LOCAL;
CREATE INDEX range_list_sales1_idx4 ON range_list_sales1(time_id, type_id) LOCAL;

EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx1) */ COUNT(product_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx1) */ COUNT(product_id) FROM range_list_sales1;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx2) */ COUNT(channel_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx2) */ COUNT(channel_id) FROM range_list_sales1;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx3) */ COUNT(customer_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx3) */ COUNT(customer_id) FROM range_list_sales1;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx4) */ COUNT(time_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx4) */ COUNT(time_id) FROM range_list_sales1;

--2. add partition/subpartition will not influence the index
ALTER TABLE range_list_sales1 ADD PARTITION customer5 VALUES LESS THAN (1500)
    (
        SUBPARTITION customer5_channel1 VALUES ('0', '1', '2'),
        SUBPARTITION customer5_channel2 VALUES ('3', '4', '5'),
        SUBPARTITION customer5_channel3 VALUES ('6', '7', '8')
    );
ALTER TABLE range_list_sales1 ADD PARTITION customer6 VALUES LESS THAN (MAXVALUE);
ALTER TABLE range_list_sales1 MODIFY PARTITION customer5 ADD SUBPARTITION customer5_channel4 VALUES ('9');
INSERT INTO range_list_sales1 SELECT generate_series(1001,2000),
                                     generate_series(1,1000),
                                     date_pli('2008-01-01', generate_series(1,1000)),
                                     generate_series(1,1000)%10,
                                     generate_series(1,1000)%10,
                                     generate_series(1,1000)%1000,
                                     generate_series(1,1000);

EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx1) */ COUNT(product_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx1) */ COUNT(product_id) FROM range_list_sales1;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx2) */ COUNT(channel_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx2) */ COUNT(channel_id) FROM range_list_sales1;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx3) */ COUNT(customer_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx3) */ COUNT(customer_id) FROM range_list_sales1;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx4) */ COUNT(time_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx4) */ COUNT(time_id) FROM range_list_sales1;

--3. drop partition/subpartition update global index
ALTER TABLE range_list_sales1 DROP PARTITION customer3 UPDATE GLOBAL INDEX;
ALTER TABLE range_list_sales1 DROP PARTITION FOR (700) UPDATE GLOBAL INDEX; --customer4
ALTER TABLE range_list_sales1 DROP SUBPARTITION FOR (700, '9') UPDATE GLOBAL INDEX; --customer5_channel4

EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx1) */ COUNT(product_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx1) */ COUNT(product_id) FROM range_list_sales1;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx2) */ COUNT(channel_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx2) */ COUNT(channel_id) FROM range_list_sales1;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx3) */ COUNT(customer_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx3) */ COUNT(customer_id) FROM range_list_sales1;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx4) */ COUNT(time_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx4) */ COUNT(time_id) FROM range_list_sales1;

--4. if drop partition without update global index, the gpi will be invalid, we can rebuild the index
ALTER TABLE range_list_sales1 DROP PARTITION FOR (1600);

EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx1) */ COUNT(product_id) FROM range_list_sales1;
ALTER INDEX range_list_sales1_idx1 REBUILD;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx1) */ COUNT(product_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx1) */ COUNT(product_id) FROM range_list_sales1;

EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx2) */ COUNT(channel_id) FROM range_list_sales1;
ALTER INDEX range_list_sales1_idx2 REBUILD;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx2) */ COUNT(channel_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx2) */ COUNT(channel_id) FROM range_list_sales1;

EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx3) */ COUNT(customer_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx3) */ COUNT(customer_id) FROM range_list_sales1;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx4) */ COUNT(time_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx4) */ COUNT(time_id) FROM range_list_sales1;

--5. if drop subpartition without update global index, the gpi will be invalid, we can rebuild the index
ALTER TABLE range_list_sales1 DROP SUBPARTITION customer5_channel3;

EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx1) */ COUNT(product_id) FROM range_list_sales1;
ALTER INDEX range_list_sales1_idx1 REBUILD;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx1) */ COUNT(product_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx1) */ COUNT(product_id) FROM range_list_sales1;

EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx2) */ COUNT(channel_id) FROM range_list_sales1;
ALTER INDEX range_list_sales1_idx2 REBUILD;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx2) */ COUNT(channel_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx2) */ COUNT(channel_id) FROM range_list_sales1;

EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx3) */ COUNT(customer_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx3) */ COUNT(customer_id) FROM range_list_sales1;
EXPLAIN(costs off) SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx4) */ COUNT(time_id) FROM range_list_sales1;
SELECT /*+ indexonlyscan(range_list_sales1 range_list_sales1_idx4) */ COUNT(time_id) FROM range_list_sales1;

DROP TABLE range_list_sales1;

--finish, clean the environment
DROP SCHEMA segment_subpartition_ddl_index CASCADE;
RESET CURRENT_SCHEMA;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
