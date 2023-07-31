-- row table pg_table_size
create schema table_size_schema;
CREATE TABLE table_size_schema.normal_table(id int);
CREATE TABLE table_size_schema.compressed_table_1024(id int) WITH(compresstype=2, compress_chunk_size=1024);
CREATE TABLE table_size_schema.compressed_table_2048(id int) WITH(compresstype=2, compress_chunk_size=2048);
CREATE TABLE table_size_schema.compressed_table_4096(id int) WITH(compresstype=2, compress_chunk_size=4096);
select pg_table_size('table_size_schema.normal_table');
select pg_table_size('table_size_schema.compressed_table_1024');
select pg_table_size('table_size_schema.compressed_table_2048');
select pg_table_size('table_size_schema.compressed_table_4096');
drop schema table_size_schema cascade;

-- partition table pg_table_size
create schema partition_table_size_schema;
create table partition_table_size_schema.normal_partition(INV_DATE_SK integer)
partition by range(inv_date_sk)(partition p0 values less than(5000),partition p1 values less than(10000));
create table partition_table_size_schema.compressed_partition_1024(INV_DATE_SK integer)
WITH(compresstype=2, compress_chunk_size=1024)
partition by range(inv_date_sk)(partition p0 values less than(5000),partition p1 values less than(10000));
create table partition_table_size_schema.compressed_partition_2048(INV_DATE_SK integer)
WITH(compresstype=2, compress_chunk_size=2048)
partition by range(inv_date_sk)(partition p0 values less than(5000),partition p1 values less than(10000));
create table partition_table_size_schema.compressed_partition_4096(INV_DATE_SK integer)
WITH(compresstype=2, compress_chunk_size=4096)
partition by range(inv_date_sk)(partition p0 values less than(5000),partition p1 values less than(10000));
select pg_table_size('partition_table_size_schema.normal_partition');
select pg_table_size('partition_table_size_schema.compressed_partition_1024');
select pg_table_size('partition_table_size_schema.compressed_partition_2048');
select pg_table_size('partition_table_size_schema.compressed_partition_4096');
drop schema partition_table_size_schema cascade;
