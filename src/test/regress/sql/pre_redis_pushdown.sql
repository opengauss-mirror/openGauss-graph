create table source_table(c1 int, c2 int);
insert into source_table values(generate_series(1,10000),generate_series(1,30));