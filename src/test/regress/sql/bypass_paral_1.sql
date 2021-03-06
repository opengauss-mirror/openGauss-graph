--
-- bypass parallel test part1
--

start transaction;
update bypass_paral set col3=13 where col1=0 and col2=0;
update bypass_paral set col2=2 where col1=1 and col2=1;
update bypass_paral set col3='ff3' where col1 is null and col2 is not null;
update bypass_paral set col3='ff4' where col2 is null and col1=1;
update bypass_paral set col2=2 where col2 is null and col1 is null;
delete from bypass_paral where col1=1 and col2 is null;
select pg_sleep(2);
commit;

start transaction;
select col3,col4 from bypass_paral2 where col2=0 and col1=0 for update;
select pg_sleep(1);
update bypass_paral2 set col3='ff3',col4='ff4' where col2=0 and col1=0;
commit;
