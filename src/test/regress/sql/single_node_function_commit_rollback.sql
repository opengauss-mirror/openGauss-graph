create or replace function test_without_commit() return void
as
begin
    drop table if exists test_commit;
    create table test_commit(a int);
	insert into test_commit select 2;
end;
/

call test_without_commit();
select * from test_commit;

create or replace function test_empty_sp() return void
as
begin
	insert into test_commit select 1;
	insert into test_commit select 2;
	insert into test_commit select 3;
end;
/

call test_empty_sp();
select * from test_commit;
drop table test_commit;

create or replace function test_commit() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	commit;
end;
/

call test_commit();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_option() return void
shippable
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	commit;
end;
/

call test_commit_insert_option();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_delete() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	insert into test_commit select 2, 2;
	delete from test_commit where a = 1;
	commit;
end;
/

call test_commit_insert_delete();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_update() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	update test_commit set b = 3 where a = 1;
	commit;
end;
/

call test_commit_insert_update();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_update_delete() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	update test_commit set b = 3 where a = 1;
	delete from test_commit where a = 1;
	commit;
end;
/

call test_commit_insert_update_delete();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_delete_update() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	insert into test_commit select 2, 2;
	delete from test_commit where a = 1;
	update test_commit set b = 3 where a = 2;
	commit;
end;
/

call test_commit_insert_delete_update();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_commit() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	commit;
	insert into test_commit select 2, 2;
	commit;
end;
/

call test_commit_commit();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_commit1() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	insert into test_commit select 2, 2;
	commit;
	update test_commit set b = 3 where a = 2;
	delete from test_commit where a = 1;
	commit;
end;
/

call test_commit_commit1();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_rollback() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	commit;
	insert into test_commit select 2, 2;
	rollback;
end;
/

call test_commit_rollback();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_rollback1() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	insert into test_commit select 2, 2;
	commit;
	update test_commit set b = 3 where a = 2;
	delete from test_commit where a = 1;
	rollback;
end;
/

call test_commit_rollback1();
select * from test_commit;
drop table test_commit;

create or replace function test_rollback_commit() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	insert into test_commit select 2, 2;
	rollback;
	drop table if exists test_commit;
	create table test_commit(a int, b int);
	insert into test_commit select 3, 3;
	insert into test_commit select 4, 4;
	insert into test_commit select 5, 5;
	update test_commit set b = 6 where a = 5;
	delete from test_commit where a = 3;
	commit;
end;
/

call test_rollback_commit();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_exception_rollback() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	commit;
	raise exception 'raise exception after commit';
exception
    when others then
	insert into test_commit select 2, 2;
	rollback;
end;
/

call test_commit_insert_exception_rollback();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_exception_commit_rollback() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	commit;
	raise exception 'raise exception after commit';
exception
    when others then
	insert into test_commit select 2, 2;
	commit;
	rollback;
end;
/

call test_commit_insert_exception_commit_rollback();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_raise_commit() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	commit;
	RAISE EXCEPTION 'After commit'; 
end;
/

call test_commit_insert_raise_commit();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_delete_raise_commit() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	insert into test_commit select 2, 2;
	delete from test_commit where a = 1;
	commit;
	RAISE EXCEPTION 'After commit'; 
end;
/

call test_commit_insert_delete_raise_commit();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_update_raise_commit() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	update test_commit set b = 3 where a = 1;
	commit;
	RAISE EXCEPTION 'After commit'; 
end;
/

call test_commit_insert_update_raise_commit();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_update_delete_raise_commit() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	update test_commit set b = 3 where a = 1;
	delete from test_commit where a = 1;
	commit;
	RAISE EXCEPTION 'After commit'; 
end;
/

call test_commit_insert_update_delete_raise_commit();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_delete_update_raise_commit() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	insert into test_commit select 2, 2;
	delete from test_commit where a = 1;
	update test_commit set b = 3 where a = 2;
	commit;
	RAISE EXCEPTION 'After commit'; 
end;
/

call test_commit_insert_delete_update_raise_commit();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_commit_raise() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	RAISE EXCEPTION 'Before commit'; 
	commit;
end;
/

call test_commit_insert_commit_raise();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_delete_commit_raise() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	insert into test_commit select 2, 2;
	delete from test_commit where a = 1;
	RAISE EXCEPTION 'Before commit'; 
	commit;
end;
/

call test_commit_insert_delete_commit_raise();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_update_commit_raise() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	update test_commit set b = 3 where a = 1;
	RAISE EXCEPTION 'Before commit'; 
	commit; 
end;
/

call test_commit_insert_update_commit_raise();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_update_delete_commit_raise() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	update test_commit set b = 3 where a = 1;
	delete from test_commit where a = 1;
	RAISE EXCEPTION 'Before commit'; 
	commit;
end;
/

call test_commit_insert_update_delete_commit_raise();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_insert_delete_update_commit_raise() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	insert into test_commit select 2, 2;
	delete from test_commit where a = 1;
	update test_commit set b = 3 where a = 2;
	RAISE EXCEPTION 'Before commit'; 
	commit;
end;
/

call test_commit_insert_delete_update_commit_raise();
select * from test_commit;
drop table test_commit;

create or replace function test_exception_commit() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	insert into test_commit select 2, 2;
	delete from test_commit where a = 1;
	update test_commit set b = 3 where a = 2;
	commit;
EXCEPTION
    when raise_exception then
		RAISE EXCEPTION '(%)', SQLERRM; 
end;
/

call test_exception_commit();
select * from test_commit;
drop table test_commit;

create or replace function test_exception_commit_commit_raise() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	insert into test_commit select 2, 2;
	delete from test_commit where a = 1;
	update test_commit set b = 3 where a = 2;
	commit;
	RAISE EXCEPTION 'After commit'; 
EXCEPTION
    when raise_exception then
		RAISE EXCEPTION '(%)', SQLERRM; 
end;
/

call test_exception_commit_commit_raise();
select * from test_commit;
drop table test_commit;

create or replace function test_exception_commit_raise_commit() return void
as
begin
drop table if exists test_commit; 
create table test_commit(a int, b int);
insert into test_commit select 1, 1;
insert into test_commit select 2, 2;
delete from test_commit where a = 1;
update test_commit set b = 3 where a = 2;
RAISE EXCEPTION 'After commit'; 
commit;
EXCEPTION
    when raise_exception then
		RAISE EXCEPTION '(%)', SQLERRM; 
end;
/

call test_exception_commit_raise_commit();
select * from test_commit;
drop table test_commit;

create or replace function test_gg_1() return void
as
begin
	drop table if exists test_commit; 
	create table test_commit(a int, b int);
	insert into test_commit select 1, 1;
	insert into test_commit select 2, 2;
	delete from test_commit where a = 1;
	update test_commit set b = 3 where a = 2;
	commit;
	insert into test_commit select 3, 3;
	RAISE EXCEPTION 'After commit'; 
EXCEPTION
    when raise_exception then
		rollback;
		insert into test_commit select 4, 4;
        commit;		
end;
/

call test_gg_1();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_exception() return void
is
begin
    drop table if exists test_commit;
    create table test_commit(a int);
	insert into test_commit select 1;
	commit;
	delete from test_commit;
	commit;
	update test_commit set a=3;
	commit;
exception
	WHEN OTHERS THEN    
		insert into test_commit select 2;
		commit;
end;
/

call test_commit_exception();
select * from test_commit;
drop table test_commit;

create or replace function test_commit2() return void
is
begin
    drop table if exists test_commit;
    create table test_commit(a int);
    FOR i IN REVERSE 3..0 LOOP
	insert into test_commit select i;
	commit;
    END LOOP;
    FOR i IN REVERSE 2..4 LOOP
	update test_commit set a=i;
	commit;
    END LOOP;
exception
WHEN OTHERS THEN   
--    FOR i IN REVERSE 200...101 LOOP
	insert into test_commit select 4;
--    END LOOP; 
    commit;
end;
/

call test_commit2();
select * from test_commit;
drop table test_commit;

create or replace function test_commit3() return void
is
begin
    drop table if exists test_commit;
    create table test_commit(a int);
	insert into test_commit select 1;
	commit;
    call test_commit2();
	update test_commit set a=2;
	commit;
exception
WHEN OTHERS THEN   
	insert into test_commit select 3;
    commit;
end;
/

call test_commit3();
select * from test_commit;
drop table test_commit;

create or replace function test_rollback_with_exception() return void
as
begin
    drop table if exists test_commit;
    create table test_commit(a int);
	insert into test_commit select 1;
	rollback;
EXCEPTION
    when raise_exception then
		RAISE EXCEPTION '(%)', SQLERRM; 
end;
/

call test_rollback_with_exception();
select * from test_commit;
drop table test_commit;

create or replace function test_nest_function_without_commit() return void
as
begin
    drop table if exists test_commit;
    create table test_commit(a int);
    insert into test_commit select 3;
	commit;
    test_without_commit();
EXCEPTION
    when raise_exception then
		RAISE EXCEPTION '(%)', SQLERRM; 
end;
/

call test_nest_function_without_commit();
select * from test_commit;
drop table test_commit;

create or replace function test_nest_function() return void
as
begin
    drop table if exists test_commit;
    create table test_commit(a int);
    insert into test_commit select 3;
	commit;
    test_commit();
EXCEPTION
    when raise_exception then
		RAISE EXCEPTION '(%)', SQLERRM; 
end;
/

call test_nest_function();
select * from test_commit;
drop table test_commit;

create or replace function test_nest_function1() return void
as
begin
    test_commit();
EXCEPTION
    when raise_exception then
		RAISE EXCEPTION '(%)', SQLERRM; 
end;
/

call test_nest_function1();
select * from test_commit;
drop table test_commit;

create or replace function test_nest_function2() return void
as
begin
    test_commit();
end;
/

call test_nest_function2();
select * from test_commit;
drop table test_commit;

create or replace function test_nest_function_rollback() return void
as
begin
    test_without_commit();
	rollback;
end;
/

call test_nest_function_rollback();
select * from test_commit;
drop table test_commit;

create or replace function test_nest_function_select() return void
as
begin
    insert into tx select 3;
	commit;
    select test_commit();
EXCEPTION
    when raise_exception then
		RAISE EXCEPTION '(%)', SQLERRM; 
end;
/

call test_nest_function_select();
select * from test_commit;
drop table test_commit;

create or replace function test_nest_function_calll() return void
as
begin
    insert into tx select 3;
	commit;
    call test_commit();
EXCEPTION
    when raise_exception then
		RAISE EXCEPTION '(%)', SQLERRM; 
end;
/

call test_nest_function_calll();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_exception_commit() return void
as
begin
    drop table if exists test_commit;
    create table test_commit(a int);
	insert into test_commit select 1;
	raise exception 'Exception rollback';
	insert into test_commit select 2;
EXCEPTION
    when raise_exception then
        insert into test_commit select 3;
		RAISE EXCEPTION '(%)', SQLERRM; 
end;
/

call test_commit_exception_commit();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_exception_commit_commit() return void
as
begin
    drop table if exists test_commit;
    create table test_commit(a int);
	insert into test_commit select 1;
	raise exception 'Exception rollback';
	insert into test_commit select 2;
EXCEPTION
    when raise_exception then
        insert into test_commit select 3;
		commit;
		RAISE EXCEPTION '(%)', SQLERRM; 
end;
/

call test_commit_exception_commit_commit();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_exception_commit_rollback() return void
as
begin
    drop table if exists test_commit;
    create table test_commit(a int);
	insert into test_commit select 1;
	raise exception 'Exception rollback';
	insert into test_commit select 2;
EXCEPTION
    when raise_exception then
        insert into test_commit select 3;
		rollback;
		RAISE EXCEPTION '(%)', SQLERRM; 
end;
/

call test_commit_exception_commit_rollback();
select * from test_commit;
drop table test_commit;

create or replace function test_rollback return void
as
begin
    drop table if exists test_commit;
    create table test_commit(a int);
	insert into test_commit select 1;
	rollback;
	insert into test_commit select 2;
end;
/

call test_rollback();
select * from test_commit;
drop table test_commit;

create or replace function test_commit_inout(p inout int) return void
as
declare
begin
	p = 3;
	commit;
	--DBE_OUTPUT.print_line('Cursor status:' + p);
end;
/

select test_commit_inout(1);

create or replace function test_rollback_inout(p inout int) return void
as
declare
begin
	p = 3;
	rollback;
	--DBE_OUTPUT.print_line('Cursor status:' + p);
end;
/

select test_rollback_inout(1);

create or replace function test_rollback_out(p out int) return void
as
declare
begin
	p = 3;
	rollback;
	--DBE_OUTPUT.print_line('Cursor status:' + p);
end;
/

select test_rollback_out();

create or replace function test_rollback1() return void
as
declare
begin
	create table test1(col1 int);
	insert into test1 values(1);
	rollback;
end;
/

call test_rollback1();

create type func_type_04 as ( v_tablefield character varying, v_tablefield2 character varying, v_tablename character varying, v_cur refcursor);
create table test_cursor_table(c1 int,c2 varchar);
insert into test_cursor_table values(1,'Jack'),(2,'Rose');

CREATE or replace function func_base13_03(v_tablefield character varying, v_tablefield2 character varying,v_tablename character varying) return refcursor
AS
v_cur refcursor;
begin
	open v_cur for
		'select '||v_tablefield||' as tablecode, '||v_tablefield2||' as tablename from '||v_tablename|| ' order by 1,2;';
	return v_cur;
end;
/

CREATE or replace function func_base13_04(v_tablefield character varying, v_tablefield2 character varying, v_tablename character varying) return void
AS
	v_record func_type_04;
	v_cur refcursor;
	num int;
begin
	num := 0;
	v_cur := func_base13_03(v_tablefield, v_tablefield2, v_tablename);
	loop
		fetch v_cur into v_record; 
		num := num+1;
		raise notice 'the num is %(%)', num,v_record;
		EXIT WHEN v_cur%notfound;
	end loop;
end;
/

call func_base13_04('c1','c2','test_cursor_table');

CREATE or replace function func_base13_05(v_tablefield character varying, v_tablefield2 character varying,v_tablename character varying) return refcursor
AS
v_cur refcursor;
begin
	open v_cur for
		'select '||v_tablefield||' as tablecode, '||v_tablefield2||' as tablename from '||v_tablename|| ' order by 1,2;';
	commit;
	return v_cur;
end;
/

CREATE or replace function func_base13_06(v_tablefield character varying, v_tablefield2 character varying, v_tablename character varying) return void
AS
	v_record func_type_04;
	v_cur refcursor;
begin
	select func_base13_05(v_tablefield, v_tablefield2, v_tablename) into v_cur;
	loop
		fetch v_cur into v_record;  
		raise notice '(%)', v_record;
		EXIT WHEN v_cur%notfound;
	end loop;
end;
/

call func_base13_06('c1','c2','test_cursor_table');

CREATE or replace function func_base13_07(v_tablefield character varying, v_tablefield2 character varying,v_tablename character varying) return refcursor
AS
v_cur refcursor;
begin
	open v_cur for
		'select '||v_tablefield||' as tablecode, '||v_tablefield2||' as tablename from '||v_tablename|| ' order by 1,2;';
	commit;
	return v_cur;
end;
/

CREATE or replace function func_base13_08(v_tablefield character varying, v_tablefield2 character varying, v_tablename character varying) return void
AS
	v_record func_type_04;
	v_cur refcursor;
begin
	select func_base13_07(v_tablefield, v_tablefield2, v_tablename) into v_cur;

	loop
		fetch v_cur into v_record; 
		raise notice 'before commit(%)', v_record;
		commit; 
		raise notice 'after commit(%)', v_record;
		EXIT WHEN v_cur%notfound;
	end loop;
	return;
end;
/

call func_base13_08('c1','c2','test_cursor_table');
select * from test_cursor_table;
drop table if exists test_cursor_table;

CREATE TABLE EXAMPLE1(COL1 INT);

CREATE OR REPLACE FUNCTION FUNCTION_EXAMPLE1 RETURN INT
AS
BEGIN
    FOR i IN 0..20 LOOP
        INSERT INTO EXAMPLE1 VALUES(i);
        IF mod(i,2) = 0 THEN
            COMMIT;
        ELSE
            ROLLBACK;
        END IF;
    END LOOP;
    RETURN 1;
END;
/

select FUNCTION_EXAMPLE1();
select * from FUNCTION_EXAMPLE1() where 1=1;
update EXAMPLE1 set COL1=666 where COL1=2 and FUNCTION_EXAMPLE1();
select (select FUNCTION_EXAMPLE1());
select (select * from FUNCTION_EXAMPLE1() where 1=1);

create or replace function func1() return void
as
declare
a int;
begin
a := 1/0;
exception
    WHEN division_by_zero THEN
        raise notice '%   %   %',sqlstate,SQLCODE,sqlerrm;
end;
/
call func1();
drop function func1;
drop table if exists EXAMPLE1;
drop function FUNCTION_EXAMPLE1;
drop function test_without_commit;
drop function test_empty_sp;
drop function test_commit;
drop function test_commit_insert_option;
drop function test_commit_insert_delete;
drop function test_commit_insert_update;
drop function test_commit_insert_update_delete;
drop function test_commit_insert_delete_update;
drop function test_commit_commit;
drop function test_commit_commit1;
drop function test_commit_rollback;
drop function test_commit_rollback1;
drop function test_rollback_commit;
drop function test_commit_insert_exception_rollback;
drop function test_commit_insert_exception_commit_rollback;
drop function test_commit_insert_raise_commit;
drop function test_commit_insert_delete_raise_commit;
drop function test_commit_insert_update_raise_commit;
drop function test_commit_insert_update_delete_raise_commit;
drop function test_commit_insert_delete_update_raise_commit;
drop function test_commit_insert_commit_raise;
drop function test_commit_insert_delete_commit_raise;
drop function test_commit_insert_update_commit_raise;
drop function test_commit_insert_update_delete_commit_raise;
drop function test_commit_insert_delete_update_commit_raise;
drop function test_exception_commit;
drop function test_exception_commit_commit_raise;
drop function test_exception_commit_raise_commit;
drop function test_gg_1;
drop function test_commit_exception;
drop function test_commit2;
drop function test_commit3;
drop function test_rollback_with_exception;
drop function test_nest_function_without_commit;
drop function test_nest_function;
drop function test_nest_function1;
drop function test_nest_function2;
drop function test_nest_function_rollback;
drop function test_nest_function_select;
drop function test_nest_function_calll;
drop function test_commit_exception_commit;
drop function test_commit_exception_commit_commit;
drop function test_commit_exception_commit_rollback;
drop function test_rollback;
drop function test_commit_inout;
drop function test_rollback_inout;
drop function test_rollback_out;
drop function test_rollback1;
drop function func_base13_03;
drop function func_base13_04;
drop function func_base13_05;
drop function func_base13_06;
drop function func_base13_07;
drop function func_base13_08;

