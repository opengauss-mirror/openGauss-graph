create table security_t1(f1 int);
create view security_v1 as select * from security_t1;
create function add_func(num1 int, num2 int) return int
as
begin
return num1+num2;
end
/
grant all on security_t1 to public;
grant all on security_v1 to public;
grant all (f1) on table security_t1 to public;
grant all on schema cstore to public;
grant all on tablespace pg_global to public;
grant all on function add_func(num1 int, num2 int) to public;
grant all on type bool to public;
grant all on language plpgsql to public;
grant all on security_t1 to public with grant option;
copy security_t1 to '/temp/copy_error';
copy security_t1 from '/temp/copy_error';
drop function add_func(num1 int, num2 int);
drop view security_v1;
drop table security_t1;