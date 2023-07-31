--Dynamic SQL Anonymous Block TEST
--CREATE schema and table ,INSERT data
CREATE SCHEMA test_schema;
create table test_schema.test_table(
    ID       INTEGER       PRIMARY KEY ,
    NAME     varchar2(20)  UNIQUE,
    AGE      INTEGER       CHECK(AGE>0),
    ADDRESS  varchar2(20)   NOT NULL,
    TELE     varchar2(20)   DEFAULT '101'
);
insert into test_schema.test_table values(1,'steve',10,'adsf');
insert into test_schema.test_table values(2,'warfield',20,'zcv','234');
insert into test_schema.test_table values(3,'henry',30,'zcv','567');


--declare and select into
create or replace FUNCTION sp_declare1()
RETURNS integer 
AS $$
DECLARE
MYCHAR1 VARCHAR2(20);
MYCHAR2 VARCHAR2(20);
PSV_SQL VARCHAR2(200);
BEGIN
     PSV_SQL := 'declare b1 integer; begin b1:=1;  select name into :1 from test_schema.test_table where id = b1;'
     	|| 'b1:=b1+1; select name into :2 from test_schema.test_table where id = b1; end;';
     EXECUTE IMMEDIATE PSV_SQL USING OUT MYCHAR1,OUT MYCHAR2;
     raise info 'NAME1 is %', MYCHAR1;
     raise info 'NAME2 is %', MYCHAR2;
     return 0;
END;
$$LANGUAGE plpgsql;
call sp_declare1();


--declare and select into
create or replace FUNCTION sp_declare2()
RETURNS integer 
AS $$
DECLARE
MYID1	INTEGER;
MYID2	INTEGER;
MYID3	INTEGER;
PSV_SQL VARCHAR2(500);
BEGIN
     PSV_SQL := 'declare b1 integer; bn1 varchar2(20); begin b1:=1;  select name into bn1 from test_schema.test_table'
     	|| ' where id = b1; select id into :1 from test_schema.test_table where name=bn1; b1:=b1+1; '
     	|| '  select name into bn1 from test_schema.test_table where id = b1; '
     	|| ' select id into :2 from test_schema.test_table where name=bn1; b1:=b1+1; '
     	|| '  select name into bn1 from test_schema.test_table where id = b1; '
     	|| ' select id into :3 from test_schema.test_table where name=bn1; end; ';
     EXECUTE IMMEDIATE PSV_SQL USING OUT MYID1, OUT MYID2, OUT MYID3;
     raise info 'ID is %', MYID1;
     raise info 'ID is %', MYID2;
     raise info 'ID is %', MYID3;
     return 0;
END;
$$LANGUAGE plpgsql;
call sp_declare2();



--SELECT INTO in Dynamic SQL
create or replace FUNCTION sp_block1()
RETURNS integer 
AS $$
DECLARE
MYID 	INTEGER;
MYCHAR VARCHAR2(20);
MYAGE 	INTEGER;
MYADDRESS	VARCHAR2(20);
MYTELE	VARCHAR2(20);
PSV_SQL VARCHAR2(200);
BEGIN
     PSV_SQL := 'begin select id,name,age,address,tele into :1,:2,:3,:4,:5 from test_schema.test_table where id = 1; end;';
     EXECUTE IMMEDIATE PSV_SQL USING OUT MYID, OUT MYCHAR,OUT MYAGE, OUT MYADDRESS, OUT MYTELE;
     raise info 'ID is %', MYID;
     raise info 'NAME is %', MYCHAR;
     raise info 'AGE is %', MYAGE;
     raise info 'ADDRESS is %', MYADDRESS;
     raise info 'TELE is %', MYTELE;
     return 0;
END;
$$LANGUAGE plpgsql;
call sp_block1();

create or replace FUNCTION sp_block2(MYINTEGER IN INTEGER)
returns INTEGER
AS $$
DECLARE
  MYCHAR   VARCHAR2(20);
  PSV_SQL  VARCHAR2(200);
BEGIN
    BEGIN
        PSV_SQL := 'begin select name into :1 from test_schema.test_table where id > '
        ||MYINTEGER||'; end;';
        EXECUTE IMMEDIATE PSV_SQL USING OUT MYCHAR;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
        raise info 'EXCEPTION is NO_DATA_FOUND'; 
        RETURN 0;                                                                                                                                                       
        WHEN TOO_MANY_ROWS THEN
        raise info 'EXCEPTION is TOO_MANY_ROWS';
        RETURN 0;
    END ;
    raise info 'name is %',MYCHAR;
    RETURN 0;
END;
$$LANGUAGE plpgsql;
select sp_block2(1000);
select sp_block2(2);
select sp_block2(0);

--USING IN
create or replace FUNCTION sp_block3()
returns INTEGER
AS $$
DECLARE
  MYINTEGER INTEGER ;
  MYCHAR   VARCHAR2(20);
  PSV_SQL   VARCHAR2(200);
BEGIN
  MYINTEGER := 1;
  PSV_SQL := 'begin select name into :1 from test_schema.test_table where id = :a; end;';
  EXECUTE IMMEDIATE PSV_SQL USING OUT MYCHAR, IN MYINTEGER;
  raise info 'NAME is %', MYCHAR;
  return 0;
END;
$$LANGUAGE plpgsql;

call sp_block3();

--USING INOUT
create or replace FUNCTION sp_block4()
returns INTEGER
AS $$
DECLARE
MYCHAR    VARCHAR2(20);
PV_TELE    VARCHAR2(20); 
BEGIN
  MYCHAR := 'MMM'; 
  EXECUTE IMMEDIATE 'begin update test_schema.test_table set tele = :a  where id =1; end;' USING IN MYCHAR;    
  select tele into PV_TELE from test_schema.test_table  where id =1;   
  raise info 'TELE IS %',PV_TELE;
  RETURN 0;
END;
$$LANGUAGE plpgsql;
call sp_block4();

--USING IN
create or replace function sp_block5(MYCHAR IN VARCHAR2(20))
returns INTEGER
AS $$
DECLARE
PV_TELE VARCHAR2(20); 
BEGIN
  EXECUTE IMMEDIATE 'begin update test_schema.test_table set tele = :a where id =1;end;' USING IN MYCHAR;
  select tele into PV_TELE from test_schema.test_table  where id =1;   
  raise info 'TELE IS %',PV_TELE;
  return 0;
END;
$$LANGUAGE plpgsql;

select sp_block5('MMM');

--USING INOUT
create or replace FUNCTION sp_block6(MYCHAR INOUT VARCHAR2(20))
returns VARCHAR2
AS $$
BEGIN
  raise notice 'MYCHAR is %', MYCHAR;
  MYCHAR := 'sp_block is called';
END;
$$LANGUAGE plpgsql;

create or replace FUNCTION sp_tempsp6()
returns INTEGER
AS $$
DECLARE
  MYCHAR   VARCHAR2(20);
  PSV_SQL  VARCHAR2(200);
BEGIN
  MYCHAR :=  'THIS IS TEST';
  PSV_SQL := 'begin call  sp_block6(:a);end;';
  EXECUTE IMMEDIATE PSV_SQL USING IN OUT MYCHAR;                       
  raise info 'MYCHAR is %', MYCHAR;
  RETURN 0;
END;
$$LANGUAGE plpgsql;

call sp_tempsp6();

--USING IN and OUT
create or replace FUNCTION sp_block7
(
 MYINTEGER IN INTEGER ,
 MYCHAR   OUT VARCHAR2(200)
)
returns VARCHAR2(200)
AS $$
DECLARE
BEGIN
     MYCHAR := 'sp_block is called';
     raise info 'MYINTEGER is %', MYINTEGER;  
  RETURN ;
END;
$$LANGUAGE plpgsql;

create or replace FUNCTION sp_tempsp7()
returns INTEGER
AS $$
DECLARE
  MYINTEGER INTEGER ;
  MYCHAR   VARCHAR2(20);
  PSV_SQL  VARCHAR2(200);
BEGIN
  MYINTEGER :=  1;
  PSV_SQL := 'begin call  sp_block7(:a,:b);end;';
  EXECUTE IMMEDIATE PSV_SQL USING IN MYINTEGER, OUT MYCHAR;
  raise info 'MYCHAR is %', MYCHAR;
  RETURN 0;
END;
$$LANGUAGE plpgsql;

call sp_tempsp7();

----USING IN,COMMAND-STRING is expr
create or replace FUNCTION sp_block8(RETURNCODE OUT INTEGER)
returns integer AS $$
DECLARE
  MYCHAR  VARCHAR2(20);
  PSV_SQL VARCHAR2(200);
BEGIN
     PSV_SQL := 'begin select name into :b from test_schema.test_table where id = :a;end;';
     EXECUTE IMMEDIATE PSV_SQL USING OUT MYCHAR, IN to_number('1')+1;             
     raise notice 'NAME is %', MYCHAR;
END;
$$LANGUAGE plpgsql;

call sp_block8(:a);

--USING IN,COMMAND-STRING is constant
create or replace function sp_block9( RETURNCODE OUT INTEGER )
returns integer AS $$
DECLARE
  MYCHAR   VARCHAR2(20);
  PSV_SQL  VARCHAR2(200);
BEGIN
     PSV_SQL := 'begin select name into :b from test_schema.test_table where id = :a;end;';
     EXECUTE IMMEDIATE PSV_SQL USING OUT MYCHAR , IN 1;        
     raise notice 'NAME is %', MYCHAR;
END; 
$$LANGUAGE plpgsql;

call sp_block9(:a);

CREATE OR REPLACE function sp_block10
(
    param1    in   INTEGER,
    param2    out  INTEGER,
    param3    in   INTEGER,
    param4    out  INTEGER,
    param5    out  INTEGER
)
returns record as $$
BEGIN
   param2:= param1 + param3;
   param4:= param1 + param2 + param3;
   param5:= param1 + param2 + param3 + param4;
END;
$$ LANGUAGE plpgsql;

create or replace function sp_block11() returns void as $$ DECLARE
    input1 INTEGER:=100;
    input2 INTEGER:=1;
    l_statement  VARCHAR2(200);
    l_param2     INTEGER;
    l_param4     INTEGER;
    l_param5     INTEGER;
BEGIN
    l_statement := 'begin call sp_block10(:1,:2,:3,:4,:5);end;';
    EXECUTE IMMEDIATE l_statement
        USING IN input1, OUT l_param2,IN input2,OUT l_param4,OUT l_param5;
    raise info 'result is:%',l_param2;
    raise info 'result is:%',l_param4;
    raise info 'result is:%',l_param5;
END;
$$ LANGUAGE plpgsql;

call sp_block11();


create or replace FUNCTION sp_insert1()
RETURNS integer 
AS $$
DECLARE
MYID	INTEGER;
MYNAME VARCHAR2(20);
MYAGE	INTEGER;
MYADDRESS VARCHAR2(20);
MYTELE	VARCHAR2(20);
PSV_SQL VARCHAR2(200);
BEGIN
     MYID := 5;
     MYNAME := 'terry';
     MYAGE := 40;
     MYADDRESS := 'broadway';
     MYTELE := '404';
     PSV_SQL := 'begin delete from test_schema.test_table where ID=:a OR NAME=:b;'
     		|| ' insert into test_schema.test_table values(:a,:b,:c,:d,:e);end;';
     EXECUTE IMMEDIATE PSV_SQL USING IN MYID, IN MYNAME, IN MYID, IN MYNAME, IN MYAGE, IN MYADDRESS,
     	IN MYTELE;
     return 0;
END;
$$LANGUAGE plpgsql;
call sp_insert1();

--drop functions,table and schema
drop function sp_declare1();
drop function sp_declare2();
drop function sp_block1();
drop function sp_block2(MYINTEGER IN INTEGER);
drop function sp_block3();
drop function sp_block4();
drop function sp_block5(MYCHAR IN VARCHAR2(20));
drop function sp_block6(MYCHAR INOUT VARCHAR2(20));
drop function sp_tempsp6();
drop function sp_block7(MYINTEGER IN INTEGER,MYCHAR OUT VARCHAR2(200));
drop function sp_tempsp7();
drop function sp_block8(RETURNCODE OUT INTEGER);
drop function sp_block9(RETURNCODE OUT INTEGER);
drop function sp_block10( param1 in INTEGER, param2 out INTEGER, param3 in INTEGER,param4 out  INTEGER, param5 out  INTEGER);
drop function sp_block11();
drop function sp_insert1();
drop table test_schema.test_table CASCADE;
drop schema test_schema;

create  or replace procedure sp_proc1
(para1  in   integer,
 para2  out  integer,
 para3  out  varchar,
 para4  in   varchar)
as
begin
   para2 :=para1;
   para3 :=para4;
   raise info 'para2 is %',para2;
   raise info 'para3 is %',para3;
end;
/

create or replace procedure sp_proc2
(id  in  integer,
 name out varchar)
as 
  add  varchar:='home';
begin
   name :=add;
   raise info 'name is %',name;
end;
/

declare 
 para1    integer:=1;
      para2    integer;
      para3    varchar;
      para4    varchar:='test';
      id       integer:=1;
      name     varchar;  
begin
   execute immediate 'begin  
                        call sp_proc1(:para1,:para2,:para3,:para4) into:para2;
                        call sp_proc2(:id , :name);
                        end;'
            using in para1, out para2, out para3, in para4,out para2, in id,out name;

end;
/

declare 
 para1    integer:=1;
      para2    integer;
      para3    varchar;
      para4    varchar:='test';
      id       integer:=1;
      name     varchar;  
begin
   execute immediate 'begin  
                        call sp_proc1(:para1,:para2,:para3,:para4) into:para2;
                        call sp_proc2(:id , :name);
                        end;'
            using in para1, out para2, out para3, in para4,in id,out name;
end;
/

drop procedure sp_proc1;
drop procedure sp_proc2;

create table test(id integer);
declare 
a  varchar;
b  integer ;
c  integer ;
begin
   execute immediate 'select count(*) from test' into a;
    
   execute immediate 'begin select count(1)  into :1 from test;
                            select count(1)  into :2 from test ;
                        end;'
             using in b, in c;
end;
/
drop table test;

CREATE OR REPLACE PROCEDURE sp_test_1
(
    param1    in   INTEGER,
    param2    out  INTEGER,
    param3    in   INTEGER
)
AS
BEGIN
   param2:= param1 + param3;
END;
/

DECLARE
    input1 INTEGER:=1;
    input2 INTEGER:=2;
    l_param2     INTEGER;
BEGIN
     
    EXECUTE IMMEDIATE 'begin select * from sp_test_1(:col_1, :col_2, :col_3) into :col_2 ;end;'
        USING IN input1, OUT l_param2, IN input2;
END;
/
drop procedure sp_test_1;

create table test(id integer,name char(9),add1 char(9));
declare 
a1 char(9):='july';
a2 char(9):='henan';
a3 char(9):='june';
a4 char(9):='xian';
a5 char(9):='mone';
a6 char(9):='taiyuan';
b integer:=0;
c integer;
begin
   execute immediate '
   declare 
   myexception exception;
   begin  
   insert into test values (:1,:2,:3);
   insert into test values (:4,:5,:6);
   insert into test values (:7,:8,:9);
   exception 
          when myexception then 
            rollback;    
       
    end;'
            using  4, a1, a2 ,6, a3, a4,8,a5, a6;   
end;
/
select * from test;
drop table test;


CREATE OR REPLACE PROCEDURE "SP_HW_SUB_ADDMODULES"
(
    returncode       OUT     integer,
    PSV_MODULEDESC   IN      VARCHAR2
)
AS
BEGIN
END  SP_HW_SUB_ADDMODULES ;
/
create or replace PROCEDURE  sp_test
(
temp IN varchar2
)
AS
  PSV_SQL  VARCHAR2(200);
BEGIN
     PSV_SQL := 'BEGIN '
               ||'SP_HW_SUB_ADDMODULES('
      ||':1,'
      ||''''||to_char(TEMP)||''''
               ||'); END;';    
     dbms_output.put_line(PSV_SQL); 
     EXECUTE IMMEDIATE PSV_SQL;
END;
/
call sp_test('jack');
drop procedure SP_HW_SUB_ADDMODULES;
drop procedure sp_test;

--Dynamic SQL Anonymous Block TEST
--CREATE schema and table ,INSERT data
CREATE SCHEMA test_schema;
create table test_schema.test_table(
    ID       INTEGER       PRIMARY KEY ,
    NAME     varchar2(20)  UNIQUE,
    AGE      INTEGER       CHECK(AGE>0),
    ADDRESS  varchar2(20)   NOT NULL,
    TELE     varchar2(20)   DEFAULT '101'
);
insert into test_schema.test_table values(1,'steve',10,'adsf');
insert into test_schema.test_table values(2,'warfield',20,'zcv','234');
insert into test_schema.test_table values(3,'henry',30,'zcv','567');


--declare and select into
create or replace FUNCTION sp_declare1()
RETURNS integer 
AS $$
DECLARE
MYCHAR1 VARCHAR2(20);
MYCHAR2 VARCHAR2(20);
PSV_SQL VARCHAR2(200);
BEGIN
     PSV_SQL := 'declare b1 integer; begin b1:=1;  select name into :1 from test_schema.test_table where id = b1;'
     	|| 'b1:=b1+1; select name into :2 from test_schema.test_table where id = b1; end;';
     EXECUTE IMMEDIATE PSV_SQL USING OUT MYCHAR1,OUT MYCHAR2;
     raise info 'NAME1 is %', MYCHAR1;
     raise info 'NAME2 is %', MYCHAR2;
     return 0;
END;
$$LANGUAGE plpgsql;
call sp_declare1();


--declare and select into
create or replace FUNCTION sp_declare2()
RETURNS integer 
AS $$
DECLARE
MYID1	INTEGER;
MYID2	INTEGER;
MYID3	INTEGER;
PSV_SQL VARCHAR2(500);
BEGIN
     PSV_SQL := 'declare b1 integer; bn1 varchar2(20); begin b1:=1;  select name into bn1 from test_schema.test_table'
     	|| ' where id = b1; select id into :1 from test_schema.test_table where name=bn1; b1:=b1+1; '
     	|| '  select name into bn1 from test_schema.test_table where id = b1; '
     	|| ' select id into :2 from test_schema.test_table where name=bn1; b1:=b1+1; '
     	|| '  select name into bn1 from test_schema.test_table where id = b1; '
     	|| ' select id into :3 from test_schema.test_table where name=bn1; end; ';
     EXECUTE IMMEDIATE PSV_SQL USING OUT MYID1, OUT MYID2, OUT MYID3;
     raise info 'ID is %', MYID1;
     raise info 'ID is %', MYID2;
     raise info 'ID is %', MYID3;
     return 0;
END;
$$LANGUAGE plpgsql;
call sp_declare2();



--SELECT INTO in Dynamic SQL
create or replace FUNCTION sp_block1()
RETURNS integer 
AS $$
DECLARE
MYID 	INTEGER;
MYCHAR VARCHAR2(20);
MYAGE 	INTEGER;
MYADDRESS	VARCHAR2(20);
MYTELE	VARCHAR2(20);
PSV_SQL VARCHAR2(200);
BEGIN
     PSV_SQL := 'begin select id,name,age,address,tele into :1,:2,:3,:4,:5 from test_schema.test_table where id = 1; end;';
     EXECUTE IMMEDIATE PSV_SQL USING OUT MYID, OUT MYCHAR,OUT MYAGE, OUT MYADDRESS, OUT MYTELE;
     raise info 'ID is %', MYID;
     raise info 'NAME is %', MYCHAR;
     raise info 'AGE is %', MYAGE;
     raise info 'ADDRESS is %', MYADDRESS;
     raise info 'TELE is %', MYTELE;
     return 0;
END;
$$LANGUAGE plpgsql;
call sp_block1();

create or replace FUNCTION sp_block2(MYINTEGER IN INTEGER)
returns INTEGER
AS $$
DECLARE
  MYCHAR   VARCHAR2(20);
  PSV_SQL  VARCHAR2(200);
BEGIN
    BEGIN
        PSV_SQL := 'begin select name into :1 from test_schema.test_table where id > '
        ||MYINTEGER||'; end;';
        EXECUTE IMMEDIATE PSV_SQL USING OUT MYCHAR;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
        raise info 'EXCEPTION is NO_DATA_FOUND'; 
        RETURN 0;                                                                                                                                                       
        WHEN TOO_MANY_ROWS THEN
        raise info 'EXCEPTION is TOO_MANY_ROWS';
        RETURN 0;
    END ;
    raise info 'name is %',MYCHAR;
    RETURN 0;
END;
$$LANGUAGE plpgsql;
select sp_block2(1000);
select sp_block2(2);
select sp_block2(0);

--USING IN
create or replace FUNCTION sp_block3()
returns INTEGER
AS $$
DECLARE
  MYINTEGER INTEGER ;
  MYCHAR   VARCHAR2(20);
  PSV_SQL   VARCHAR2(200);
BEGIN
  MYINTEGER := 1;
  PSV_SQL := 'begin select name into :1 from test_schema.test_table where id = :a; end;';
  EXECUTE IMMEDIATE PSV_SQL USING OUT MYCHAR, IN MYINTEGER;
  raise info 'NAME is %', MYCHAR;
  return 0;
END;
$$LANGUAGE plpgsql;

call sp_block3();

--USING INOUT
create or replace FUNCTION sp_block4()
returns INTEGER
AS $$
DECLARE
MYCHAR    VARCHAR2(20);
PV_TELE    VARCHAR2(20); 
BEGIN
  MYCHAR := 'MMM'; 
  EXECUTE IMMEDIATE 'begin update test_schema.test_table set tele = :a  where id =1; end;' USING IN MYCHAR;    
  select tele into PV_TELE from test_schema.test_table  where id =1;   
  raise info 'TELE IS %',PV_TELE;
  RETURN 0;
END;
$$LANGUAGE plpgsql;
call sp_block4();

--USING IN
create or replace function sp_block5(MYCHAR IN VARCHAR2(20))
returns INTEGER
AS $$
DECLARE
PV_TELE VARCHAR2(20); 
BEGIN
  EXECUTE IMMEDIATE 'begin update test_schema.test_table set tele = :a where id =1;end;' USING IN MYCHAR;
  select tele into PV_TELE from test_schema.test_table  where id =1;   
  raise info 'TELE IS %',PV_TELE;
  return 0;
END;
$$LANGUAGE plpgsql;

select sp_block5('MMM');

--USING INOUT
create or replace FUNCTION sp_block6(MYCHAR INOUT VARCHAR2(20))
returns VARCHAR2
AS $$
BEGIN
  raise notice 'MYCHAR is %', MYCHAR;
  MYCHAR := 'sp_block is called';
END;
$$LANGUAGE plpgsql;

create or replace FUNCTION sp_tempsp6()
returns INTEGER
AS $$
DECLARE
  MYCHAR   VARCHAR2(20);
  PSV_SQL  VARCHAR2(200);
BEGIN
  MYCHAR :=  'THIS IS TEST';
  PSV_SQL := 'begin call  sp_block6(:a);end;';
  EXECUTE IMMEDIATE PSV_SQL USING IN OUT MYCHAR;                       
  raise info 'MYCHAR is %', MYCHAR;
  RETURN 0;
END;
$$LANGUAGE plpgsql;

call sp_tempsp6();

--USING IN and OUT
create or replace FUNCTION sp_block7
(
 MYINTEGER IN INTEGER ,
 MYCHAR   OUT VARCHAR2(200)
)
returns VARCHAR2(200)
AS $$
DECLARE
BEGIN
     MYCHAR := 'sp_block is called';
     raise info 'MYINTEGER is %', MYINTEGER;  
  RETURN ;
END;
$$LANGUAGE plpgsql;

create or replace FUNCTION sp_tempsp7()
returns INTEGER
AS $$
DECLARE
  MYINTEGER INTEGER ;
  MYCHAR   VARCHAR2(20);
  PSV_SQL  VARCHAR2(200);
BEGIN
  MYINTEGER :=  1;
  PSV_SQL := 'begin call  sp_block7(:a,:b);end;';
  EXECUTE IMMEDIATE PSV_SQL USING IN MYINTEGER, OUT MYCHAR;
  raise info 'MYCHAR is %', MYCHAR;
  RETURN 0;
END;
$$LANGUAGE plpgsql;

call sp_tempsp7();

----USING IN,COMMAND-STRING is expr
create or replace FUNCTION sp_block8(RETURNCODE OUT INTEGER)
returns integer AS $$
DECLARE
  MYCHAR  VARCHAR2(20);
  PSV_SQL VARCHAR2(200);
BEGIN
     PSV_SQL := 'begin select name into :b from test_schema.test_table where id = :a;end;';
     EXECUTE IMMEDIATE PSV_SQL USING OUT MYCHAR, IN to_number('1')+1;             
     raise notice 'NAME is %', MYCHAR;
END;
$$LANGUAGE plpgsql;

call sp_block8(:a);

--USING IN,COMMAND-STRING is constant
create or replace function sp_block9( RETURNCODE OUT INTEGER )
returns integer AS $$
DECLARE
  MYCHAR   VARCHAR2(20);
  PSV_SQL  VARCHAR2(200);
BEGIN
     PSV_SQL := 'begin select name into :b from test_schema.test_table where id = :a;end;';
     EXECUTE IMMEDIATE PSV_SQL USING OUT MYCHAR , IN 1;        
     raise notice 'NAME is %', MYCHAR;
END; 
$$LANGUAGE plpgsql;

call sp_block9(:a);

CREATE OR REPLACE function sp_block10
(
    param1    in   INTEGER,
    param2    out  INTEGER,
    param3    in   INTEGER,
    param4    out  INTEGER,
    param5    out  INTEGER
)
returns record as $$
BEGIN
   param2:= param1 + param3;
   param4:= param1 + param2 + param3;
   param5:= param1 + param2 + param3 + param4;
END;
$$ LANGUAGE plpgsql;

create or replace function sp_block11() returns void as $$ DECLARE
    input1 INTEGER:=100;
    input2 INTEGER:=1;
    l_statement  VARCHAR2(200);
    l_param2     INTEGER;
    l_param4     INTEGER;
    l_param5     INTEGER;
BEGIN
    l_statement := 'begin call sp_block10(:1,:2,:3,:4,:5);end;';
    EXECUTE IMMEDIATE l_statement
        USING IN input1, OUT l_param2,IN input2,OUT l_param4,OUT l_param5;
    raise info 'result is:%',l_param2;
    raise info 'result is:%',l_param4;
    raise info 'result is:%',l_param5;
END;
$$ LANGUAGE plpgsql;

call sp_block11();


create or replace FUNCTION sp_insert1()
RETURNS integer 
AS $$
DECLARE
MYID	INTEGER;
MYNAME VARCHAR2(20);
MYAGE	INTEGER;
MYADDRESS VARCHAR2(20);
MYTELE	VARCHAR2(20);
PSV_SQL VARCHAR2(200);
BEGIN
     MYID := 5;
     MYNAME := 'terry';
     MYAGE := 40;
     MYADDRESS := 'broadway';
     MYTELE := '404';
     PSV_SQL := 'begin delete from test_schema.test_table where ID=:a OR NAME=:b;'
     		|| ' insert into test_schema.test_table values(:a,:b,:c,:d,:e);end;';
     EXECUTE IMMEDIATE PSV_SQL USING IN MYID, IN MYNAME, IN MYID, IN MYNAME, IN MYAGE, IN MYADDRESS,
     	IN MYTELE;
     return 0;
END;
$$LANGUAGE plpgsql;
call sp_insert1();

--drop functions,table and schema
drop function sp_declare1();
drop function sp_declare2();
drop function sp_block1();
drop function sp_block2(MYINTEGER IN INTEGER);
drop function sp_block3();
drop function sp_block4();
drop function sp_block5(MYCHAR IN VARCHAR2(20));
drop function sp_block6(MYCHAR INOUT VARCHAR2(20));
drop function sp_tempsp6();
drop function sp_block7(MYINTEGER IN INTEGER,MYCHAR OUT VARCHAR2(200));
drop function sp_tempsp7();
drop function sp_block8(RETURNCODE OUT INTEGER);
drop function sp_block9(RETURNCODE OUT INTEGER);
drop function sp_block10( param1 in INTEGER, param2 out INTEGER, param3 in INTEGER,param4 out  INTEGER, param5 out  INTEGER);
drop function sp_block11();
drop function sp_insert1();
drop table test_schema.test_table CASCADE;
drop schema test_schema;

create  or replace procedure sp_proc1
(para1  in   integer,
 para2  out  integer,
 para3  out  varchar,
 para4  in   varchar)
as
begin
   para2 :=para1;
   para3 :=para4;
   raise info 'para2 is %',para2;
   raise info 'para3 is %',para3;
end;
/

create or replace procedure sp_proc2
(id  in  integer,
 name out varchar)
as 
  add  varchar:='home';
begin
   name :=add;
   raise info 'name is %',name;
end;
/

declare 
 para1    integer:=1;
      para2    integer;
      para3    varchar;
      para4    varchar:='test';
      id       integer:=1;
      name     varchar;  
begin
   execute immediate 'begin  
                        call sp_proc1(:para1,:para2,:para3,:para4) into:para2;
                        call sp_proc2(:id , :name);
                        end;'
            using in para1, out para2, out para3, in para4,out para2, in id,out name;

end;
/

declare 
 para1    integer:=1;
      para2    integer;
      para3    varchar;
      para4    varchar:='test';
      id       integer:=1;
      name     varchar;  
begin
   execute immediate 'begin  
                        call sp_proc1(:para1,:para2,:para3,:para4) into:para2;
                        call sp_proc2(:id , :name);
                        end;'
            using in para1, out para2, out para3, in para4,in id,out name;
end;
/

drop procedure sp_proc1;
drop procedure sp_proc2;

create table test(id integer);
declare 
a  varchar;
b  integer ;
c  integer ;
begin
   execute immediate 'select count(*) from test' into a;
    
   execute immediate 'begin select count(1)  into :1 from test;
                            select count(1)  into :2 from test ;
                        end;'
             using in b, in c;
end;
/
drop table test;

CREATE OR REPLACE PROCEDURE sp_test_1
(
    param1    in   INTEGER,
    param2    out  INTEGER,
    param3    in   INTEGER
)
AS
BEGIN
   param2:= param1 + param3;
END;
/

DECLARE
    input1 INTEGER:=1;
    input2 INTEGER:=2;
    l_param2     INTEGER;
BEGIN
     
    EXECUTE IMMEDIATE 'begin select * from sp_test_1(:col_1, :col_2, :col_3) into :col_2 ;end;'
        USING IN input1, OUT l_param2, IN input2;
END;
/
drop procedure sp_test_1;

create table test(id integer,name char(9),add1 char(9));
declare 
a1 char(9):='july';
a2 char(9):='henan';
a3 char(9):='june';
a4 char(9):='xian';
a5 char(9):='mone';
a6 char(9):='taiyuan';
b integer:=0;
c integer;
begin
   execute immediate '
   declare 
   myexception exception;
   begin  
   insert into test values (:1,:2,:3);
   insert into test values (:4,:5,:6);
   insert into test values (:7,:8,:9);
   exception 
          when myexception then 
            rollback;    
       
    end;'
            using  4, a1, a2 ,6, a3, a4,8,a5, a6;   
end;
/
select * from test;
drop table test;


CREATE OR REPLACE PROCEDURE "SP_HW_SUB_ADDMODULES"
(
    returncode       OUT     integer,
    PSV_MODULEDESC   IN      VARCHAR2
)
AS
BEGIN
END  SP_HW_SUB_ADDMODULES ;
/
create or replace PROCEDURE  sp_test
(
temp IN varchar2
)
AS
  PSV_SQL  VARCHAR2(200);
BEGIN
     PSV_SQL := 'BEGIN '
               ||'SP_HW_SUB_ADDMODULES('
      ||':1,'
      ||''''||to_char(TEMP)||''''
               ||'); END;';    
     dbms_output.put_line(PSV_SQL); 
     EXECUTE IMMEDIATE PSV_SQL;
END;
/
call sp_test('jack');
drop procedure SP_HW_SUB_ADDMODULES;
drop procedure sp_test;

