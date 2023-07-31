------------------------------------------------------------------------------
-----test implicit cursor attributes for DML: select,insert,update,delete-----
------------------------------------------------------------------------------
create schema hw_cursor_part7;
set current_schema = hw_cursor_part7;
set behavior_compat_options = 'skip_insert_gs_source';
create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');

create or replace procedure sp_testsp
as
    v int:=0;
begin
    --select
    select v1 into v from t1 where v1=1; 
    if not sql%isopen then --sql%isopen always be false    
        raise notice '%','test select: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test select: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test select: sql%notfound=true';
    end if;    
    raise notice 'test select: sql%%rowcount=%',sql%rowcount;

    --insert
    insert into t1 values (4,'abc4');
    if not sql%isopen then --sql%isopen always be false    
        raise notice '%','test insert: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test insert: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test insert: sql%notfound=true';
    end if;    
        raise notice 'test insert: sql%%rowcount=%',sql%rowcount;
    
    --update
    update t1 set v1=v1+100 where v1>1000;
    if not sql%isopen then --sql%isopen always be false    
        raise notice '%','test update: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test update: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test update: sql%notfound=true';
    end if;    
    raise notice 'test update: sql%%rowcount=%',sql%rowcount;
    
    update t1 set v1=v1+100 where v1<1000;
    if not sql%isopen then --sql%isopen always be false    
        raise notice '%','test update: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test update: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test update: sql%notfound=true';
    end if;    
    raise notice 'test update: sql%%rowcount=%',sql%rowcount;
 
    --delete
    delete from t1 where v1>1000;
    if not sql%isopen then --sql%isopen always be false    
        raise notice '%','test delete: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test delete: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test delete: sql%notfound=true';
    end if;    
    raise notice 'test delete: sql%%rowcount=%',sql%rowcount;
    
    delete from t1 where v1<1000;
    if not sql%isopen then --sql%isopen always be false    
        raise notice '%','test delete: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test delete: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test delete: sql%notfound=true';
    end if;    
    raise notice 'test delete: sql%%rowcount=%',sql%rowcount;
end;
/
call sp_testsp();
drop procedure sp_testsp;
drop table t1;
------------------------------------------------------------------------------
-----support A db's cursor in or out params---------------------------------
------------------------------------------------------------------------------
CREATE TABLE TBL(VALUE INT);
INSERT INTO TBL VALUES (1);
INSERT INTO TBL VALUES (2);
INSERT INTO TBL VALUES (3);
INSERT INTO TBL VALUES (4);

CREATE OR REPLACE PROCEDURE TEST_SP
IS
    CURSOR C1(NO IN VARCHAR2) IS SELECT * FROM TBL WHERE VALUE < NO ORDER BY 1;
    CURSOR C2(NO OUT VARCHAR2) IS SELECT * FROM TBL WHERE VALUE < 10 ORDER BY 1;
    V INT;
    RESULT INT;
BEGIN
    OPEN C1(10);
    OPEN C2(RESULT);
    LOOP
    FETCH C1 INTO V; 
        IF C1%FOUND THEN 
            raise notice '%',V;
        ELSE 
            EXIT;
        END IF;
    END LOOP;
    CLOSE C1;
    
    LOOP
    FETCH C2 INTO V; 
        IF C2%FOUND THEN 
            raise notice '%',V;
        ELSE 
            EXIT;
        END IF;
    END LOOP;
    CLOSE C2;
END;
/
CALL  TEST_SP();
DROP TABLE TBL;
DROP PROCEDURE TEST_SP;

---------------------------------------------------------------------------------
----- test the mixed use of implicit and explicit cursor attributes -------------
----- test the effect of the implicit cursor use to explicit cursor attributes --
---------------------------------------------------------------------------------
drop table t1;
create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');

create or replace procedure sp_testsp_select
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --select
    select v1 into v from t1 where v1=1;    
    if not cur%isopen then   
raise notice '%','test select: cur%isopen=false';
    end if;
    if cur%found then 
raise notice '%','test select: cur%found=true';
    end if;
    if cur%notfound then 
raise notice '%','test select: cur%notfound=true';
    end if;    
raise notice 'test select: cur%%rowcount=%',cur%rowcount;
    close cur;
end;
/
call sp_testsp_select();
drop procedure sp_testsp_select;
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_insert
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --insert
    insert into t1 values (4,'abc4');
    if not cur%isopen then    
        raise notice '%','test insert: cur%isopen=false';
    end if;
    if cur%found then 
        raise notice '%','test insert: cur%found=true';
    end if;
    if cur%notfound then 
        raise notice '%','test insert: cur%notfound=true';
    end if;    
    raise notice 'test insert: cur%%rowcount=%',cur%rowcount;
    close cur;
end;
/
call sp_testsp_insert();
drop procedure sp_testsp_insert;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_update
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --update
    update t1 set v1=v1+100 where v1>1000;
    if not cur%isopen then    
        raise notice '%','test update: cur%isopen=false';
    end if;
    if cur%found then 
        raise notice '%','test update: cur%found=true';
    end if;
    if cur%notfound then 
        raise notice '%','test update: cur%notfound=true';
    end if;    
    raise notice 'test update: cur%%rowcount=%',cur%rowcount;
    
    update t1 set v1=v1+100 where v1<1000;
    if not cur%isopen then    
        raise notice '%','test update: cur%isopen=false';
    end if;
    if cur%found then 
        raise notice '%','test update: cur%found=true';
    end if;
    if cur%notfound then 
        raise notice '%','test update: cur%notfound=true';
    end if;    
    raise notice 'test update: cur%%rowcount=%',cur%rowcount;
    close cur;
end;
/
call sp_testsp_update();
drop procedure sp_testsp_update;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_delete
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --delete
    delete from t1 where v1>1000;
    if not cur%isopen then    
        raise notice '%','test delete: cur%isopen=false';
    end if;
    if cur%found then 
        raise notice '%','test delete: cur%found=true';
    end if;
    if cur%notfound then 
        raise notice '%','test delete: cur%notfound=true';
    end if;    
    raise notice 'test delete: cur%%rowcount=%',cur%rowcount;
    
    delete from t1 where v1<1000;
    if not cur%isopen then    
        raise notice '%','test delete: cur%isopen=false';
    end if;
    if cur%found then 
        raise notice '%','test delete: cur%notfound=true';
    end if;
    if cur%notfound then 
        raise notice '%','test delete: cur%notfound=true';
    end if;    
    raise notice 'test delete: cur%%rowcount=%',cur%rowcount;
    close cur;
end;
/
call sp_testsp_delete();
drop procedure sp_testsp_delete;  
drop table t1;

---------------------------------------------------------------------------------
----- test the mixed use of implicit and explicit cursor attributes -------------
----- test the effect of the explicit cursor use to implicit cursor attributes --
---------------------------------------------------------------------------------
create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');

create or replace procedure sp_testsp_select
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --select    
    select v1 into v from t1 where v1=1;    
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then   
        raise notice '%','test select: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test select: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test select: sql%notfound=true';
    end if;    
    raise notice 'test select: sql%%rowcount=%',sql%rowcount;
    close cur;
end;
/
call sp_testsp_select();
drop procedure sp_testsp_select;
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_insert
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --insert
    insert into t1 values (4,'abc4');    
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
        raise notice '%','test insert: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test insert: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test insert: sql%notfound=true';
    end if;    
    raise notice 'test insert: sql%%rowcount=%',sql%rowcount;
    close cur;
end;
/
call sp_testsp_insert();
drop procedure sp_testsp_insert;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_update
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --update
    update t1 set v1=v1+100 where v1>1000;    
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
        raise notice '%','test update: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test update: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test update: sql%notfound=true';
    end if;    
    raise notice 'test update: sql%%rowcount=%',sql%rowcount;
    
    update t1 set v1=v1+100 where v1<1000;    
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
        raise notice '%','test update: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test update: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test update: sql%notfound=true';
    end if;    
    raise notice 'test update: sql%%rowcount=%',sql%rowcount;
    close cur;
end;
/
call sp_testsp_update();
drop procedure sp_testsp_update;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_delete
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --delete
    delete from t1 where v1>1000;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
        raise notice '%','test delete: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test delete: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test delete: sql%notfound=true';
    end if;    
    raise notice 'test delete: sql%%rowcount=%',sql%rowcount;
    
    delete from t1 where v1<1000;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
        raise notice '%','test delete: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test delete: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test delete: sql%notfound=true';
    end if;    
    raise notice 'test delete: sql%%rowcount=%',sql%rowcount;
    close cur;
end;
/
call sp_testsp_delete();
drop procedure sp_testsp_delete;  
drop table t1;
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CREATE TABLE TBL(V1 INTEGER);
INSERT INTO TBL VALUES(1);
INSERT INTO TBL VALUES(2);
CREATE OR REPLACE PROCEDURE SP_TEST 
AS
    CURSOR CUR IS
        SELECT * FROM TBL;
BEGIN 
    --EXPLICIT CURSOR ATTRIBUTES INITIAL STATUS
    IF CUR%ISOPEN THEN 
        raise notice '%','CUR%ISOPEN INITIAL STATUS BEFORE OPEN : TRUE';
    ELSIF NOT CUR%ISOPEN THEN 
        raise notice '%','CUR%ISOPEN INITIAL STATUS BEFORE OPEN : FALSE';
    ELSE
        raise notice '%','CUR%ISOPEN INITIAL STATUS BEFORE OPEN : NULL';
    END IF;
    OPEN CUR;
    IF CUR%FOUND THEN 
        raise notice '%','CUR%FOUND INITIAL STATUS : TRUE';
    ELSIF NOT CUR%FOUND THEN 
        raise notice '%','CUR%FOUND INITIAL STATUS : FALSE';
    ELSE
        raise notice '%','CUR%FOUND INITIAL STATUS : NULL';
    END IF;
    
    IF CUR%NOTFOUND THEN 
        raise notice '%','CUR%NOTFOUND INITIAL STATUS : TRUE';
    ELSIF NOT CUR%NOTFOUND THEN 
        raise notice '%','CUR%NOTFOUND INITIAL STATUS : FALSE';
    ELSE
        raise notice '%','CUR%NOTFOUND INITIAL STATUS : NULL';
    END IF;
    
    raise notice 'CUR%%ROWCOUNT INITIAL STATUS :%',NVL(TO_CHAR(CUR%ROWCOUNT),'NULL');
    
    CLOSE CUR;
    IF CUR%ISOPEN THEN 
        raise notice '%','CUR%ISOPEN STATUS AFTER CLOSE : TRUE';
    ELSIF NOT CUR%ISOPEN THEN 
        raise notice '%','CUR%ISOPEN STATUS AFTER CLOSE : FALSE';
    ELSE
        raise notice '%','CUR%ISOPEN STATUS AFTER CLOSE : NULL';
    END IF;
    
    --IMPLICIT CURSOR ATTRIBUTES INITIAL STATUS 
    IF SQL%ISOPEN THEN 
        raise notice '%','SQL%ISOPEN INITIAL STATUS : TRUE';
    ELSIF NOT SQL%ISOPEN THEN 
        raise notice '%','SQL%ISOPEN INITIAL STATUS : FALSE';
    ELSE
        raise notice '%','SQL%ISOPEN INITIAL STATUS : NULL';
    END IF;

    IF SQL%FOUND THEN 
       raise notice '%','SQL%FOUND INITIAL STATUS : TRUE';
    ELSIF NOT SQL%FOUND THEN 
       raise notice '%','SQL%FOUND INITIAL STATUS : FALSE';
    ELSE
       raise notice '%','SQL%FOUND INITIAL STATUS : NULL';
    END IF;
    
    IF SQL%NOTFOUND THEN 
      raise notice '%','SQL%NOTFOUND INITIAL STATUS : TRUE';
    ELSIF NOT SQL%NOTFOUND THEN 
      raise notice '%','SQL%NOTFOUND INITIAL STATUS : FALSE';
    ELSE
      raise notice '%','SQL%NOTFOUND INITIAL STATUS : NULL';
    END IF;
    
    raise notice 'SQL%%ROWCOUNT INITIAL STATUS : %',NVL(TO_CHAR(SQL%ROWCOUNT),'NULL') ;
END;
/
CALL SP_TEST();
DROP TABLE TBL;
DROP PROCEDURE SP_TEST;

CREATE TABLE TBL_H248LNK_INFO(ULBSGMODULENO INTEGER);
INSERT INTO TBL_H248LNK_INFO VALUES(123);
INSERT INTO TBL_H248LNK_INFO VALUES(456);
INSERT INTO TBL_H248LNK_INFO VALUES(789);
CREATE TABLE TBL (I_MODULENO INTEGER);

CREATE OR REPLACE PROCEDURE TEST_CURSOR_7 
AS
        TYPE CUR_TYPE IS REF CURSOR;
        CUR CUR_TYPE;
        PSV_SQL VARCHAR2(1000);
        PI_MODULENO INTEGER;
        TBL_STR VARCHAR2(1000) := 'TBL';
BEGIN
        OPEN CUR FOR SELECT DISTINCT ULBSGMODULENO FROM TBL_H248LNK_INFO;
        LOOP
            FETCH CUR INTO PI_MODULENO;
            EXIT WHEN CUR%NOTFOUND;            
            PSV_SQL := 'BEGIN INSERT INTO TBL (I_MODULENO) VALUES('||PI_MODULENO||');END;';
            EXECUTE IMMEDIATE PSV_SQL;

            -- check cursor attris status
            IF CUR%ISOPEN THEN 
                raise notice '%','CUR%ISOPEN : TRUE';
            ELSIF NOT CUR%ISOPEN THEN 
                raise notice '%','CUR%ISOPEN : FALSE';
            ELSE
                raise notice '%','CUR%ISOPEN : NULL';
            END IF;
            IF CUR%FOUND THEN 
                raise notice '%','CUR%FOUND : TRUE';
            ELSIF NOT CUR%FOUND THEN 
                raise notice '%','CUR%FOUND : FALSE';
            ELSE
                raise notice '%','CUR%FOUND : NULL';
            END IF;            
            IF CUR%NOTFOUND THEN 
                raise notice '%','CUR%NOTFOUND : TRUE';
            ELSIF NOT CUR%NOTFOUND THEN 
                raise notice '%','CUR%NOTFOUND : FALSE';
            ELSE
                raise notice '%','CUR%NOTFOUND : NULL';
            END IF;            
            raise notice 'CUR%%ROWCOUNT : %', NVL(TO_CHAR(CUR%ROWCOUNT),'NULL');
            IF SQL%ISOPEN THEN 
                raise notice '%','SQL%ISOPEN : TRUE';
            ELSIF NOT SQL%ISOPEN THEN 
                raise notice '%','SQL%ISOPEN : FALSE';
            ELSE
                raise notice '%','SQL%ISOPEN : NULL';
            END IF;
            IF SQL%FOUND THEN 
                raise notice '%','SQL%FOUND : TRUE';
            ELSIF NOT SQL%FOUND THEN 
                raise notice '%','SQL%FOUND : FALSE';
            ELSE
                raise notice '%','SQL%FOUND : NULL';
            END IF;            
            IF SQL%NOTFOUND THEN 
                raise notice '%','SQL%NOTFOUND : TRUE';
            ELSIF NOT SQL%NOTFOUND THEN 
                raise notice '%','SQL%NOTFOUND : FALSE';
            ELSE
                raise notice '%','SQL%NOTFOUND : NULL';
            END IF;            
            raise notice 'SQL%%ROWCOUNT :%',NVL(TO_CHAR(SQL%ROWCOUNT),'NULL');

        END LOOP;
        
    -- check cursor attris status
    IF CUR%ISOPEN THEN 
        raise notice '%','CUR%ISOPEN : TRUE';
    ELSIF NOT CUR%ISOPEN THEN 
        raise notice '%','CUR%ISOPEN : FALSE';
    ELSE
        raise notice '%','CUR%ISOPEN : NULL';
    END IF;
    IF CUR%FOUND THEN 
        raise notice '%','CUR%FOUND : TRUE';
    ELSIF NOT CUR%FOUND THEN 
        raise notice '%','CUR%FOUND : FALSE';
    ELSE
        raise notice '%','CUR%FOUND : NULL';
    END IF;            
    IF CUR%NOTFOUND THEN 
        raise notice '%','CUR%NOTFOUND : TRUE';
    ELSIF NOT CUR%NOTFOUND THEN 
        raise notice '%','CUR%NOTFOUND : FALSE';
    ELSE
        raise notice '%','CUR%NOTFOUND : NULL';
    END IF;            
    raise notice 'CUR%%ROWCOUNT :%',NVL(TO_CHAR(CUR%ROWCOUNT),'NULL');
    IF SQL%ISOPEN THEN 
        raise notice '%','SQL%ISOPEN : TRUE';
    ELSIF NOT SQL%ISOPEN THEN 
        raise notice '%','SQL%ISOPEN : FALSE';
    ELSE
        raise notice '%','SQL%ISOPEN : NULL';
    END IF;
    IF SQL%FOUND THEN 
        raise notice '%','SQL%FOUND : TRUE';
    ELSIF NOT SQL%FOUND THEN 
        raise notice '%','SQL%FOUND : FALSE';
    ELSE
        raise notice '%','SQL%FOUND : NULL';
    END IF;            
    IF SQL%NOTFOUND THEN 
        raise notice '%','SQL%NOTFOUND : TRUE';
    ELSIF NOT SQL%NOTFOUND THEN 
        raise notice '%','SQL%NOTFOUND : FALSE';
    ELSE
        raise notice '%','SQL%NOTFOUND : NULL';
    END IF;            
    raise notice 'SQL%%ROWCOUNT :%',NVL(TO_CHAR(SQL%ROWCOUNT),'NULL');
END;
/
CALL TEST_CURSOR_7();
DROP PROCEDURE TEST_CURSOR_7;
DROP TABLE TBL_H248LNK_INFO;
DROP TABLE TBL;
DROP TABLE TBL_RCWSCFG;

CREATE TABLE TBL_RCWSCFG (
    IWSNO INTEGER,
    USCDBMID SMALLINT,
    USWSBOARDMID SMALLINT,
    UCWSTYPE8100 SMALLINT,
    UCWSTYPE6600 SMALLINT,
    UCLOGINTYPE SMALLINT,
    UCTTSCAPABILITY SMALLINT,
    UCASRCAPABILITY SMALLINT,
    UCRESCAPABILITY CHARACTER VARYING(8)
);
INSERT INTO TBL_RCWSCFG VALUES (0, 184, 472, 0, 1, 0, NULL, NULL, '11011000');

CREATE TABLE TBL_TEMP_MODULE_312 (
    I_MODULENO INTEGER
);
CREATE OR REPLACE PROCEDURE TEST_TEMP
AS
BEGIN
        raise notice '%','TEST_TEMP';
END;
/
CREATE OR REPLACE PROCEDURE TEST_CRS_RPT_EMPTYSOR(FLAG INTEGER)
AS
    TYPE T_PSTMT_CRS_RPT_EMPTY IS REF CURSOR;
    CRS_RPT_EMPTY T_PSTMT_CRS_RPT_EMPTY;
	PI_MODULENO INTEGER;
	PSV_MODULETBLNAME VARCHAR2(128) := 'TBL_TEMP_MODULE_312';
	PSV_SQL  VARCHAR2(128);
	V_TEMP INTEGER := 0;
	PI_NN INTEGER := NULL;
BEGIN
	OPEN CRS_RPT_EMPTY FOR SELECT DISTINCT USCDBMID FROM TBL_RCWSCFG WHERE IWSNO >=0 AND IWSNO <= 0;
	LOOP
		FETCH CRS_RPT_EMPTY INTO PI_MODULENO;
		EXIT WHEN CRS_RPT_EMPTY%NOTFOUND;
		IF (FLAG = 0) THEN 
			-- INSERT INTO TBL_TEMP_MODULE_312, INSERT TRIGGER FUNCTION CALLED
			PSV_SQL := 'BEGIN INSERT INTO '||PSV_MODULETBLNAME||' (I_MODULENO) VALUES('||PI_MODULENO||');END;';
			EXECUTE IMMEDIATE PSV_SQL;
		ELSE
			TEST_TEMP();
		END IF;
	END LOOP;
	-- check cursor attris status
	IF CRS_RPT_EMPTY%ISOPEN THEN 
                raise notice '%','CRS_RPT_EMPTY%ISOPEN : TRUE';
	ELSIF NOT CRS_RPT_EMPTY%ISOPEN THEN 
                raise notice '%','CRS_RPT_EMPTY%ISOPEN : FALSE';
	ELSE
                raise notice '%','CRS_RPT_EMPTY%ISOPEN : NULL';
	END IF;
	IF CRS_RPT_EMPTY%FOUND THEN 
                raise notice '%','CRS_RPT_EMPTY%FOUND : TRUE';
	ELSIF NOT CRS_RPT_EMPTY%FOUND THEN 
                raise notice '%','CRS_RPT_EMPTY%FOUND : FALSE';
	ELSE
                raise notice '%','CRS_RPT_EMPTY%FOUND : NULL';
	END IF;            
	IF CRS_RPT_EMPTY%NOTFOUND THEN 
                raise notice '%','CRS_RPT_EMPTY%NOTFOUND : TRUE';
	ELSIF NOT CRS_RPT_EMPTY%NOTFOUND THEN 
                raise notice '%','CRS_RPT_EMPTY%NOTFOUND : FALSE';
	ELSE
                raise notice '%','CRS_RPT_EMPTY%NOTFOUND : NULL';
	END IF;            
        raise notice 'CRS_RPT_EMPTY%%ROWCOUNT : %',NVL(TO_CHAR(CRS_RPT_EMPTY%ROWCOUNT),'NULL');
	IF SQL%ISOPEN THEN 
                raise notice '%','SQL%ISOPEN : TRUE';
	ELSIF NOT SQL%ISOPEN THEN 
                raise notice '%','SQL%ISOPEN : FALSE';
	ELSE
                raise notice '%','SQL%ISOPEN : NULL';
	END IF;
	IF SQL%FOUND THEN 
                raise notice '%','SQL%FOUND : TRUE';
	ELSIF NOT SQL%FOUND THEN 
                raise notice '%','SQL%FOUND : FALSE';
	ELSE
                raise notice '%','SQL%FOUND : NULL';
	END IF;            
	IF SQL%NOTFOUND THEN 
                raise notice '%','SQL%NOTFOUND : TRUE';
	ELSIF NOT SQL%NOTFOUND THEN 
                raise notice '%','SQL%NOTFOUND : FALSE';
	ELSE
                raise notice '%','SQL%NOTFOUND : NULL';
	END IF;            
        raise notice 'SQL%%ROWCOUNT :%',NVL(TO_CHAR(SQL%ROWCOUNT),'NULL');
END;
/
CALL TEST_CRS_RPT_EMPTYSOR(0);
CALL TEST_CRS_RPT_EMPTYSOR(1);

create table tb_test(col1 int);
create or replace procedure proc_test()
as
v_count int;
begin
insert into tb_test select 1;
update tb_test set col1=2;
select 1 into v_count;
raise notice '%',v_count||','||SQL%FOUND || ',' || SQL%ROWCOUNT;
end;
/

declare
v_count int;
begin
insert into tb_test select 1;
update tb_test set col1=2;
select 1 into v_count;
proc_test();
v_count:=1;
raise notice '%',v_count||','||SQL%FOUND || ',' || SQL%ROWCOUNT;
end
/

drop table tb_test;

drop schema hw_cursor_part7 CASCADE;
