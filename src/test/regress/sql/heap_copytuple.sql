CREATE TABLE heap_copytuple_s (rf_a SERIAL PRIMARY KEY,
	b INT);

CREATE TABLE heap_copytuple (a SERIAL PRIMARY KEY,
	b INT,
	c TEXT,
	d TEXT
	);

CREATE INDEX heap_copytuple_b ON heap_copytuple (b);
CREATE INDEX heap_copytuple_c ON heap_copytuple (c);
CREATE INDEX heap_copytuple_c_b ON heap_copytuple (c,b);
CREATE INDEX heap_copytuple_b_c ON heap_copytuple (b,c);

INSERT INTO heap_copytuple_s (b) VALUES (0);
INSERT INTO heap_copytuple_s (b) SELECT b FROM heap_copytuple_s;
INSERT INTO heap_copytuple_s (b) SELECT b FROM heap_copytuple_s;
INSERT INTO heap_copytuple_s (b) SELECT b FROM heap_copytuple_s;
INSERT INTO heap_copytuple_s (b) SELECT b FROM heap_copytuple_s;
INSERT INTO heap_copytuple_s (b) SELECT b FROM heap_copytuple_s;
drop table heap_copytuple_s cascade;
INSERT INTO heap_copytuple (b, c) VALUES (11, 'once');
INSERT INTO heap_copytuple (b, c) VALUES (10, 'diez');
INSERT INTO heap_copytuple (b, c) VALUES (31, 'treinta y uno');
INSERT INTO heap_copytuple (b, c) VALUES (22, 'veintidos');
INSERT INTO heap_copytuple (b, c) VALUES (3, 'tres');
INSERT INTO heap_copytuple (b, c) VALUES (20, 'veinte');
INSERT INTO heap_copytuple (b, c) VALUES (23, 'veintitres');
INSERT INTO heap_copytuple (b, c) VALUES (21, 'veintiuno');
INSERT INTO heap_copytuple (b, c) VALUES (4, 'cuatro');
INSERT INTO heap_copytuple (b, c) VALUES (14, 'catorce');
INSERT INTO heap_copytuple (b, c) VALUES (2, 'dos');
INSERT INTO heap_copytuple (b, c) VALUES (18, 'dieciocho');
INSERT INTO heap_copytuple (b, c) VALUES (27, 'veintisiete');
INSERT INTO heap_copytuple (b, c) VALUES (25, 'veinticinco');
INSERT INTO heap_copytuple (b, c) VALUES (13, 'trece');
INSERT INTO heap_copytuple (b, c) VALUES (28, 'veintiocho');
INSERT INTO heap_copytuple (b, c) VALUES (32, 'treinta y dos');
INSERT INTO heap_copytuple (b, c) VALUES (5, 'cinco');
INSERT INTO heap_copytuple (b, c) VALUES (29, 'veintinueve');
INSERT INTO heap_copytuple (b, c) VALUES (1, 'uno');
INSERT INTO heap_copytuple (b, c) VALUES (24, 'veinticuatro');
INSERT INTO heap_copytuple (b, c) VALUES (30, 'treinta');
INSERT INTO heap_copytuple (b, c) VALUES (12, 'doce');
INSERT INTO heap_copytuple (b, c) VALUES (17, 'diecisiete');
INSERT INTO heap_copytuple (b, c) VALUES (9, 'nueve');
INSERT INTO heap_copytuple (b, c) VALUES (19, 'diecinueve');
INSERT INTO heap_copytuple (b, c) VALUES (26, 'veintiseis');
INSERT INTO heap_copytuple (b, c) VALUES (15, 'quince');
INSERT INTO heap_copytuple (b, c) VALUES (7, 'siete');
INSERT INTO heap_copytuple (b, c) VALUES (16, 'dieciseis');
INSERT INTO heap_copytuple (b, c) VALUES (8, 'ocho');
-- This entry is needed to test that TOASTED values are copied correctly.
INSERT INTO heap_copytuple (b, c, d) VALUES (6, 'seis', repeat('xyzzy', 100000));

CLUSTER heap_copytuple_c ON heap_copytuple;
INSERT INTO heap_copytuple (b, c) VALUES (1111, 'this should fail');
ALTER TABLE heap_copytuple CLUSTER ON heap_copytuple_b_c;

-- Try turning off all clustering
ALTER TABLE heap_copytuple SET WITHOUT CLUSTER;
drop table heap_copytuple cascade;





create table tGin122 (
        name varchar(50) not null, 
        age int, 
        birth date, 
        ID varchar(50) , 
        phone varchar(15),
        carNum varchar(50),
        email varchar(50), 
        info text, 
        config varchar(50) default 'english',
        tv tsvector,
        i varchar(50)[],
        ts tsquery);
insert into tGin122 values('Linda', 20, '1996-06-01', '140110199606012076', '13454333333', '???A QL666', 'linda20@sohu.com', 'When he was busy with teaching men the art of living, Prometheus had left a bigcask in the care of Epimetheus. He had warned his brother not to open the lid. Pandora was a curious woman. She had been feeling very disappointed that her husband did not allow her to take a look at the contents of the cask. One day, when Epimetheus was out, she lifted the lid and out it came unrest and war, Plague and sickness, theft and violence, grief, sorrow, and all the other evils. The human world was hence to experience these evils. Only hope stayed within the mouth of the jar and never flew out. So men always have hope within their hearts.
?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
', 'ngram', '', '{''brother'',''????????????'',''???????????????''}',NULL);
insert into tGin122 values('??????', 20,  '1996-07-01', '140110199607012076', '13514333333', '???K QL662', 'zhangsan@163.com', '????????????????????????????????????????????????????????????????????????????????????
?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
?????????????????????????????????????????????', 'ngram', '',  '{''?????????????????????'',''??????'',''??????????????????''}',NULL); 
insert into tGin122 values('Sara', 20,  '1996-07-02', '140110199607022076', '13754333333', '???A QL661', 'sara20@sohu.com', '??????????????????????????????hypotaxis????????????????????????parataxis???>???????????????????????????????????????????????????????????????connectives??????????????????????????????inflection???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????>?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????', 'english', '',  '{''parataxis'',''???????????????'',''??????''}',NULL);
insert into tGin122 values('Mira', 20,  '1996-08-01', '140110199608012076', '13654333333', '???A QL660', 'mm20@sohu.com', '[??????]??????????????????????????????????????????????????????????????????????????????>?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????IAA????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????[??????]???????????????????????????????????????????????????????????????IAA??????????????????????????????????????????????????????IAA???????????????????????????????????????????????????????????????????????????', 'english', '',  '{''????????????'',''????????????'',''???????????????''}',NULL);
insert into tGin122 values('Amy', 20,  ' 1996-09-01', '140110199609012076', '13854333333', '???A QL663', 'amy2008@163.com', '[??????]??????????????????????????????????????????????????????Current concern focus on ??????, and on???????????????????????????????????????on???????????????intrusionon???????????????????????????????????????on???????????????focuson????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????', 'ngram', '',  '{''intrusionon'',''13854333333'',''140110199609012076''}',NULL);
insert into tGin122 values('????????? ', 20,  ' 1996-09-01', '44088319921103106X', '13854333333', '???YWZJW0', 'si2008@163.com', '?????????????????????????????????????????????????????????????????????????????????>??????????????????[??????]???????????????????????????This led to a whole new field of academic research??????????????????????????????????????????including the milestone paper by Paterson and co-workers in 1988???????????????the milestone pape????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????using an approach that could be applied to dissect the genetic make-up of any physiological, morphological and behavioural trat in plants and animals?????????????????????????????????????????????????????????', 'ngram', '',  '{''44088319921103106X'',''????????????'',''??????''}',NULL);
create index tgin122_idx1 on tgin122 (substr(email,2,5));
create index tgin122_idx2 on tgin122 (upper(info));
set default_statistics_target=-2;
analyze tGin122 ((tv, ts));
select * from pg_ext_stats where schemaname='distribute_stat_2' and tablename='tgin122' order by attname;
alter table tGin122 delete statistics ((tv, ts));
update tGin122 set tv=to_tsvector(config::regconfig, coalesce(name,'') || ' ' || coalesce(ID,'') || ' ' || coalesce(carNum,'') || ' ' || coalesce(phone,'') || ' ' || coalesce(email,'') || ' ' || coalesce(info,''));
update tGin122 set ts=to_tsquery('ngram', coalesce(phone,'')); 
analyze tGin122 ((tv, ts));
select * from pg_ext_stats where schemaname='distribute_stat_2' and tablename='tgin122' order by attname;
alter table tGin122 delete statistics ((tv, ts));
select * from pg_ext_stats where schemaname='distribute_stat_2' and tablename='tgin122' order by attname;
alter table tGin122 add statistics ((tv, ts));
analyze tGin122;
select * from pg_ext_stats where schemaname='distribute_stat_2' and tablename='tgin122' order by attname;
select * from pg_stats where tablename='tgin122' and attname = 'tv';
select attname,avg_width,n_distinct,histogram_bounds from pg_stats where tablename='tgin122_idx1';
drop table tgin122 cascade;








CREATE SCHEMA regress_rls_schema;
GRANT CREATE ON SCHEMA regress_rls_schema to public;
GRANT USAGE ON SCHEMA regress_rls_schema to public;
-- reconnect
\c
SET search_path = regress_rls_schema;
	
CREATE TABLE regress_rls_schema.document_row(
    did     int primary key,
    cid     int,
    dlevel  int not null,
    dauthor name,
    dtitle  text
);
GRANT ALL ON regress_rls_schema.document_row TO public;
INSERT INTO regress_rls_schema.document_row VALUES
    ( 1, 11, 1, 'regress_rls_bob', 'my first novel'),
    ( 2, 11, 5, 'regress_rls_bob', 'my second novel'),
    ( 3, 22, 7, 'regress_rls_bob', 'my science fiction'),
    ( 4, 44, 9, 'regress_rls_bob', 'my first manga'),
    ( 5, 44, 3, 'regress_rls_bob', 'my second manga'),
    ( 6, 22, 2, 'regress_rls_peter', 'great science fiction'),
    ( 7, 33, 6, 'regress_rls_peter', 'great technology book'),
    ( 8, 44, 4, 'regress_rls_peter', 'great manga'),
    ( 9, 22, 5, 'regress_rls_david', 'awesome science fiction'),
    (10, 33, 4, 'regress_rls_david', 'awesome technology book'),
    (11, 55, 8, 'regress_rls_alice', 'great biography'),
    (12, 33, 10, 'regress_rls_admin', 'physical technology'),
    (13, 55, 5, 'regress_rls_single_user', 'Beethoven biography');
ANALYZE regress_rls_schema.document_row;
UPDATE document_row SET dlevel = dlevel + 1 - 1 WHERE did > 1;
INSERT INTO document_row VALUES (100, 49, 1, 'regress_rls_david', 'testing sorting of policies');
DELETE FROM document_row WHERE did = 100;
INSERT INTO document_row VALUES (100, 49, 1, 'regress_rls_david', 'testing sorting of policies');
DELETE FROM document_row WHERE did = 100 RETURNING dauthor, did;

CREATE TABLE regress_rls_schema.account_row(
    aid   int,
    aname varchar(100)
) WITH (ORIENTATION=row);
CREATE ROW LEVEL SECURITY POLICY p01 ON document_row AS PERMISSIVE
    USING (dlevel <= (SELECT aid FROM account_row WHERE aname = current_user));

ALTER POLICY p01 ON document_row USING (dauthor = current_user);
ALTER POLICY p01 ON document_row RENAME TO p12;
ALTER POLICY p12 ON document_row RENAME TO p13;
ALTER POLICY p13 ON document_row RENAME TO p01;
SELECT * FROM pg_rlspolicies ORDER BY tablename, policyname;
drop schema regress_rls_schema cascade;
reset search_path;



















CREATE TABLE y (a INTEGER PRIMARY KEY INITIALLY DEFERRED)  ;
INSERT INTO y SELECT generate_series(1, 10);
TRUNCATE TABLE y;
INSERT INTO y SELECT generate_series(1, 10);

CREATE FUNCTION y_trigger() RETURNS trigger AS $$
begin
  raise notice 'y_trigger: a = %', new.a;
  return new;
end;
$$ LANGUAGE plpgsql;

CREATE TRIGGER y_trig BEFORE INSERT ON y FOR EACH ROW
    EXECUTE PROCEDURE y_trigger();

WITH t AS (
    INSERT INTO y
    VALUES
        (21),
        (22),
        (23)
    RETURNING *
)
SELECT * FROM t;

drop table y cascade;
drop function y_trigger;

