/*
###############################################################################
# TESTCASE NAME : postgis_setup.py
# COMPONENT(S)  : column_table drop column
# PREREQUISITE  : 
# PLATFORM      : all
# DESCRIPTION   : created table
# TAG           : column table
# TC LEVEL      : Level 1
################################################################################
*/
create schema postgis;
set current_schema=postgis;
create extension postgis;
--I1.简单点
DROP TABLE IF EXISTS GEOM_POINT;
CREATE TABLE GEOM_POINT(id int4, citiId int,name varchar(50), the_geom geometry)  ;
INSERT INTO GEOM_POINT SELECT i*100+j, i*200+j ,repeat( chr((i+j)%26 +65),4) , ST_Point(i*10,j*10) As the_geog FROM generate_series(0,8) As i CROSS JOIN generate_series(0,10) As j;
CREATE INDEX POINT_INDEX ON GEOM_POINT USING GIST (the_geom);
--UPDATE GEOM_POINT SET(id)=('2');
UPDATE GEOM_POINT SET(citiID, name, the_geom)=('2', 'abcde', ST_POINT(0, 1));
SELECT citiID,name,the_geom FROM GEOM_POINT order by id limit 20;

DROP TABLE IF EXISTS GEOM_POINT;
CREATE TABLE GEOM_POINT(id int4, citiId int,name varchar(50), the_geom geometry)  ;
INSERT INTO GEOM_POINT SELECT i*100+j, i*200+j ,repeat( chr((i+j)%26 +65),4) , ST_Point(i*10,j*10) As the_geog FROM generate_series(0,8) As i CROSS JOIN generate_series(0,10) As j;
CREATE INDEX POINT_INDEX ON GEOM_POINT USING GIST (the_geom);

--I2.简单线
DROP TABLE IF EXISTS GEOM_LINE;
CREATE TABLE GEOM_LINE(id int4, citiId int,name varchar(50), the_geom geometry) ;
INSERT INTO GEOM_LINE SELECT i*100+j, i*200+j ,repeat( chr((i+j)%26 +65),4) , ST_LineFromText('LINESTRING('||i*10||' '|| j*10||', '||i*10+5||' '|| j*10+5||')',4326) As the_geog FROM generate_series(0,8) As i CROSS JOIN generate_series(0,10) As j;
CREATE INDEX LINE_INDEX ON GEOM_LINE USING GIST (the_geom);
SELECT COUNT(*) FROM GEOM_LINE;

--I3.单部件多边形
DROP TABLE IF EXISTS GEOM_POLYGON;
CREATE TABLE GEOM_POLYGON(id int4, citiId int,name varchar(50), the_geom geometry) ;
INSERT INTO GEOM_POLYGON SELECT i*100+j, i*200+j ,repeat( chr((i+j)%26 +65),4) , ST_Polygon('LINESTRING('||i*5||' '|| j*10||', '||i*15||' '|| j*20+5||', '||i*20+5||' '|| j*15+5||', '||i*10+5||' '|| j*5|| ', '|| i*5||' '|| j*10||')',4326) As the_geog FROM generate_series(0,8) As i CROSS JOIN generate_series(0,10) As j;
CREATE INDEX POLYGON_INDEX ON GEOM_POLYGON USING GIST (the_geom);

--I4.多点
DROP TABLE IF EXISTS GEOM_MULTIPOINT;
CREATE TABLE GEOM_MULTIPOINT(id int4, citiId int,name varchar(50), the_geom geometry) ; 
INSERT INTO GEOM_MULTIPOINT(SELECT i*100+j, i*200+j ,repeat( chr((i+j)%26 +65),4) ,ST_GeomFromText('MultiPoint('||i*10||' '||j*10||','||i*5||' '||j*20||', '||i*15||' '||j*11||')')As the_geog FROM generate_series(0,10) As i CROSS JOIN generate_series(0,10) As j);

--I5.多线
DROP TABLE IF EXISTS GEOM_MULTILINESTRING;
CREATE TABLE GEOM_MULTILINESTRING(id int4, citiId int,name varchar(50), the_geom geometry) ;
INSERT INTO GEOM_MULTILINESTRING(SELECT i*100+j, i*200+j ,repeat( chr((i+j)%26 +65),4) ,ST_GeomFromText('MultiLineString(('||i*10||' '||j*10||','||i*5||' '||j*20||', '||i*15||' '||j*11||'))')As the_geog FROM generate_series(0,10) As i CROSS JOIN generate_series(0,10) As j);

--I6.多部件多边形
DROP TABLE IF EXISTS GEOM_MULTIPOLYGON;
CREATE TABLE GEOM_MULTIPOLYGON(id int4, citiId int,name varchar(50), the_geom geometry);
INSERT INTO GEOM_MULTIPOLYGON(SELECT i*100+j, i*200+j ,repeat( chr((i+j)%26 +65),4) ,ST_GeomFromText('MultiPolygon((('||i*10||' '||j*10||','||i*5||' '||j*20||', '||i*15||' '||j*11||', '||i*3||' '||j*2||' , '||i*10||' '||j*10||')), (('||i*10||' '||j*10||','||i*5||' '||j*20||',  '||i*3||' '||j*2||' , '||i*10||' '||j*10||')))')As the_geog FROM generate_series(0,10) As i CROSS JOIN generate_series(0,10) As j);

--I7.复杂几何
DROP TABLE IF EXISTS GEOM_COLLECTION;
CREATE TABLE GEOM_COLLECTION(id int4, citiID int, name varchar(50), the_geom geometry);
INSERT INTO GEOM_MULTIPOLYGON(SELECT i*100+j, i*200+j ,repeat( chr((i+j)%26 +65),4) ,ST_GeomFromText('MultiPolygon((('||i*10||' '||j*10||','||i*5||' '||j*20||', '||i*15||' '||j*11||
', '||i*3||' '||j*2||' , '||i*10||' '||j*10||')), (('||i*10||' '||j*10||','||i*5||' '||j*20||',  '||i*3||' '||j*2||' , '||i*10||' '||j*10||')))')As the_geog FROM generate_series(0,10) As i
CROSS JOIN generate_series(0,10) As j);

ANALYZE GEOM_MULTIPOLYGON;
ANALYZE GEOM_MULTILINESTRING;
ANALYZE GEOM_MULTIPOINT;
ANALYZE GEOM_POLYGON;
ANALYZE GEOM_POINT;
ANALYZE GEOM_LINE;
ANALYZE GEOM_COLLECTION;
