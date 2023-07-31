set current_schema=postgis;
/*
###############################################################################
# TESTCASE NAME : postgis_realation_measure.sql
# COMPONENT(S)  : test function realtionship_measure
# PREREQUISITE  : 
# PLATFORM      : SUSE11.3
# DESCRIPTION   : ��֤postgis���ζ����ϵ�Ͳ�������
# TAG           : ST_3DClosestPoint,ST_3DDistance
# TC LEVEL      : Level 1
################################################################################
*/

--S1.��֤����ST_3DClosestPoint��������������ӽ��ĵ�
SELECT ST_AsEWKT(ST_3DClosestPoint(line,pt)) AS cp3d_line_pt,ST_AsEWKT(ST_ClosestPoint(line,pt)) As cp2d_line_pt 
FROM (SELECT 
'POINT(100 100 30)'::geometry As pt,
'LINESTRING (20 80 20, 98 190 1, 110 180 3, 50 75 1000)':: geometry As line
) As foo;
--S2.��֤����ST_3DDistance��������������ά�ѿ�����С����
SELECT ST_3DDistance(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 4)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163)
) As dist_3d,
ST_Distance(
ST_Transform(ST_GeomFromText('POINT(-72.1235 42.3521)',4326),2163),
ST_Transform(ST_GeomFromText('LINESTRING(-72.1260 42.45, -72.123 42.1546)', 4326) ,2163)
) As dist_2d;
--S3.��֤����ST_3DDWithin�������������3D���뵥λ�ڷ���true
SELECT ST_3DDWithin(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 4)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163),
126.8
) As within_dist_3d,
ST_DWithin(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 4)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163),
126.8
) As within_dist_2d;
--S4.��֤����T_3DDFullyWithin������м��ζ���ָ����Χ�ڷ���true 
SELECT ST_3DDFullyWithin(geom_a, geom_b, 10) as D3DFullyWithin10, ST_3DDWithin(geom_a, geom_b, 10) as D3DWithin10,
ST_DFullyWithin(geom_a, geom_b, 20) as D2DFullyWithin20,
ST_3DDFullyWithin(geom_a, geom_b, 20) as D3DFullyWithin20 from
(select ST_GeomFromEWKT('POINT(1 1 2)') as geom_a,
ST_GeomFromEWKT('LINESTRING(1 5 2, 2 7 20, 1 9 100, 14 12 3)') as geom_b) t1;
--S5.��֤����ST_3DIntersects�������ͼ���ڿռ��ཻ����true
SELECT ST_3DIntersects(pt, line), ST_Intersects(pt,line)
FROM (SELECT 'POINT(0 0 2)'::geometry As pt,
'LINESTRING (0 0 1, 0 2 3 )'::geometry As line) As foo;
--S6.��֤����ST_3DLongestLine�����������μ���ά���
SELECT ST_AsEWKT(ST_3DLongestLine(line,pt)) AS lol3d_line_pt,
ST_AsEWKT(ST_LongestLine(line,pt)) As lol2d_line_pt
FROM (SELECT 'POINT(100 100 30)'::geometry As pt,
'LINESTRING (20 80 20, 98 190 1, 110 180 3, 50 75 1000)':: geometry As line
) As foo;
--S7.��֤����ST_3DMaxDistance��������������ά�ѿ���������
SELECT ST_3DMaxDistance(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 10000)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163)
) As dist_3d,
ST_MaxDistance(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 10000)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163)
) As dist_2d;
--S8.��֤����ST_3DShortestLine�����������μ���ά�����
SELECT ST_AsEWKT(ST_3DShortestLine(line,pt)) AS shl3d_line_pt,
ST_AsEWKT(ST_ShortestLine(line,pt)) As shl2d_line_pt
FROM (SELECT 'POINT(100 100 30)'::geometry As pt,
'LINESTRING (20 80 20, 98 190 1, 110 180 3, 50 75 1000)':: geometry As line
) As foo;
--S9.��֤����ST_Area�����������
SELECT ST_Area(the_geom) As sqft, ST_Area(the_geom)*POWER(0.3048,2) As sqm
FROM (SELECT
ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,
743265 2967450,743265.625 2967416,743238 2967416))',2249) ) As foo(the_geom);
--S10.��֤����ST_Azimuth���ظ��豱��λ�ķ�λ��
SELECT degrees(ST_Azimuth(ST_Point(25, 45), ST_Point(75, 100))) AS degA_B,
degrees(ST_Azimuth(ST_Point(75, 100), ST_Point(25, 45))) AS degB_A;
--S11.��֤����ST_Centroid���ؼ�����ļ�������
SELECT ST_AsText(ST_Centroid('MULTIPOINT ( -1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6 )'));
--S12.��֤����ST_ClosestPoint������ӽ��������εĶ�ά��
SELECT ST_AsText(ST_ClosestPoint(pt,line) ) AS cp_pt_line,
ST_AsText(ST_ClosestPoint(line,pt )) As cp_line_pt
FROM (SELECT 'POINT(100 100)'::geometry As pt,
'LINESTRING (20 80, 98 190, 110 180, 50 75 )'::geometry As line
) As foo;
--S13.��֤����ST_Contains����A�ڼ���B���ڲ�
SELECT ST_Contains(smallc, bigc) As smallcontainsbig,
ST_Contains(bigc,smallc) As bigcontainssmall,
ST_Contains(bigc, ST_Union(smallc, bigc)) as bigcontainsunion,
ST_Equals(bigc, ST_Union(smallc, bigc)) as bigisunion,
ST_Covers(bigc, ST_ExteriorRing(bigc)) As bigcoversexterior,
ST_Contains(bigc, ST_ExteriorRing(bigc)) As bigcontainsexterior
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc,
ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;
--S14.��֤����ST_ContainsProperly����A�ڼ���B���ڲ��Ҳ���B�ı߽��ཻ
SELECT ST_ContainsProperly(smallc, bigc) As smallcontainspropbig,
ST_ContainsProperly(bigc,smallc) As bigcontainspropsmall,
ST_ContainsProperly(bigc, ST_Union(smallc, bigc)) as bigcontainspropunion,
ST_Equals(bigc, ST_Union(smallc, bigc)) as bigisunion,
ST_Covers(bigc, ST_ExteriorRing(bigc)) As bigcoversexterior,
ST_ContainsProperly(bigc, ST_ExteriorRing(bigc)) As bigcontainsexterior
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc,
ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;
--S15.��֤����ST_Covers����B�ĵ㲻�ڼ���A֮��
SELECT ST_Covers(smallc,smallc) As smallinsmall,
ST_Covers(smallc, bigc) As smallcoversbig,
ST_Covers(bigc, ST_ExteriorRing(bigc)) As bigcoversexterior,
ST_Contains(bigc, ST_ExteriorRing(bigc)) As bigcontainsexterior
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc,
ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;
--S16.��֤����ST_CoveredBy����B�ĵ㲻�ڼ���A֮��
SELECT ST_CoveredBy(smallc,smallc) As smallinsmall,
ST_CoveredBy(smallc, bigc) As smallcoveredbybig,
ST_CoveredBy(ST_ExteriorRing(bigc), bigc) As exteriorcoveredbybig,
ST_Within(ST_ExteriorRing(bigc),bigc) As exeriorwithinbig
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc,
ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;
--S17.��֤����ST_Crosses���������ཻ�����غ�
SELECT 'crosses', ST_crosses('LINESTRING(0 10, 0 -10)'::geometry, 'LINESTRING(-4 0, 1 1)'::geometry);
--S18.��֤����ST_LineCrossingDirection���������߶εĽ�����Ϊ
SELECT ST_LineCrossingDirection(foo.line1, foo.line2) As l1_cross_l2 ,
ST_LineCrossingDirection(foo.line2, foo.line1) As l2_cross_l1
FROM (
SELECT
ST_GeomFromText('LINESTRING(25 169,89 114,40 70,86 43)') As line1,
ST_GeomFromText('LINESTRING(171 154,20 140,71 74,161 53)') As line2
) As foo;
--S19.��֤����ST_Disjoint�������β������κοռ�
SELECT ST_Disjoint('POINT(0 0)'::geometry, 'LINESTRING ( 0 0, 0 2 )'::geometry);
--S20.��֤����ST_Distance������������֮���2D�ѿ�������
SELECT ST_Distance(
ST_GeomFromText('POINT(-72.1235 42.3521)',4326),
ST_GeomFromText('LINESTRING(-72.1260 42.45, -72.123 42.1546)', 4326)
);
--S20.��֤����ST_HausdorffDistance��������������֮���Hausdorff����
SELECT ST_HausdorffDistance(
'LINESTRING (0 0, 2 0)'::geometry,
'MULTIPOINT (0 1, 1 0, 2 1)'::geometry);
--S21.��֤����ST_MaxDistance����ͶӰ��λ����������֮���������
SELECT ST_MaxDistance('POINT(0 0)'::geometry, 'LINESTRING ( 2 0, 0 2 )'::geometry );
--S22.��֤����ST_DistanceSphere������������ͼ��֮�����С����
SELECT round(CAST(ST_DistanceSphere(ST_Centroid(the_geom), ST_GeomFromText('POINT(-118 38) ',4326)) As numeric),2) As dist_meters,
round(CAST(ST_Distance(ST_Transform(ST_Centroid(the_geom),32611),
ST_Transform(ST_GeomFromText('POINT(-118 38)', 4326),32611)) As numeric),2) As dist_utm11_meters,
round(CAST(ST_Distance(ST_Centroid(the_geom), ST_GeomFromText('POINT(-118 38)', 4326)) As numeric),5) As dist_degrees,
round(CAST(ST_Distance(ST_Transform(the_geom,32611),
ST_Transform(ST_GeomFromText('POINT(-118 38)', 4326),32611)) As numeric),2) As min_dist_line_point_meters
FROM
(SELECT ST_GeomFromText('LINESTRING(-118.584 38.374,-118.583 38.5)', 4326) As the_geom) as foo;
--S23.��֤����ST_DistanceSpheroid���ظ����ض�������ļ���ͼ����С����
--S24.��֤����ST_DFullyWithin���м���ͼ�ζ��ڱ˴˾�����
SELECT ST_DFullyWithin(geom_a, geom_b, 10) as DFullyWithin10, ST_DWithin(geom_a, geom_b, 10) as DWithin10, ST_DFullyWithin(geom_a, geom_b, 20) as DFullyWithin20 from
(select ST_GeomFromText('POINT(1 1)') as geom_a,ST_GeomFromText('LINESTRING(1 5, 2 7, 1 9, 14 12)') as geom_b) t1;
--S25.��֤����ST_DWithin����ͼ���ڱ˴�ָ������
SELECT '#1264', ST_DWithin('POLYGON((-10 -10, -10 10, 10 10, 10 -10, -10 -10))'::geography, 'POINT(0 0)'::geography, 0);
--S26.��֤����ST_Equals�����ļ�������ͬ
SELECT ST_Equals(ST_GeomFromText('LINESTRING(0 0, 10 10)'),
ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10)'));
--S27.��֤����ST_HasArc����Բ���ַ���
SELECT ST_HasArc(ST_Collect('LINESTRING(1 2, 3 4, 5 6)', 'CIRCULARSTRING(1 1, 2 3, 4 5, 6 7, 5 6)'));
--S28.��֤����ST_Intersects������2D�ռ����ཻ
SELECT ST_Intersects('POINT(0 0)'::geometry, 'LINESTRING ( 2 0, 0 2 )'::geometry);
--S29.��֤����ST_Length���ؼ���ͼ�ε�2D����
SELECT ST_Length(ST_GeomFromText('LINESTRING(743238 2967416,743238 2967450,743265 2967450,
743265.625 2967416,743238 2967416)',2249));
--S30.��֤����ST_Length2D���ؼ���ͼ�ε�2D����

--S31.��֤����ST_3DLength���ؼ���ͼ�ε�3D����
SELECT ST_3DLength(ST_GeomFromText('LINESTRING(743238 2967416 1,743238 2967450 1,743265 2967450 3,
743265.625 2967416 3,743238 2967416 3)',2249));
--S33.��֤����ST_Length2D_Spheroid�����������ϼ���ͼ�ε�2D����
SELECT ST_Length2D_Spheroid( the_geom, sph_m ) As tot_len,
ST_Length2D_Spheroid(ST_GeometryN(the_geom,1), sph_m) As len_line1,
ST_Length2D_Spheroid(ST_GeometryN(the_geom,2), sph_m) As len_line2
FROM (SELECT ST_GeomFromText('MULTILINESTRING((-118.584 38.374,-118.583 38.5),
(-71.05957 42.3589 , -71.061 43))') As the_geom,
CAST('SPHEROID["GRS_1980",6378137,298.257222101]' As spheroid) As sph_m) as foo;
--S34.��֤����ST_LongestLine������������ͼ�ε��������
SELECT ST_AsText(
ST_LongestLine('POINT(100 100)':: geometry,
'LINESTRING (20 80, 98 190, 110 180, 50 75 )'::geometry)
) As lline;
--S35.��֤����ST_OrderingEquals������������ͬ���ҵ�ķ���Ҳ��ͬ
SELECT ST_OrderingEquals(ST_GeomFromText('LINESTRING(0 0, 10 10)'),
ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10)'));
--S36.��֤����ST_Overlaps�����干��ռ�
SELECT ST_Overlaps(a,b) As a_overlap_b,
ST_Crosses(a,b) As a_crosses_b,
ST_Intersects(a, b) As a_intersects_b, ST_Contains(b,a) As b_contains_a
FROM (SELECT ST_GeomFromText('POINT(1 0.5)') As a, ST_GeomFromText('LINESTRING(1 0, 1 1, 3 5)') As b)
As foo;
--S37.��֤����ST_Perimeter���ؼ��α߽�ĳ���
SELECT ST_Perimeter(ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,743265 2967450,
743265.625 2967416,743238 2967416))', 2249));
--S38.��֤����ST_Perimeter2D���ض���εĶ�ά�߽�

--S39.��֤����ST_3DPerimeter���ض���ε���ά�߽�
SELECT ST_3DPerimeter(the_geom), ST_Perimeter2d(the_geom), ST_Perimeter(the_geom) FROM
(SELECT ST_GeomFromEWKT('SRID=2249;POLYGON((743238 2967416 2,743238 2967450 1,
743265.625 2967416 1,743238 2967416 2))') As the_geom) As foo;
--S40.��֤����ST_PointOnSurface����һ��λ�ڱ���ĵ�
SELECT ST_AsText(ST_PointOnSurface('POINT(0 5)'::geometry));
--S41.��֤����ST_Project���ش����ͶӰ�ĵ�
SELECT ST_AsText(ST_Project('POINT(0 0)'::geography, 100000, radians(45.0)));
--S42.��֤����ST_Relate���������
SELECT ST_Relate(ST_GeometryFromText('POINT(1 2)'), ST_Buffer(ST_GeometryFromText('POINT(1 
2)'),2), '*FF*FF212');
--S43.��֤����ST_RelateMatch���������ƥ��
SELECT ST_RelateMatch('101202FFF', 'TTTTTTFFF') ;
--S44.��֤����T_ShortestLine�����������μ�Ķ�ά�����
SELECT ST_AsText(
ST_ShortestLine('POINT(100 100) '::geometry,
'LINESTRING (20 80, 98 190, 110 180, 50 75 )'::geometry)
) As sline;
--S45.��֤����ST_Touches���������Ӵ�
SELECT ST_Touches('LINESTRING(0 0, 1 1, 0 2)'::geometry, 'POINT(1 1)'::geometry);
--S46.��֤����ST_Within����A��ȫλ�ڼ���B��
SELECT ST_Within(smallc,smallc) As smallinsmall,
ST_Within(smallc, bigc) As smallinbig,
ST_Within(bigc,smallc) As biginsmall,
ST_Within(ST_Union(smallc, bigc), bigc) as unioninbig,
ST_Within(bigc, ST_Union(smallc, bigc)) as biginunion,
ST_Equals(bigc, ST_Union(smallc, bigc)) as bigisunion
 FROM
(
SELECT ST_Buffer(ST_GeomFromText('POINT(50 50)'), 20) As smallc,
ST_Buffer(ST_GeomFromText('POINT(50 50)'), 40) As bigc) As foo;

--I2.join����
--S1.��֤����ST_3DClosestPoint join����
SELECT ST_AsEWKT(ST_3DClosestPoint(ST_setsrid(geom_point.the_geom,4326), st_setsrid(geom_line.the_geom,4326))) from geom_point join geom_line on geom_point.id=15 and geom_line.id=7;  
--S2.��֤����ST_3DDistance join����
SELECT ST_3DDistance(ST_setsrid(geom_point.the_geom,4326), st_setsrid(geom_line.the_geom,4326)) from geom_line join geom_point on geom_point.id=geom_line.id order by geom_point.id limit 10;
--S3.��֤����ST_3DDWithin�������������3D���뵥λ�ڷ���true
SELECT ST_3DDWithin(ST_setsrid(geom_line.the_geom,4326), st_setsrid(geom_polygon.the_geom,4326),10) from geom_line join geom_polygon on geom_polygon.id=geom_line.id order by geom_line.id limit 10;

--����
select ST_FindExtent('geom_polygon','the_geom');
select ST_Find_Extent('postgis','geom_polygon','the_geom');

select DropGeometryColumn('public2','cities');

select UpdateGeometrySRID('postgis','cities',3453);

CREATE TABLE cities ( id int4, name varchar(50) );
SELECT AddGeometryColumn('cities', 'the_geom', 4326, 'POINT', 2);
INSERT INTO cities (id, the_geom, name) VALUES (1,ST_GeomFromText('POINT(-0.1257 51.508)',4326),'London, England');
SELECT UpdateGeometrySRID('cities','the_geom',4333);
SELECT find_srid('postgis','cities', 'the_geom');
SELECT DROPGeometryColumn('cities', 'the_geom');
SELECT DropGeometryTable ('cities');
SELECT AddGeometryColumn('cities', 'the_geom', 4326, 'POINT', 2);
SELECT DropGeometryTable ('cities');

CREATE TABLE cities ( id int4, name varchar(50) );
SELECT AddGeometryColumn('postgis', 'cities', 'the_geom', 4326, 'POINT', 2);
INSERT INTO cities (id, the_geom, name) VALUES (1,ST_GeomFromText('POINT(-0.1257 51.508)',4326),'London, England');
SELECT UpdateGeometrySRID('postgis','cities','the_geom',4333);
SELECT find_srid('postgis','cities', 'the_geom');
SELECT DROPGeometryColumn('postgis','cities', 'the_geom');
SELECT DropGeometryTable ('postgis','cities');
SELECT AddGeometryColumn('cities', 'the_geom', 4326, 'POINT', 2);
SELECT DropGeometryTable ('cities');

CREATE SCHEMA cities_schema;
CREATE TABLE cities_schema.cities ( id int4, name varchar(50) );
SELECT AddGeometryColumn('cities_schema', 'cities', 'the_geom', 4326, 'POINT', 2);
INSERT INTO cities_schema.cities (id, the_geom, name) VALUES (1,ST_GeomFromText('POINT(-0.1257 51.508)',4326),'London, England');
SELECT UpdateGeometrySRID('cities_schema','cities','the_geom',4333);
SELECT find_srid('cities_schema','cities', 'the_geom');

SELECT DROPGeometryColumn('cities_schema','cities', 'the_geom');
SELECT DropGeometryTable ('cities_schema','cities');
drop schema cities_schema cascade;

CREATE TABLE postgis.myspatial_table_cs(gid serial, geom geometry);
INSERT INTO myspatial_table_cs(geom) VALUES(ST_GeomFromText('LINESTRING(1 2, 3 4)',4326) );
SELECT Populate_Geometry_Columns('postgis.myspatial_table_cs'::regclass, false);
\d myspatial_table_cs 
DROP TABLE postgis.myspatial_table_cs;

CREATE TABLE postgis.myspatial_table_cs(gid serial, geom geometry);
INSERT INTO myspatial_table_cs(geom) VALUES(ST_GeomFromText('LINESTRING(1 2, 3 4)',4326) );
SELECT Populate_Geometry_Columns( false);
\d myspatial_table_cs
DROP TABLE postgis.myspatial_table_cs;
