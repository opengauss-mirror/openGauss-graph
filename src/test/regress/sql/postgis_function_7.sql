set current_schema=postgis;
SELECT ST_Dump(the_geom) AS the_geom FROM geom_point order by id limit 10;
--Break a compound curve into its constituent linestrings and circularstrings
SELECT ST_AsEWKT(a.geom), ST_HasArc(a.geom)FROM ( SELECT (ST_Dump(p_geom)).geom AS geom FROM (SELECT ST_GeomFromEWKT('COMPOUNDCURVE(CIRCULARSTRING(0 0, 1 1, 1 0),(1 0, 0 1))') AS p_geom) AS b) AS a;
--S10.��֤����ST_DumpPoints������ɼ���ͼ�ε����е��һ��Geometry_dumpֵ
SELECT edge_id, (dp).path[1] As index, ST_AsText((dp).geom) As wktnode FROM (SELECT 1 As edge_id, ST_DumpPoints(ST_GeomFromText('LINESTRING(1 2, 3 4, 10 10)')) AS dp UNION ALL SELECT 2 As edge_id, ST_DumpPoints(ST_GeomFromText('LINESTRING(3 5, 5 6, 9 10)')) AS dp) As foo;

--S12.��֤����ST_FlipCoordinates���ط�תX���Y��ļ�����
SELECT ST_AsEWKT(ST_FlipCoordinates(GeomFromEWKT('POINT(1 2)')));
--S13.��֤����ST_Intersection�����������εĹ�����
SELECT ST_AsText(ST_Intersection('POINT(0 0)'::geometry, 'LINESTRING ( 2 0, 0 2 )':: geometry));
--S14.��֤����ST_LineToCurve���߻�����ת����ѭ��������
SELECT ST_AsText(ST_LineToCurve(foo.the_geom)) As curvedastext,ST_AsText(foo.the_geom) As non_curvedastext
FROM (SELECT ST_Buffer('POINT(1 3)'::geometry, 3) As the_geom) As foo;
SELECT ST_AsText(ST_LineToCurve(geom)) As curved, ST_AsText(geom) AS not_curved
FROM (SELECT ST_Translate(ST_Force3D(ST_Boundary(ST_Buffer(ST_Point(1,3), 2,2))),0,0,3) AS geom) AS foo;
--S17.��֤����ST_MinimumBoundingCircle���ؿ�����ȫ�������ε���СԲ
SELECT ST_AsText(ST_MinimumBoundingCircle(
ST_Collect(
ST_GeomFromEWKT('LINESTRING(55 75,125 150)'),
ST_Point(20, 80)), 8
)) As wktmbc;
--S18.��֤����ST_Polygonize����һ�����μ���
SELECT '#3470b', ST_Area(ST_Polygonize(ARRAY[NULL, 'LINESTRING (0 0, 10 0, 10 10)', NULL, 'LINESTRING (0 0, 10 10)', NULL]::geometry[]));
--S19.��֤����ST_Node����һ���ߴ��Ľڵ�
SELECT ST_AsText(
ST_Node('LINESTRINGZ(0 0 0, 10 10 10, 0 10 5, 10 0 3)'::geometry)
) As output;
--S20.��֤����ST_OffsetCurve���������������һ���ƫ����
SELECT ST_AsText(ST_OffsetCurve( 
ST_GeomFromText(
'LINESTRING(164 16,144 16,124 16,104 
16,84 16,64 16,44 16,24 16,20 16,18 16,17 17,
16 18,16 20,16 40,16 60,16 80,16 
100,16 120,16 140,16 160,16 180,16 195)'),
15, 'quad_segs=4 join=round'));
--S21.��֤����ST_RemoveRepeatedPoints����ɾ���ظ���ļ���
--S22.��֤����ST_SharedPaths���ذ��������������߻����ε�·���ļ���
SELECT ST_AsText(ST_SharedPaths(ST_GeomFromText('MULTILINESTRING((26 125,26 200,126 200,126 125,26 125),(51 150,101 150,76 175,51 150))'),ST_GeomFromText('LINESTRING(151 100,126 156.25,126 125,90 161, 76 175)'))) As wkt;
--S23.��֤����ST_ShiftLongitude��-180��180��0��360��Χ֮���л���������
SELECT ST_AsEWKT(ST_ShiftLongitude(ST_GeomFromEWKT('SRID=4326;POINT(-118.58 38.38 10)'))) As geomA,
ST_AsEWKT(ST_ShiftLongitude(ST_GeomFromEWKT('SRID=4326;POINT(241.42 38.38 10)'))) As geomb;
--S24.��֤����ST_Simplify���ظ������εļ򻯼���
SELECT ST_Npoints(the_geom) As np_before, ST_NPoints(ST_Simplify(the_geom,0.1)) As np01_notbadcircle, ST_NPoints(ST_Simplify(the_geom,0.5)) As np05_notquitecircle,
ST_NPoints(ST_Simplify(the_geom,1)) As np1_octagon, ST_NPoints(ST_Simplify(the_geom,10)) As np10_triangle,
(ST_Simplify(the_geom,100) is null) As np100_geometrygoesaway
FROM (SELECT ST_Buffer('POINT(1 3)', 10,12) As the_geom) As foo;
--S25.��֤����ST_SimplifyPreserveTopology���ظ������εļ򻯼���
SELECT ST_Npoints(the_geom) As np_before, ST_NPoints(ST_SimplifyPreserveTopology(the_geom ,0.1)) As np01_notbadcircle, ST_NPoints(ST_SimplifyPreserveTopology(the_geom,0.5)) As np05_notquitecircle,
ST_NPoints(ST_SimplifyPreserveTopology(the_geom,1)) As np1_octagon, ST_NPoints( ST_SimplifyPreserveTopology(the_geom,10)) As np10_square,
ST_NPoints(ST_SimplifyPreserveTopology(the_geom,100)) As np100_stillsquare
FROM (SELECT ST_Buffer('POINT(1 3)', 10,12) As the_geom) As foo;
--S26.��֤����ST_Split��ּ���
SELECT ST_Split(circle, line)
FROM (SELECT
ST_MakeLine(ST_MakePoint(10, 10),ST_MakePoint(190, 190)) As line,
ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50) As circle) As foo;
--S27.��֤����ST_SymDifference���ر�ʾA��B���ཻ�Ĳ��ּ���ͼ��
SELECT ST_AsText(
ST_SymDifference(
ST_GeomFromText('LINESTRING(50 100, 50 200)'),
ST_GeomFromText('LINESTRING(50 50, 50 150)')
)
);
--S28.��֤����ST_Union���ر�ʾ������ĵ㼯�ϲ��ļ���
SELECT geom_point.id,ST_AsText(ST_Union(st_setsrid(geom_point.the_geom,4326),st_setsrid(geom_line.the_geom,4326))) from geom_point inner join geom_line on geom_point.id=geom_line.id order by id limit 10;
--S29.��֤����ST_UnaryUnion���ر�ʾ������ĵ㼯�ϲ��ļ���
select ST_AsText(ST_UnaryUnion(the_geom)) from geom_point where id=1;




