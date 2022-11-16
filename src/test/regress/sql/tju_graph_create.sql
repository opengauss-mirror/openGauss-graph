create graph test;
set graph_path=test;
show graph_path;
drop graph test cascade;

create graph test;
set graph_path=test;
CREATE VLABEL person;
CREATE VLABEL friend inherits (person);
CREATE ELABEL knows;
CREATE ELABEL live_together;
CREATE ELABEL room_mate inherits (knows, live_together);
DROP VLABEL friend;
DROP ELABEL knows CASCADE;

CREATE VLABEL person;
CREATE VLABEL friend inherits (person);
CREATE ELABEL knows;
CREATE ELABEL live_together;
CREATE ELABEL room_mate inherits (knows, live_together);
cypher CREATE (:person {name: 'Tom'})-[:knows {fromdate:'2011-11-24'}]->(:person {name: 'Summer'});
cypher CREATE (:person {name: 'Pat'})-[:knows {fromdate:'2013-12-25'}]->(:person {name: 'Nikki'});
cypher CREATE (:person {name: 'Olive'})-[:knows {fromdate:'2015-01-26'}]->(:person {name: 'Todd'});
cypher MATCH (p:Person {name: 'Tom'}),(k:Person{name: 'Pat'}) CREATE (p)-[:KNOWS {fromdate:'2017-02-27'} ]->(k);

cypher MATCH (:person {name: 'Tom'})-[r:knows]->(:person {name: 'Summer'}) SET r.since = '2009-01-08';

