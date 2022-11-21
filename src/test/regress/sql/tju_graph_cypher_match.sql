CREATE graph test;
SET graph_path=test;
CREATE VLABEL person;
CREATE VLABEL friend inherits (person);
CREATE ELABEL knows;
CREATE ELABEL live_together;
CREATE ELABEL room_mate inherits (knows, live_together);
CREATE PROPERTY INDEX ON person (name);

CYPHER CREATE (:person {name: 'Tom'})-[:knows {fromdate:'2011-11-24'}]->(:person {name: 'Summer'});
CYPHER CREATE (:person {name: 'Pat'})-[:knows {fromdate:'2013-12-25'}]->(:person {name: 'Nikki'});
CYPHER CREATE (:person {name: 'Olive'})-[:knows {fromdate:'2015-01-26'}]->(:person {name: 'Todd'});
CYPHER MATCH (p:Person {name: 'Tom'}),(k:Person{name: 'Pat'}) 
CREATE (p)-[:KNOWS {fromdate:'2017-02-27'} ]->(k);
CYPHER MATCH (a:person {name: 'Olive'})-[:knows {fromdate:'2015-01-26'}]->(k:person)
RETURN k;

CYPHER MATCH (:person {name: 'Tom'})-[r:knows]->(:person {name: 'Summer'})
SET r.since = '2009-01-08';
CYPHER MATCH (a:person {name: 'Tom'})-[r:knows]->(b:person {name: 'Summer'})
RETURN a,r,b;

CYPHER CREATE (:person {name: 'Jack'})-[:knows {fromdate:'2015-01-26'}]->(:person {name: 'Tom'});                         
CYPHER MATCH (m:person {name: 'Jack'})-[l:knows]->(b:person {name: 'Tom'}) DELETE l;
CYPHER MATCH (m:person {name: 'Jack'})-[l:knows]->(b:person {name: 'Tom'}) RETURN l;
CYPHER MATCH (m:person {name: 'Tom'})-[l:knows]->(b:person {name: 'Summer'}) DELETE l;
CYPHER MATCH (m:person {name: 'Tom'})-[l:knows]->(b:person {name: 'Pat'}) DELETE l;
CYPHER MATCH (m:person {name: 'Tom'}) DELETE m;
CYPHER MATCH (m:person {name: 'Tom'}) RETURN m;

DROP GRAPH test CASCADE;