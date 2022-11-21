set graph_path=test;
SPARQL SELECT ?X	
WHERE
{
  ?X rdf:type <GraduateStudent> .
  ?X <takesCourse> <http://www.Department0.University0.edu/GraduateCourse0> .
};
DROP GRAPH test CASCADE;