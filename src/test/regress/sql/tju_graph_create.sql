CREATE graph test;
SET graph_path=test;
SHOW graph_path;
DROP graph test cascade;

CREATE graph test;
SET graph_path=test;
CREATE VLABEL person;
CREATE VLABEL friend inherits (person);
CREATE ELABEL knows;
CREATE ELABEL live_together;
CREATE ELABEL room_mate inherits (knows, live_together);
DROP VLABEL friend;
DROP ELABEL knows CASCADE;
DROP graph test cascade;