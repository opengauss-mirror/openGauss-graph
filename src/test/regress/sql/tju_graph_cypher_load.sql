CREATE graph test;
SET graph_path=test;
CREATE SERVER import_server0 FOREIGN DATA WRAPPER file_fdw;

create foreign table  fdwComment
(
    id int8,
    creationDate varchar(80),
    locationIP varchar(80),
    browserUsed varchar(80),
    content varchar(2000),
    length int4
)
server import_server0
options
(
 FORMAT 'csv',
 HEADER 'false',
 DELIMITER '|',
 NULL '',
 FILENAME '../../comment.csv'
 );
CYPHER LOAD FROM fdwComment AS row 
CREATE (:comment = JSONB_STRIP_NULLS(
  TO_JSONB(ROW_TO_JSON(row))
));
Drop graph test CASCADE;