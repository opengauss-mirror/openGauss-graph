CREATE graph test;
SET graph_path=test;
CREATE SERVER import_server FOREIGN DATA WRAPPER file_fdw;

create foreign table lubm_profile_regress
(
  	subject Text,
  	predicate Text,
    object Text,
    dot    char
)
server import_server
options
(
 FORMAT 'csv',
 HEADER 'false',     
 DELIMITER ' ',
 NULL '',
 FILENAME '../../lubm1.nt'
 );
SPARQL LOAD lubm_profile_regress;
