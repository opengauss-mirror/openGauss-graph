CREATE GLOBAL TEMPORARY TABLE gtt1 (
  ID INTEGER NOT NULL,
  NAME CHAR(16) NOT NULL,
  ADDRESS VARCHAR(50),
  POSTCODE CHAR(6)
)
ON COMMIT PRESERVE ROWS;
select * from pg_get_tabledef('gtt1');