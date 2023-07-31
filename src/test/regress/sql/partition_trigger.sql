DROP TABLE IF EXISTS pt2;
DROP TABLE IF EXISTS pt1;
DROP FUNCTION IF EXISTS test_function();

CREATE TABLE pt1(
                d_date date UNIQUE
)partition BY RANGE (d_date) INTERVAL('1 month')
(PARTITION part1 VALUES LESS THAN ('1900-01-01'));

CREATE TABLE pt2(
                d_date date
)partition BY RANGE (d_date) INTERVAL('1 month')
(PARTITION part1 VALUES LESS THAN ('1900-01-01'));


INSERT INTO pt1 VALUES
('1999-01-01'),('1999-01-02'),('1999-01-019'),('1999-01-20'),
('1999-02-01'),('1999-02-02'),('1999-02-19'),('1999-02-20'),
('1999-03-01'),('1999-03-02'),('1999-03-19'),('1999-03-20'),
('1999-04-01'),('1999-04-02'),('1999-04-19'),('1999-04-20'),
('1999-05-01'),('1999-05-02'),('1999-05-19'),('1999-05-20'),
('1999-06-01'),('1999-06-02'),('1999-06-19'),('1999-06-20'),
('1999-07-01'),('1999-07-02'),('1999-07-19'),('1999-07-20'),
('1999-08-01'),('1999-08-02'),('1999-08-19'),('1999-08-20'),
('1999-09-01'),('1999-09-02'),('1999-09-19'),('1999-09-20'),
('1999-10-01'),('1999-10-02'),('1999-10-19'),('1999-10-20'),
('1999-11-01'),('1999-11-02'),('1999-11-19'),('1999-11-20'),
('1999-12-01'),('1999-12-02'),('1999-12-19'),('1999-12-20');

INSERT INTO pt2 VALUES
('1999-01-01'),('1999-01-02'),('1999-01-019'),('1999-01-20'),
('1999-02-01'),('1999-02-02'),('1999-02-19'),('1999-02-20'),
('1999-03-01'),('1999-03-02'),('1999-03-19'),('1999-03-20'),
('1999-04-01'),('1999-04-02'),('1999-04-19'),('1999-04-20'),
('1999-05-01'),('1999-05-02'),('1999-05-19'),('1999-05-20'),
('1999-06-01'),('1999-06-02'),('1999-06-19'),('1999-06-20'),
('1999-07-01'),('1999-07-02'),('1999-07-19'),('1999-07-20'),
('1999-08-01'),('1999-08-02'),('1999-08-19'),('1999-08-20'),
('1999-09-01'),('1999-09-02'),('1999-09-19'),('1999-09-20'),
('1999-10-01'),('1999-10-02'),('1999-10-19'),('1999-10-20'),
('1999-11-01'),('1999-11-02'),('1999-11-19'),('1999-11-20'),
('1999-12-01'),('1999-12-02'),('1999-12-19'),('1999-12-20');


CREATE FUNCTION test_function()
RETURNS TRIGGER AS $$
BEGIN
  IF(TG_OP = 'DELETE') THEN
    DELETE FROM pt2 WHERE d_date=OLD.d_date;
    RETURN OLD;
  ELSEIF(TG_OP = 'UPDATE') THEN
    UPDATE pt2 set d_date=NEW.d_date where d_date=OLD.d_date;
    RETURN NEW;
  ELSEIF(TG_OP = 'INSERT') THEN
    INSERT INTO pt2 VALUES(NEW.d_date);
    RETURN NEW;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER test_trigger
AFTER INSERT OR UPDATE OR DELETE on pt1
FOR EACH ROW EXECUTE PROCEDURE test_function();

INSERT INTO pt1 VALUES('2016-01-01');
UPDATE pt2 set d_date = d_date + INTERVAL '4' month where d_date>='1999-05-01';
DELETE FROM pt1 WHERE d_date<'1999-05-01';
SELECT * FROM pt1;
SELECT * FROM pt2;

DROP TABLE IF EXISTS pt2;
DROP TABLE IF EXISTS pt1;
DROP FUNCTION IF EXISTS test_function();

CREATE TABLE pt1(
                d_date date UNIQUE
)partition BY RANGE (d_date) INTERVAL('1 month')
(PARTITION part1 VALUES LESS THAN ('1900-01-01'));

CREATE TABLE pt2(
                d_date date
)partition BY RANGE (d_date) INTERVAL('1 month')
(PARTITION part1 VALUES LESS THAN ('1900-01-01'));


INSERT INTO pt1 VALUES
('1999-01-01'),('1999-01-02'),('1999-01-019'),('1999-01-20'),
('1999-02-01'),('1999-02-02'),('1999-02-19'),('1999-02-20'),
('1999-03-01'),('1999-03-02'),('1999-03-19'),('1999-03-20'),
('1999-04-01'),('1999-04-02'),('1999-04-19'),('1999-04-20'),
('1999-05-01'),('1999-05-02'),('1999-05-19'),('1999-05-20'),
('1999-06-01'),('1999-06-02'),('1999-06-19'),('1999-06-20'),
('1999-07-01'),('1999-07-02'),('1999-07-19'),('1999-07-20'),
('1999-08-01'),('1999-08-02'),('1999-08-19'),('1999-08-20'),
('1999-09-01'),('1999-09-02'),('1999-09-19'),('1999-09-20'),
('1999-10-01'),('1999-10-02'),('1999-10-19'),('1999-10-20'),
('1999-11-01'),('1999-11-02'),('1999-11-19'),('1999-11-20'),
('1999-12-01'),('1999-12-02'),('1999-12-19'),('1999-12-20');

INSERT INTO pt2 VALUES
('1999-01-01'),('1999-01-02'),('1999-01-019'),('1999-01-20'),
('1999-02-01'),('1999-02-02'),('1999-02-19'),('1999-02-20'),
('1999-03-01'),('1999-03-02'),('1999-03-19'),('1999-03-20'),
('1999-04-01'),('1999-04-02'),('1999-04-19'),('1999-04-20'),
('1999-05-01'),('1999-05-02'),('1999-05-19'),('1999-05-20'),
('1999-06-01'),('1999-06-02'),('1999-06-19'),('1999-06-20'),
('1999-07-01'),('1999-07-02'),('1999-07-19'),('1999-07-20'),
('1999-08-01'),('1999-08-02'),('1999-08-19'),('1999-08-20'),
('1999-09-01'),('1999-09-02'),('1999-09-19'),('1999-09-20'),
('1999-10-01'),('1999-10-02'),('1999-10-19'),('1999-10-20'),
('1999-11-01'),('1999-11-02'),('1999-11-19'),('1999-11-20'),
('1999-12-01'),('1999-12-02'),('1999-12-19'),('1999-12-20');


CREATE FUNCTION test_function()
RETURNS TRIGGER AS $$
BEGIN
  IF(TG_OP = 'DELETE') THEN
    DELETE FROM pt2 WHERE d_date=OLD.d_date;
    RETURN OLD;
  ELSEIF(TG_OP = 'UPDATE') THEN
    UPDATE pt2 set d_date=NEW.d_date where d_date=OLD.d_date;
    RETURN NEW;
  ELSEIF(TG_OP = 'INSERT') THEN
    INSERT INTO pt2 VALUES(NEW.d_date);
    RETURN NEW;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER test_trigger
BEFORE INSERT OR UPDATE OR DELETE on pt1
FOR EACH ROW EXECUTE PROCEDURE test_function();

INSERT INTO pt1 VALUES('2016-01-01');
UPDATE pt2 set d_date = d_date + INTERVAL '4' month where d_date>='1999-05-01';
DELETE FROM pt1 WHERE d_date<'1999-05-01';
SELECT * FROM pt1;
SELECT * FROM pt2;