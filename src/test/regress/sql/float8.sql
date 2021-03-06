
--
-- FLOAT8
--

CREATE TABLE FLOAT8_TBL(f1 float8);

INSERT INTO FLOAT8_TBL(f1) VALUES ('    0.0   ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1004.30  ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('   -34.84');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e+200');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e-200');

-- test for underflow and overflow handling
SELECT '10e400'::float8;
SELECT '-10e400'::float8;
SELECT '10e-400'::float8;
SELECT '-10e-400'::float8;

-- bad input
INSERT INTO FLOAT8_TBL(f1) VALUES ('');
INSERT INTO FLOAT8_TBL(f1) VALUES ('     ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('xyz');
INSERT INTO FLOAT8_TBL(f1) VALUES ('5.0.0');
INSERT INTO FLOAT8_TBL(f1) VALUES ('5 . 0');
INSERT INTO FLOAT8_TBL(f1) VALUES ('5.   0');
INSERT INTO FLOAT8_TBL(f1) VALUES ('    - 3');
INSERT INTO FLOAT8_TBL(f1) VALUES ('123           5');

-- special inputs
SELECT 'NaN'::float8;
SELECT 'nan'::float8;
SELECT '   NAN  '::float8;
SELECT 'infinity'::float8;
SELECT '          -INFINiTY   '::float8;
-- bad special inputs
SELECT 'N A N'::float8;
SELECT 'NaN x'::float8;
SELECT ' INFINITY    x'::float8;

SELECT 'Infinity'::float8 + 100.0;
SELECT 'Infinity'::float8 / 'Infinity'::float8;
SELECT 'nan'::float8 / 'nan'::float8;
SELECT 'nan'::numeric::float8;

SELECT '' AS five, * FROM FLOAT8_TBL ORDER BY f1;

SELECT '' AS four, f.* FROM FLOAT8_TBL f WHERE f.f1 <> '1004.3' ORDER BY f1;

SELECT '' AS one, f.* FROM FLOAT8_TBL f WHERE f.f1 = '1004.3';

SELECT '' AS three, f.* FROM FLOAT8_TBL f WHERE '1004.3' > f.f1 ORDER BY f1;

SELECT '' AS three, f.* FROM FLOAT8_TBL f WHERE  f.f1 < '1004.3' ORDER BY f1;

SELECT '' AS four, f.* FROM FLOAT8_TBL f WHERE '1004.3' >= f.f1 ORDER BY f1;

SELECT '' AS four, f.* FROM FLOAT8_TBL f WHERE  f.f1 <= '1004.3' ORDER BY f1;

SELECT '' AS three, f.f1, f.f1 * '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0' ORDER BY f1;

SELECT '' AS three, f.f1, f.f1 + '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0' ORDER BY f1;

SELECT '' AS three, f.f1, f.f1 / '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0' ORDER BY f1;

SELECT '' AS three, f.f1, f.f1 - '-10' AS x
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0' ORDER BY f1;

SELECT '' AS one, f.f1 ^ '2.0' AS square_f1
   FROM FLOAT8_TBL f where f.f1 = '1004.3';

-- absolute value 
SELECT '' AS five, f.f1, @f.f1 AS abs_f1 
   FROM FLOAT8_TBL f ORDER BY f1;

-- truncate
SELECT '' AS five, f.f1, trunc(f.f1) AS trunc_f1
   FROM FLOAT8_TBL f ORDER BY f1;

-- round
SELECT '' AS five, f.f1, round(f.f1) AS round_f1
   FROM FLOAT8_TBL f ORDER BY f1;

-- ceil / ceiling
select ceil(f1) as ceil_f1 from float8_tbl f ORDER BY f1;
select ceiling(f1) as ceiling_f1 from float8_tbl f ORDER BY f1;

-- floor
select floor(f1) as floor_f1 from float8_tbl f ORDER BY f1;

-- sign
select sign(f1) as sign_f1 from float8_tbl f ORDER BY f1;

-- square root
SELECT sqrt(float8 '64') AS eight;

SELECT |/ float8 '64' AS eight;

SELECT '' AS three, f.f1, |/f.f1 AS sqrt_f1
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0' ORDER BY f1;

-- power
SELECT power(float8 '144', float8 '0.5');

-- take exp of ln(f.f1)
SELECT '' AS three, f.f1, exp(ln(f.f1)) AS exp_ln_f1
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0' ORDER BY f1;

-- cube root
SELECT ||/ float8 '27' AS three;

SELECT '' AS five, f.f1, ||/f.f1 AS cbrt_f1 FROM FLOAT8_TBL f ORDER BY f1;


SELECT '' AS five, * FROM FLOAT8_TBL ORDER BY f1;

UPDATE FLOAT8_TBL
   SET f1 = FLOAT8_TBL.f1 * '-1'
   WHERE FLOAT8_TBL.f1 > '0.0';
   
SELECT '' AS bad, f.f1 * '1e200' from FLOAT8_TBL f ORDER BY f1;

SELECT '' AS bad, f.f1 ^ '1e200' from FLOAT8_TBL f ORDER BY f1;

SELECT 0 ^ 0 + 0 ^ 1 + 0 ^ 0.0 + 0 ^ 0.5;

SELECT '' AS bad, ln(f.f1) from FLOAT8_TBL f where f.f1 = '0.0' ;

SELECT '' AS bad, ln(f.f1) from FLOAT8_TBL f where f.f1 < '0.0';

SELECT '' AS bad, exp(f.f1) from FLOAT8_TBL f ORDER BY f1;

SELECT '' AS bad, f.f1 / '0.0' from FLOAT8_TBL f;

SELECT '' AS five, * FROM FLOAT8_TBL ORDER BY f1;

-- test for over- and underflow
INSERT INTO FLOAT8_TBL(f1) VALUES ('10e400');

INSERT INTO FLOAT8_TBL(f1) VALUES ('-10e400');

INSERT INTO FLOAT8_TBL(f1) VALUES ('10e-400');

INSERT INTO FLOAT8_TBL(f1) VALUES ('-10e-400');

-- maintain external table consistency across platforms
-- delete all values and reinsert well-behaved ones

DELETE FROM FLOAT8_TBL;

INSERT INTO FLOAT8_TBL(f1) VALUES ('0.0');

INSERT INTO FLOAT8_TBL(f1) VALUES ('-34.84');

INSERT INTO FLOAT8_TBL(f1) VALUES ('-1004.30');

INSERT INTO FLOAT8_TBL(f1) VALUES ('-1.2345678901234e+200');

INSERT INTO FLOAT8_TBL(f1) VALUES ('-1.2345678901234e-200');

SELECT '' AS five, * FROM FLOAT8_TBL ORDER BY f1;

-- test edge-case coercions to integer
SELECT '32767.4'::float8::int2;
SELECT '32767.6'::float8::int2;
SELECT '-32768.4'::float8::int2;
SELECT '-32768.6'::float8::int2;
SELECT '2147483647.4'::float8::int4;
SELECT '2147483647.6'::float8::int4;
SELECT '-2147483648.4'::float8::int4;
SELECT '-2147483648.6'::float8::int4;
SELECT '9223372036854773760'::float8::int8;
SELECT '9223372036854775807'::float8::int8;
SELECT '-9223372036854775808.5'::float8::int8;
SELECT '-9223372036854780000'::float8::int8;

SELECT sin('1');
SELECT cos('1');
SELECT tan('1');
-- test Inf/NaN cases for functions
SELECT sin('infinity');
SELECT sin('-infinity');
SELECT sin('nan');
SELECT cos('infinity');
SELECT cos('-infinity');
SELECT cos('nan');
SELECT tan('infinity');
SELECT tan('-infinity');
SELECT tan('nan');

--test power
SELECT power('0', '0');
SELECT power('0', '1');
SELECT power('0', '2');
SELECT power('0', 'infinity');
SELECT power('0', '-infinity');
SELECT power('0', 'nan');

SELECT power('5', '0');
SELECT power('5', '1');
SELECT power('5', '2');
SELECT power('5', 'infinity');
SELECT power('5', '-infinity');
SELECT power('5', 'nan');

SELECT power('infinity', '0');
SELECT power('infinity', '1');
SELECT power('infinity', '2');
SELECT power('infinity', 'infinity');
SELECT power('infinity', '-infinity');
SELECT power('infinity', 'nan');

SELECT power('-infinity', '0');
SELECT power('-infinity', '1');
SELECT power('-infinity', '2');
SELECT power('-infinity', 'infinity');
SELECT power('-infinity', '-infinity');
SELECT power('-infinity', 'nan');

SELECT power('nan', '0');
SELECT power('nan', '1');
SELECT power('nan', '2');
SELECT power('nan', 'infinity');
SELECT power('nan', '-infinity');
SELECT power('nan', 'nan');