\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS MyCMK CASCADE;
CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
DROP TABLE IF EXISTS public.client_customer CASCADE;
CREATE TABLE public.client_customer (
    client_customer_id integer, 
    store_id integer NOT NULL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    email character varying(50),
    address_id integer NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
    activebool boolean DEFAULT true NOT NULL,
    create_date date DEFAULT ('now'::text)::date NOT NULL,
    last_update timestamp without time zone DEFAULT now(),
    active integer
);
DROP TABLE IF EXISTS public.address CASCADE;
CREATE TABLE public.address (
    address_id integer NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
    address character varying(100) NOT NULL,
    address2 character varying(50),
    district character varying(20) NOT NULL,
    city_id integer NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) ,
    postal_code character varying(10),
    phone character varying(20) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);
DROP TABLE IF EXISTS public.city CASCADE;
CREATE TABLE public.city (
    city_id integer NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
    city character varying(50) NOT NULL,
    country_id integer NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
    last_update timestamp without time zone DEFAULT now() NOT NULL
);
DROP TABLE IF EXISTS public.country CASCADE;
CREATE TABLE public.country (
    country_id integer NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
    country character varying(50) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);
INSERT INTO client_customer VALUES (1, 1, 'Mary', 'Smith', 'mary.smith@sakilaclient_customer.org', 5, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (2, 1, 'Patricia', 'Johnson', 'patricia.johnson@sakilaclient_customer.org', 6, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (3, 1, 'Linda', 'Williams', 'linda.williams@sakilaclient_customer.org', 7, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (4, 2, 'Barbara', 'Jones', 'barbara.jones@sakilaclient_customer.org', 8, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (5, 1, 'Elizabeth', 'Brown', 'elizabeth.brown@sakilaclient_customer.org', 9, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (6, 2, 'Jennifer', 'Davis', 'jennifer.davis@sakilaclient_customer.org', 10, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (7, 1, 'Maria', 'Miller', 'maria.miller@sakilaclient_customer.org', 11, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (8, 2, 'Susan', 'Wilson', 'susan.wilson@sakilaclient_customer.org', 12, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (9, 2, 'Margaret', 'Moore', 'margaret.moore@sakilaclient_customer.org', 13, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (10, 1, 'Dorothy', 'Taylor', 'dorothy.taylor@sakilaclient_customer.org', 14, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (11, 2, 'Lisa', 'Anderson', 'lisa.anderson@sakilaclient_customer.org', 15, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (12, 1, 'Nancy', 'Thomas', 'nancy.thomas@sakilaclient_customer.org', 16, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (13, 2, 'Karen', 'Jackson', 'karen.jackson@sakilaclient_customer.org', 17, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (14, 2, 'Betty', 'White', 'betty.white@sakilaclient_customer.org', 18, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (15, 1, 'Helen', 'Harris', 'helen.harris@sakilaclient_customer.org', 19, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (16, 2, 'Sandra', 'Martin', 'sandra.martin@sakilaclient_customer.org', 20, 't', '2006-02-14', '2013-05-26 14:49:45.738', 0);
INSERT INTO client_customer VALUES (17, 1, 'Donna', 'Thompson', 'donna.thompson@sakilaclient_customer.org', 21, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (18, 2, 'Carol', 'Garcia', 'carol.garcia@sakilaclient_customer.org', 22, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (19, 1, 'Gina', 'Williamson', 'gina.williamson@sakilaclient_customer.org', 217, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);
INSERT INTO client_customer VALUES (20, 1, 'Derrick', 'Bourque', 'derrick.bourque@sakilaclient_customer.org', 481, 't', '2006-02-14', '2013-05-26 14:49:45.738', 1);

INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (5,'1913 Hanoi Way', 'Nagasaki', 463, '35200', '28303384290', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (6,'1121 Loja Avenue', 'California', 449, '17886', '838635286649', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (7,'692 Joliet Street', 'Attika', 38, '83579', '448477190408', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (8,'1566 Inegl Manor', 'Mandalay', 349, '53561', '705814003527', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (9,'53 Idfu Parkway', 'Nantou', 361, '42399', '10655648674', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (12,'478 Joliet Way', 'Hamilton', 200, '77948', '657282285970', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (13,'613 Korolev Drive', 'Masqat', 329, '45844', '380657522649', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (14,'1531 Sal Drive', 'Esfahan', 162, '53628', '648856936185', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (15,'1542 Tarlac Parkway', 'Kanagawa', 440, '1027', '635297277345', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (16,'808 Bhopal Manor', 'Haryana', 582, '10672', '465887807014', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (17,'270 Amroha Parkway', 'Osmaniye', 384, '29610', '695479687538', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (18,'770 Bydgoszcz Avenue', 'California', 120, '16266', '517338314235', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (20,'360 Toulouse Parkway', 'England', 495, '54308', '949312333307', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (21,'1001 Miyakonojo Lane', 'Taizz', 518, '67924', '584316724815', '2006-02-15 09:45:30');
INSERT INTO address (address_id, address, district, city_id, postal_code, phone, last_update) VALUES (22,'1153 Allende Way', 'Qubec', 179, '20336', '856872225376', '2006-02-15 09:45:30');

INSERT INTO city (city_id, city, country_id, last_update) VALUES (1,'A Corua (La Corua)', 87, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (2,'Abha', 82, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (3,'Abu Dhabi', 101, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (4,'Acua', 60, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (5,'Adana', 97, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (6,'Addis Abeba', 31, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (7,'Aden', 107, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (8,'Adoni', 44, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (9,'Ahmadnagar', 44, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (10,'Akishima', 50, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (11,'Akron', 103, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (12,'al-Ayn', 101, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (13,'al-Hawiya', 82, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (14,'al-Manama', 11, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (15,'al-Qadarif', 89, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (16,'al-Qatif', 82, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (17,'Alessandria', 49, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (18,'Allappuzha (Alleppey)', 44, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (19,'Allende', 60, '2006-02-15 09:45:25 ');
INSERT INTO city (city_id, city, country_id, last_update) VALUES (20,'Gatineau', 20, '2006-02-15 09:45:25 ');

INSERT INTO country (country_id, country, last_update) VALUES (1,'Afghanistan', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (2,'Algeria', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (3,'American Samoa', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (4,'Angola', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (5,'Anguilla', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (6,'Argentina', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (7,'Armenia', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (8,'Australia', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (9,'Austria', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (10,'Azerbaijan', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (11,'Bahrain', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (12,'Bangladesh', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (13,'Belarus', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (14,'Bolivia', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (15,'Brazil', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (16,'Brunei', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (17,'Bulgaria', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (18,'Cambodia', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (19,'Cameroon', '2006-02-15 09:44:00 ');
INSERT INTO country (country_id, country, last_update) VALUES (20,'Yemen', '2006-02-15 09:44:00 ');

SELECT cu.client_customer_id AS id,
    cu.first_name || ' ' || cu.last_name AS name,
    a.address,
    a.postal_code AS "zip code",
    a.phone,
    city.city,
    country.country,
        CASE
            WHEN cu.activebool THEN 'active'
            ELSE ''
        END AS notes,
    cu.store_id AS sid
   FROM client_customer cu
     INNER JOIN address a USING (address_id)
     INNER JOIN city USING (city_id)
     INNER JOIN country USING (country_id)
     ORDER BY address, city, country, id;
CREATE VIEW client_customer_master AS
  SELECT cu.client_customer_id AS id,
    cu.first_name || ' ' || cu.last_name AS name,
    a.address,
    a.postal_code AS "zip code",
    a.phone,
    city.city,
    country.country,
        CASE
            WHEN cu.activebool THEN 'active'
            ELSE ''
        END AS notes,
    cu.store_id AS sid
   FROM client_customer cu
     INNER JOIN address a USING (address_id)
     INNER JOIN city USING (city_id)
     INNER JOIN country USING (country_id)
     ORDER BY address, city, country, id;
SELECT * FROM client_customer_master ORDER BY address, city, country, id;

SELECT * FROM client_customer_master where country='Canada';

CREATE VIEW test_view AS SELECT city_id, country_id, city from city ORDER BY city;
SELECT * FROM test_view ORDER BY city;

DROP VIEW client_customer_master;
DROP VIEW test_view;
DROP table client_customer cascade;
DROP table address cascade;
DROP table city cascade;
DROP table country cascade;
DROP CLIENT MASTER KEY MyCMK CASCADE;

\! gs_ktool -d all
