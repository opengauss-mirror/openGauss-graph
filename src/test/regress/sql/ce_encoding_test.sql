\! gs_ktool -d all
\! gs_ktool -g

CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE 信息 (
  ID bigint NOT NULL ,
  姓名 varchar(255) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) ,
  年龄 int NOT NULL ,
  地址 varchar(255) DEFAULT NULL ,
  性别 int DEFAULT NULL ,
  民族 varchar(10) DEFAULT NULL
  ) ;
INSERT INTO 信息 (ID, 姓名, 年龄, 地址, 性别, 民族) VALUES (1, '王仲刚', 31, '中国四川成都', 1, '汉族');
SELECT * FROM 信息;
DROP TABLE 信息;
DROP CLIENT MASTER KEY mycmk CASCADE;

\! gs_ktool -d all