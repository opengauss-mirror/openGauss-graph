\! gs_ktool -d all
\! gs_ktool -g
\! gs_ktool -g
\! gs_ktool -g

select  count(*), 'count' from gs_client_global_keys;
select  count(*), 'count' from gs_column_keys;

-- case create CMK - success
CREATE CLIENT MASTER KEY ImgCMK WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);

-- fail  duplicate key
CREATE CLIENT MASTER KEY ImgCMK WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/2" , ALGORITHM = AES_256_CBC);

-- fail   didn't support RSA_2048 algorithm
CREATE CLIENT MASTER KEY ImgCMK1 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/2" , ALGORITHM = RSA_2048);

-- fail  ALGORITHM is missing or invalid
CREATE CLIENT MASTER KEY ImgCMK1 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/2");
-- fail  KEY_PATHis missing or invalid
CREATE CLIENT MASTER KEY ImgCMK1 WITH (KEY_STORE = gs_ktool, ALGORITHM = AES_256_CBC);

-- fail  KEY_STORE is missing or invalid
CREATE CLIENT MASTER KEY ImgCMK1 WITH (KEY_PATH = "gs_ktool/2", ALGORITHM = AES_256_CBC);

-- fail duplicate KEY_PATHargs
CREATE CLIENT MASTER KEY ImgCMK1 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/2", KEY_PATH = "gs_ktool/3", ALGORITHM = AES_256_CBC);

-- fail duplicate KEY_STORE args
CREATE CLIENT MASTER KEY ImgCMK1 WITH (KEY_STORE = gs_ktool, KEY_STORE = kmc, KEY_PATH = "gs_ktool/2", ALGORITHM = AES_256_CBC);

-- fail duplicate ALGORITHM args
CREATE CLIENT MASTER KEY ImgCMK1 WITH (KEY_STORE = gs_ktool, KEY_PATH = "gs_ktool/2", ALGORITHM = AES_256_CBC, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);

-- case create CEK - success
CREATE COLUMN ENCRYPTION KEY ImgCEK WITH VALUES (CLIENT_MASTER_KEY = ImgCMK, ALGORITHM  = AEAD_AES_256_CBC_HMAC_SHA256);

CREATE COLUMN ENCRYPTION KEY ImgCEK1 WITH VALUES (CLIENT_MASTER_KEY = ImgCMK, ALGORITHM  = AEAD_AES_256_CBC_HMAC_SHA256, ENCRYPTED_VALUE='abcdefghijklmnopqrstuvwxyz12');

--fail  encryption key too short
CREATE COLUMN ENCRYPTION KEY ImgCEK2 WITH VALUES (CLIENT_MASTER_KEY = ImgCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256, ENCRYPTED_VALUE='abcdefghijklmnopqrstuvwxyz1');

--fail object does not exist
CREATE COLUMN ENCRYPTION KEY ImgCEK2 WITH VALUES (CLIENT_MASTER_KEY = ImgCMK2, ALGORITHM  = AEAD_AES_256_CBC_HMAC_SHA256);

-- fail   duplicate key value
CREATE COLUMN ENCRYPTION KEY ImgCEK WITH VALUES (CLIENT_MASTER_KEY = ImgCMK, ALGORITHM  = AEAD_AES_256_CBC_HMAC_SHA256);

--fail    ImgCMK1 dose not exist
CREATE COLUMN ENCRYPTION KEY ImgCEK2 WITH VALUES (CLIENT_MASTER_KEY = ImgCMK1, ALGORITHM  = AEAD_AES_256_CBC_HMAC_SHA256);

-- fail  didn't support AES_128_CBC algorithm
CREATE COLUMN ENCRYPTION KEY ImgCEK2 WITH VALUES (CLIENT_MASTER_KEY = ImgCMK, ALGORITHM  = AES_128_CBC);

-- fail   syntax error parsing cek creation query
CREATE COLUMN ENCRYPTION KEY ImgCEK2 WITH VALUES (CLIENT_MASTER_KEY = ImgCMK);

-- fail   syntax error parsing cek creation query
CREATE COLUMN ENCRYPTION KEY ImgCEK2 WITH VALUES (ALGORITHM = AEAD_AES_128_CBC_HMAC_SHA256);

select  count(*), 'count' from gs_client_global_keys;
select  count(*), 'count' from gs_column_keys;

--cek dose not exist
CREATE TABLE account(user_id INT, username VARCHAR (50)  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ImgCEK3, ENCRYPTION_TYPE = DETERMINISTIC)
);
DROP CLIENT MASTER KEY ImgCMK CASCADE;

\! gs_ktool -d all