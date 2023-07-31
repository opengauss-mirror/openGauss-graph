#ifndef PG_BUILD_H
#define PG_BUILD_H

#include "postgres_fe.h"
#include "libpq/libpq-fe.h"
#include "access/xlogdefs.h"

#include <locale.h>
#include <signal.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "replication/replicainternal.h"

#define CONFIGRURE_FILE "postgresql.conf"
#define CONFIGRURE_FILE_BAK "postgresql.conf.bak"
#define CONFIG_PARAM1 "replconninfo1"
#define CONFIG_PARAM2 "replconninfo2"
#define CONFIG_CASCADE_STANDBY "cascade_standby"
#define CONFIG_NODENAME "pgxc_node_name"
#define MAX_CONNINFO 8

#define MAX_PARAM_LEN 1024
#define MAX_VALUE_LEN 1024
#define INVALID_LINES_IDX (int)(~0)
#define MAX_CONFIG_FILE_SIZE 0xFFFFF /* max file size for configurations = 1M */
#define MAX_QUERY_LEN 512

extern char ssl_cert_file[];
extern char ssl_key_file[];
extern char ssl_ca_file[];
extern char ssl_crl_file[];
extern char* ssl_cipher_file;
extern char* ssl_rand_file;

extern char pgxcnodename[];
/* global variables for con */
extern char conninfo_global[MAX_REPLNODE_NUM][MAX_VALUE_LEN];
extern int standby_recv_timeout;
extern int standby_connect_timeout; /* 120 sec = default */
extern char gaussdb_state_file[MAXPGPATH];

void delete_datadir(const char* dirname);

int find_gucoption(
    const char** optlines, const char* opt_name, int* name_offset, int* name_len, int* value_offset, int* value_len);

void get_conninfo(const char* filename);

extern PGconn* check_and_conn(int conn_timeout, int recv_timeout, uint32 term = 0);
extern PGconn* check_and_conn_for_standby(int conn_timeout, int recv_timeout, uint32 term = 0);
int GetLengthAndCheckReplConn(const char* ConnInfoList);

extern int replconn_num;
extern int get_replconn_number(const char* filename);
extern bool ParseReplConnInfo(const char* ConnInfoList, int* InfoLength, ReplConnInfo* repl);
char** readfile(const char* path);
extern char* pg_strdup(const char* in);
extern void pg_free(void* ptr);
extern bool GetPaxosValue(const char *filename);
extern void get_slot_name(char* slotname, size_t len);
extern bool libpqRotateCbmFile(PGconn* connObj, XLogRecPtr lsn);
extern int fsync_fname(const char *fname, bool isdir);
extern void fsync_pgdata(const char *pg_data);

#endif /* PG_BUILD_H */
