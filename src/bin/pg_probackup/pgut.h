/*-------------------------------------------------------------------------
 *
 * pgut.h
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2017-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGUT_H
#define PGUT_H

#include "postgres_fe.h"
#include "libpq/libpq-fe.h"

typedef void (*pgut_atexit_callback)(bool fatal, void *userdata);

extern void pgut_help(bool details);

/*
 * pgut framework variables and functions
 */
extern bool                      prompt_password;

extern bool                      interrupted;
extern bool                     in_cleanup;
extern bool                     in_password;	/* User prompts password */

extern void pgut_atexit_push(pgut_atexit_callback callback, void *userdata);
extern void pgut_atexit_pop(pgut_atexit_callback callback, void *userdata);

extern void pgut_init(void);
extern void on_cleanup(void);
/*
 * Database connections
 */
extern PGconn *pgut_connect(const char *host, const char *port,
                                                    const char *dbname, const char *username);
extern PGconn *pgut_connect_replication(const char *host, const char *port,
                                                                        const char *dbname,
                                                                        const char *username);
extern void pgut_disconnect(PGconn *conn);
extern void pgut_disconnect_callback(bool fatal, void *userdata);
extern PGresult *pgut_execute(PGconn* conn, const char *query, int nParams,
                                                    const char **params);
extern PGresult *pgut_execute_extended(PGconn* conn, const char *query, int nParams,
                                                                        const char **params, bool text_result, bool ok_error);
extern PGresult *pgut_execute_parallel(PGconn* conn, PGcancel* thread_cancel_conn,
                                                                    const char *query, int nParams,
                                                                    const char **params, bool text_result, bool ok_error, bool async);
extern bool pgut_send(PGconn* conn, const char *query, int nParams, const char **params, int elevel);
extern void pgut_cancel(PGconn* conn);
extern int pgut_wait(int num, PGconn *connections[], struct timeval *timeout);

/*
 * memory allocators
 */
extern void *pgut_malloc(size_t size);
extern void *pgut_realloc(void *p, size_t oldSize, size_t size);
extern char *pgut_strdup(const char *str);

#define pgut_new(type)                  ((type *) pgut_malloc(sizeof(type)))
#define pgut_newarray(type, n)      ((type *) pgut_malloc(sizeof(type) * (n)))

/*
 * file operations
 */
extern FILE *pgut_fopen(const char *path, const char *mode, bool missing_ok);

/*
 * Assert
 */
#undef Assert
#undef AssertArg
#undef AssertMacro

#ifdef USE_ASSERT_CHECKING
#define Assert(x)                   assert(x)
#define AssertArg(x)             assert(x)
#define AssertMacro(x)          assert(x)
#else
#define Assert(x)                   ((void) 0)
#define AssertArg(x)            ((void) 0)
#define AssertMacro(x)      ((void) 0)
#endif

#define IsSpace(c)              (isspace((unsigned char)(c)))
#define IsAlpha(c)              (isalpha((unsigned char)(c)))
#define IsAlnum(c)          (isalnum((unsigned char)(c)))
#define ToLower(c)          (tolower((unsigned char)(c)))
#define ToUpper(c)          (toupper((unsigned char)(c)))

/*
 * socket operations
 */
extern int wait_for_socket(int sock, struct timeval *timeout);
extern int wait_for_sockets(int nfds, fd_set *fds, struct timeval *timeout);

#ifdef WIN32
extern int sleep(unsigned int seconds);
extern int usleep(unsigned int usec);
#endif

#endif   /* PGUT_H */
