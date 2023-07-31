/*-------------------------------------------------------------------------
 *
 * fetch.c
 *    Functions for fetching files from openGauss data directory
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include <sys/stat.h>
#include <unistd.h>
#include "common/fe_memutils.h"

/*
 * Read a file into memory. The file to be read is <datadir>/<path>.
 * The file contents are returned in a malloc'd buffer, and *filesize
 * is set to the length of the file.
 *
 * The returned buffer is always zero-terminated; the size of the returned
 * buffer is actually *filesize + 1. That's handy when reading a text file.
 * This function can be used to read binary files as well, you can just
 * ignore the zero-terminator in that case.
 *
 */
char *
slurpFile(const char *datadir, const char *path, size_t *filesize, bool safe, fio_location location)
{
    int         fd;
    char       *buffer;
    struct stat statbuf;
    char        fullpath[MAXPGPATH];
    int         len;

    join_path_components(fullpath, datadir, path);

    if ((fd = fio_open(fullpath, O_RDONLY | PG_BINARY, location)) == -1)
    {
        if (safe)
            return NULL;
        else
            elog(ERROR, "Could not open file \"%s\" for reading: %s",
                    fullpath, strerror(errno));
    }

    if (fio_stat(fullpath, &statbuf, true, location) < 0)
    {
        if (safe)
            return NULL;
        else
            elog(ERROR, "Could not stat file \"%s\": %s",
                fullpath, strerror(errno));
    }

    len = statbuf.st_size;
    buffer = (char *)pg_malloc(len + 1);

    if (fio_read(fd, buffer, len) != len)
    {
        if (safe) {
            pg_free(buffer);
            return NULL;
        } else
            elog(ERROR, "Could not read file \"%s\": %s\n",
                fullpath, strerror(errno));
    }

    fio_close(fd);

    /* Zero-terminate the buffer. */
    buffer[len] = '\0';

    if (filesize)
        *filesize = len;
    return buffer;
}

/*
 * Receive a single file as a malloc'd buffer.
 */
char *
fetchFile(PGconn *conn, const char *filename, size_t *filesize)
{
    PGresult   *res;
    char       *result;
    const char *params[1];
    int            len;
    errno_t rc = 0;

    params[0] = filename;
    res = pgut_execute_extended(conn, "SELECT pg_catalog.pg_read_binary_file($1)",
                                1, params, false, false);

    /* sanity check the result set */
    if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
        elog(ERROR, "unexpected result set while fetching remote file \"%s\"",
             filename);

    /* Read result to local variables */
    len = PQgetlength(res, 0, 0);
    result = (char *)pg_malloc(len + 1);
    rc = memcpy_s(result, len + 1,PQgetvalue(res, 0, 0), len);
    securec_check_c(rc, "\0", "\0");
    result[len] = '\0';

    PQclear(res);
    *filesize = len;

    return result;
}
