/* -------------------------------------------------------------------------
 *
 * exec.cpp
 *		Functions for finding and validating executable files
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/port/exec.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#include "knl/knl_variable.h"
#else
#include "postgres_fe.h"
#endif

#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "mb/pg_wchar.h"

#ifndef FRONTEND
/* We use only 3-parameter elog calls in this file, for simplicity */
/* NOTE: caller must provide gettext call around str! */
#define log_error(str, param) ereport(LOG, (errmsg(str, param)))
#else
#define log_error(str, param) (fprintf(stderr, str, param), fputc('\n', stderr))
#endif

#ifdef WIN32_ONLY_COMPILER
#define getcwd(cwd, len) GetCurrentDirectory(len, cwd)
#endif

static int validate_exec(const char* path);
static int resolve_symlinks(char* path);
static char* pipe_read_line(const char* cmd, char* line, int maxsize);

#ifdef WIN32
static BOOL GetTokenUser(HANDLE hToken, PTOKEN_USER* ppTokenUser);
extern BOOL WINAPI AddAccessAllowedAceEx(PACL pAcl, DWORD dwAceRevision, DWORD AceFlags, DWORD AccessMask, PSID pSid);
#endif

/*
 *  * @Description: check the value from environment variablethe to prevent command injection.
 *   * @in input_env_value : the input value need be checked.
 *    */
static bool check_env(const char* input_env_value)
{
#define MAXENVLEN 1024

    const char* danger_character_list[] = {";", "`", "\\", "'", "\"", ">", "<", "$", "&", "|", "!", "\n", NULL};
    int i = 0;

    if (input_env_value == NULL || strlen(input_env_value) >= MAXENVLEN) {
        log_error(_("wrong environment variable \"%s\""), input_env_value);
        return false;
    }

    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr((const char*)input_env_value, danger_character_list[i])) {
            log_error(_("Error: environment variable \"%s\" contain invaild symbol\n"),
                input_env_value);
            return false;
        }
    }

    return true;
}

/*
 * validate_exec -- validate "path" as an executable file
 *
 * returns 0 if the file is found and no error is encountered.
 *		  -1 if the regular file "path" does not exist or cannot be executed.
 *		  -2 if the file is otherwise valid but cannot be read.
 */
static int validate_exec(const char* path)
{
    struct stat buf;
    int is_r;
    int is_x;

#ifdef WIN32
    char path_exe[MAXPGPATH + sizeof(".exe") - 1];

    /* Win32 requires a .exe suffix for stat() */
    if (strlen(path) >= strlen(".exe") && pg_strcasecmp(path + strlen(path) - strlen(".exe"), ".exe") != 0) {
        errno_t rc = strncpy_s(path_exe, MAXPGPATH, path, MAXPGPATH - 1);
        securec_check_c(rc, "\0", "\0");
        rc = strcat_s(path_exe, MAXPGPATH + sizeof(".exe") - 1, ".exe");
        securec_check_c(rc, "\0", "\0");
        path = path_exe;
    }
#endif

    /*
     * Ensure that the file exists and is a regular file.
     *
     * XXX if you have a broken system where stat() looks at the symlink
     * instead of the underlying file, you lose.
     */
    if (stat(path, &buf) < 0) {
        return -1;
    }

    if (!S_ISREG(buf.st_mode)) {
        return -1;
    }
    /*
     * Ensure that the file is both executable and readable (required for
     * dynamic loading).
     */
#ifndef WIN32
    is_r = (access(path, R_OK) == 0);
    is_x = (access(path, X_OK) == 0);
#else
    is_r = buf.st_mode & S_IRUSR;
    is_x = buf.st_mode & S_IXUSR;
#endif
    return is_x ? (is_r ? 0 : -2) : -1;
}

/*
 * find_my_exec -- find an absolute path to a valid executable
 *
 *	argv0 is the name passed on the command line
 *	retpath is the output area (must be of size MAXPGPATH)
 *	Returns 0 if OK, -1 if error.
 *
 * The reason we have to work so hard to find an absolute path is that
 * on some platforms we can't do dynamic loading unless we know the
 * executable's location.  Also, we need a full path not a relative
 * path because we will later change working directory.  Finally, we want
 * a true path not a symlink location, so that we can locate other files
 * that are part of our installation relative to the executable.
 */
int find_my_exec(const char* argv0, char* retpath)
{
    char cwd[MAXPGPATH], test_path[MAXPGPATH];
    char* path = NULL;
    errno_t rc;

    if (getcwd(cwd, MAXPGPATH) == NULL) {
        log_error(_("could not identify current directory: %s"), gs_strerror(errno));
        return -1;
    }

    /*
     * If argv0 contains a separator, then PATH wasn't used.
     */
    if (first_dir_separator(argv0) != NULL) {
        if (is_absolute_path(argv0)) {
            rc = strncpy_s(retpath, MAXPGPATH, argv0, MAXPGPATH - 1);
            securec_check_c(rc, "\0", "\0");
        } else {
            join_path_components(retpath, cwd, argv0);
        }
        canonicalize_path(retpath);

        if (validate_exec(retpath) == 0) {
            return resolve_symlinks(retpath);
        }
        log_error(_("invalid binary \"%s\""), retpath);
        return -1;
    }

#ifdef WIN32
    /* Win32 checks the current directory first for names without slashes */
    join_path_components(retpath, cwd, argv0);
    if (validate_exec(retpath) == 0) {
        return resolve_symlinks(retpath);
    }
#endif

    /*
     * Since no explicit path was supplied, the user must have been relying on
     * PATH.  We'll search the same PATH.
     */
    if (((path = gs_getenv_r("PATH")) != NULL) && *path) {
        char* startp = NULL;
        char* endp = NULL;

        do {
            if (startp == NULL) {
                startp = path;
            } else {
                startp = endp + 1;
            }

            endp = first_path_var_separator(startp);
            if (endp == NULL) {
                endp = startp + strlen(startp); /* point to end */
            }

            rc = strncpy_s(test_path, MAXPGPATH, startp, Min(endp - startp + 1, MAXPGPATH) - 1);
            securec_check_c(rc, "\0", "\0");

            if (!check_env(test_path)) {
                break;
            }

            if (is_absolute_path(test_path)) {
                join_path_components(retpath, test_path, argv0);
            } else {
                join_path_components(retpath, cwd, test_path);
                join_path_components(retpath, retpath, argv0);
            }
            canonicalize_path(retpath);

            switch (validate_exec(retpath)) {
                case 0: /* found ok */
                    return resolve_symlinks(retpath);
                case -1: /* wasn't even a candidate, keep looking */
                    break;
                case -2: /* found but disqualified */
                    log_error(_("could not read binary \"%s\""), retpath);
                    break;
                default:
                    break;
            }
        } while (*endp);
    }

    log_error(_("could not find a \"%s\" to execute"), argv0);
    return -1;
}

/*
 * resolve_symlinks - resolve symlinks to the underlying file
 *
 * Replace "path" by the absolute path to the referenced file.
 *
 * Returns 0 if OK, -1 if error.
 *
 * Note: we are not particularly tense about producing nice error messages
 * because we are not really expecting error here; we just determined that
 * the symlink does point to a valid executable.
 */
static int resolve_symlinks(char* path)
{
#ifdef HAVE_READLINK
    struct stat buf;
    char tmp_path[MAXPGPATH] = {0};
    char link_buf[MAXPGPATH] = {0};
    int len = 0;
    int rc = 0;

    /* openGauss is process based, and our MPPDB is thread based.
     * And chdir() will affect all the threads within the process.
     * So we have to avoid calling chdir() after main thread is working
     * well in normal state.
     *
     * So we rewrite a new version resolve_symlinks() without calling
     * chdir(). its function is tested well under SUSE and redhat system.
     * Note: maybe some other platform/system fails to work.
     */

    if (is_absolute_path(path)) {
        len = strlen(path);
        rc = memcpy_s(tmp_path, MAXPGPATH, path, len + 1);
        securec_check_c(rc, "\0", "\0");
    } else { /* relative path */
        if (getcwd(tmp_path, MAXPGPATH) == NULL) {
            log_error(_("could not identify current directory: %s"), gs_strerror(errno));
            return -1;
        }
        join_path_components(tmp_path, tmp_path, path);
        canonicalize_path(tmp_path);
    }

    for (;;) {
        /* make sure that tmp_path is an absolute path */
        if (lstat(tmp_path, &buf) < 0 || !S_ISLNK(buf.st_mode)) {
            break;
        }

        /* read the real file which this link file points to */
        len = readlink(tmp_path, link_buf, sizeof(link_buf));
        if (len < 0 || len >= (int)sizeof(link_buf)) {
            log_error(_("could not read symbolic link \"%s\""), tmp_path);
            return -1;
        }
        link_buf[len] = '\0';

        if (is_absolute_path(link_buf)) {
            /* copy this absolute path directly to tmp_path */
            rc = memcpy_s(tmp_path, MAXPGPATH, link_buf, len + 1);
            securec_check_c(rc, "\0", "\0");
        } else { /* relative path */
            /* find its directory holding current link file */
            char* lsep = last_dir_separator(tmp_path);
            *lsep = '\0';
            /* find this absolute path for real file */
            join_path_components(tmp_path, tmp_path, link_buf);
            canonicalize_path(tmp_path);
        }
    }

    len = strlen(tmp_path);
    rc = memcpy_s(path, MAXPGPATH, tmp_path, len + 1);
    securec_check_c(rc, "\0", "\0");
#endif

    return 0;
}

/*
 * Find another program in our binary's directory,
 * then make sure it is the proper version.
 */
int find_other_exec(const char* argv0, const char* target, const char* versionstr, char* retpath)
{
    char cmd[MAXPGPATH];
    char line[150];

    if (find_my_exec(argv0, retpath) < 0) {
        return -1;
    }

    /* Trim off program name and keep just directory */
    *last_dir_separator(retpath) = '\0';
    canonicalize_path(retpath);

    /* Now append the other program's name */
    int sret = snprintf_s(
        retpath + strlen(retpath), MAXPGPATH - strlen(retpath), MAXPGPATH - strlen(retpath) - 1, "/%s%s", target, EXE);
    securec_check_ss_c(sret, "\0", "\0");

    if (validate_exec(retpath) != 0) {
        return -1;
    }

    int rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" -V", retpath);
    securec_check_ss_c(rc, "\0", "\0");

    if (pipe_read_line(cmd, line, sizeof(line)) == NULL) {
        return -1;
    }
#if (defined ENABLE_MULTIPLE_NODES) && (!defined ENABLE_LLT)
    if (strcmp(line, versionstr) != 0) {
        return -2;
    }
#endif
    return 0;
}

/*
 * The runtime library's popen() on win32 does not work when being
 * called from a service when running on windows <= 2000, because
 * there is no stdin/stdout/stderr.
 *
 * Executing a command in a pipe and reading the first line from it
 * is all we need.
 */
static char* pipe_read_line(const char* cmd, char* line, int maxsize)
{
#ifndef WIN32
    FILE* pgver = NULL;

    /* flush output buffers in case popen does not... */
    fflush(stdout);
    fflush(stderr);

    errno = 0;
    if ((pgver = popen(cmd, "r")) == NULL) {
        perror("popen failure");
        return NULL;
    }

    errno = 0;
    if (fgets(line, maxsize, pgver) == NULL) {
        if (feof(pgver)) {
            fprintf(stderr, "no data was returned by command \"%s\"\n", cmd);
        } else {
            perror("fgets failure");
        }
        pclose(pgver); /* no error checking */
        return NULL;
    }

    if (pclose_check(pgver)) {
        return NULL;
    }

    return line;
#else  /* WIN32 */

    SECURITY_ATTRIBUTES sattr;
    HANDLE childstdoutrd = NULL;
    HANDLE childstdoutwr = NULL;
    HANDLE childstdoutrddup = NULL;
    PROCESS_INFORMATION pi;
    STARTUPINFO si;
    char* retval = NULL;

    sattr.nLength = sizeof(SECURITY_ATTRIBUTES);
    sattr.bInheritHandle = TRUE;
    sattr.lpSecurityDescriptor = NULL;

    if (!CreatePipe(&childstdoutrd, &childstdoutwr, &sattr, 0)) {
        return NULL;
    }

    if (!DuplicateHandle(GetCurrentProcess(),
            childstdoutrd,
            GetCurrentProcess(),
            &childstdoutrddup,
            0,
            FALSE,
            DUPLICATE_SAME_ACCESS)) {
        CloseHandle(childstdoutrd);
        CloseHandle(childstdoutwr);
        return NULL;
    }

    CloseHandle(childstdoutrd);

    ZeroMemory(&pi, sizeof(pi));
    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);
    si.dwFlags = STARTF_USESTDHANDLES;
    si.hStdError = childstdoutwr;
    si.hStdOutput = childstdoutwr;
    si.hStdInput = INVALID_HANDLE_VALUE;

    if (CreateProcess(NULL, (LPSTR)cmd, NULL, NULL, TRUE, 0, NULL, NULL, &si, &pi)) {
        /* Successfully started the process */
        char* lineptr = NULL;

        ZeroMemory(line, maxsize);

        /* Try to read at least one line from the pipe */
        /* This may require more than one wait/read attempt */
        for (lineptr = line; lineptr < line + maxsize - 1;) {
            DWORD bytesread = 0;

            /* Let's see if we can read */
            if (WaitForSingleObject(childstdoutrddup, 10000) != WAIT_OBJECT_0) {
                break; /* Timeout, but perhaps we got a line already */
            }

            if (!ReadFile(childstdoutrddup, lineptr, maxsize - (lineptr - line), &bytesread, NULL)) {
                break; /* Error, but perhaps we got a line already */
            }

            lineptr += strlen(lineptr);

            if (!bytesread) {
                break; /* EOF */
            }

            if (strchr(line, '\n')) {
                break; /* One or more lines read */
            }
        }

        if (lineptr != line) {
            /* OK, we read some data */
            int len;

            /* If we got more than one line, cut off after the first \n */
            lineptr = strchr(line, '\n');
            if (lineptr != NULL) {
                *(lineptr + 1) = '\0';
            }
            len = strlen(line);

            /*
             * If EOL is \r\n, convert to just \n. Because stdout is a
             * text-mode stream, the \n output by the child process is
             * received as \r\n, so we convert it to \n.  The server main.c
             * sets setvbuf(stdout, NULL, _IONBF, 0) which has the effect of
             * disabling \n to \r\n expansion for stdout.
             */
            if (len >= 2 && line[len - 2] == '\r' && line[len - 1] == '\n') {
                line[len - 2] = '\n';
                line[len - 1] = '\0';
                len--;
            }

            /*
             * We emulate fgets() behaviour. So if there is no newline at the
             * end, we add one...
             */
            if (len == 0 || line[len - 1] != '\n') {
                errno_t rc = strcat_s(line, len, "\n");
                securec_check_c(rc, "\0", "\0");
            }
            retval = line;
        }

        CloseHandle(pi.hProcess);
        CloseHandle(pi.hThread);
    }

    CloseHandle(childstdoutwr);
    CloseHandle(childstdoutrddup);

    return retval;
#endif /* WIN32 */
}

/*
 * pclose() plus useful error reporting
 * Is this necessary?  bjm 2004-05-11
 * Originally this was stated to be here because pipe.c had backend linkage.
 * Perhaps that's no longer so now we have got rid of pipe.c amd 2012-03-28
 */
int pclose_check(FILE* stream)
{
    int exitstatus = pclose(stream);

    if (exitstatus == 0) {
        return 0; /* all is well */
    }

    if (exitstatus == -1) {
        /* pclose() itself failed, and hopefully set errno */
        perror("pclose failed");
    } else if (WIFEXITED(exitstatus)) {
        log_error(_("child process exited with exit code %d"), WEXITSTATUS(exitstatus));
    } else if (WIFSIGNALED(exitstatus)) {
#if defined(WIN32)
        log_error(_("child process was terminated by exception 0x%X"), WTERMSIG(exitstatus));
    }
#elif defined(HAVE_DECL_SYS_SIGLIST) && HAVE_DECL_SYS_SIGLIST
        char str[256];

        errno_t rc = snprintf_s(str,
            sizeof(str),
            sizeof(str) - 1,
            "%d: %s",
            WTERMSIG(exitstatus),
            (WTERMSIG(exitstatus) < NSIG) ? sys_siglist[WTERMSIG(exitstatus)] : "(unknown)");
        securec_check_ss_c(rc, "\0", "\0");

        log_error(_("child process was terminated by signal %s"), str);
    }
#else
        log_error(_("child process was terminated by signal %d"), WTERMSIG(exitstatus));
    }
#endif
    else {
        log_error(_("child process exited with unrecognized status %d"), exitstatus);
    }

    return -1;
}

/*
 *	set_pglocale_pgservice
 *
 *	Set application-specific locale and service directory
 *
 *	This function takes the value of argv[0] rather than a full path.
 *
 * (You may be wondering why this is in exec.c.  It requires this module's
 * services and doesn't introduce any new dependencies, so this seems as
 * good as anyplace.)
 */
void set_pglocale_pgservice(const char* argv0, const char* app)
{
    char path[MAXPGPATH];
    char my_exec_path[MAXPGPATH];

    /* longer than PGLOCALEDIR */
    char env_path[MAXPGPATH + sizeof("PGSYSCONFDIR=")];

    /* don't set LC_ALL in the backend */
    if (strcmp(app, PG_TEXTDOMAIN("gaussdb")) != 0) {
        (void)gs_setlocale_r(LC_ALL, "");
    }

    if (find_my_exec(argv0, my_exec_path) < 0) {
        return;
    }

#ifdef ENABLE_NLS
    get_locale_path(my_exec_path, path);
    bindtextdomain(app, path);
    textdomain(app);

    if (gs_getenv_r("PGLOCALEDIR") == NULL) {
        char* ptr = NULL;
        /* set for libpq to use */
        errno_t rc = snprintf_s(env_path, sizeof(env_path), sizeof(env_path) - 1, "PGLOCALEDIR=%s", path);
        securec_check_ss_c(rc, "\0", "\0");
        canonicalize_path(env_path + 12);
#ifdef FRONTEND
        ptr = strdup(env_path);
#else
        ptr = MemoryContextStrdup(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), env_path);
#endif
        if (ptr != NULL) {
            gs_putenv_r(ptr);
        }
    }
#endif

    if (gs_getenv_r("PGSYSCONFDIR") == NULL) {
        char* ptr = NULL;

        get_etc_path(my_exec_path, path, sizeof(path));

        /* set for libpq to use */
        errno_t rc = snprintf_s(env_path, sizeof(env_path), sizeof(env_path) - 1, "PGSYSCONFDIR=%s", path);
        securec_check_ss_c(rc, "\0", "\0");
        canonicalize_path(env_path + 13);
#ifdef FRONTEND
        ptr = strdup(env_path);
#else
        ptr = MemoryContextStrdup(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), env_path);
#endif
        if (ptr != NULL) {
            gs_putenv_r(ptr);
        }
    }
}

#ifdef WIN32

/*
 * AddUserToTokenDacl(HANDLE hToken)
 *
 * This function adds the current user account to the restricted
 * token used when we create a restricted process.
 *
 * This is required because of some security changes in Windows
 * that appeared in patches to XP/2K3 and in Vista/2008.
 *
 * On these machines, the Administrator account is not included in
 * the default DACL - you just get Administrators + System. For
 * regular users you get User + System. Because we strip Administrators
 * when we create the restricted token, we are left with only System
 * in the DACL which leads to access denied errors for later CreatePipe()
 * and CreateProcess() calls when running as Administrator.
 *
 * This function fixes this problem by modifying the DACL of the
 * token the process will use, and explicitly re-adding the current
 * user account.  This is still secure because the Administrator account
 * inherits its privileges from the Administrators group - it doesn't
 * have any of its own.
 */
BOOL AddUserToTokenDacl(HANDLE hToken)
{
    int i;
    ACL_SIZE_INFORMATION asi;
    ACCESS_ALLOWED_ACE* pace = NULL;
    DWORD dwNewAclSize;
    DWORD dwSize = 0;
    const DWORD dwTokenInfoLength = 0;
    PACL pacl = NULL;
    PTOKEN_USER pTokenUser = NULL;
    TOKEN_DEFAULT_DACL tddNew;
    TOKEN_DEFAULT_DACL* ptdd = NULL;
    TOKEN_INFORMATION_CLASS tic = TokenDefaultDacl;
    BOOL ret = FALSE;

    /* Figure out the buffer size for the DACL info */
    if (!GetTokenInformation(hToken, tic, (LPVOID)NULL, dwTokenInfoLength, &dwSize)) {
        if (GetLastError() == ERROR_INSUFFICIENT_BUFFER) {
            ptdd = (TOKEN_DEFAULT_DACL*)LocalAlloc(LPTR, dwSize);
            if (ptdd == NULL) {
                log_error(_("could not allocate %lu bytes of memory"), dwSize);
                goto cleanup;
            }

            if (!GetTokenInformation(hToken, tic, (LPVOID)ptdd, dwSize, &dwSize)) {
                log_error(_("could not get token information: error code %lu"), GetLastError());
                goto cleanup;
            }
        } else {
            log_error(_("could not get token information buffer size: error code %lu"), GetLastError());
            goto cleanup;
        }
    }

    /* Get the ACL info */
    if (ptdd != NULL &&
        !GetAclInformation(ptdd->DefaultDacl, (LPVOID)&asi, (DWORD)sizeof(ACL_SIZE_INFORMATION), AclSizeInformation)) {
        log_error(_("could not get ACL information: error code %lu"), GetLastError());
        goto cleanup;
    }

    /*
     * Get the user token for the current user, which provides us with the SID
     * that is needed for creating the ACL.
     */
    if (!GetTokenUser(hToken, &pTokenUser)) {
        log_error(_("could not get user token: error code %lu"), GetLastError());
        goto cleanup;
    }

    /* Figure out the size of the new ACL */
    dwNewAclSize = asi.AclBytesInUse + sizeof(ACCESS_ALLOWED_ACE) + GetLengthSid(pTokenUser->User.Sid) - sizeof(DWORD);

    /* Allocate the ACL buffer & initialize it */
    pacl = (PACL)LocalAlloc(LPTR, dwNewAclSize);
    if (pacl == NULL) {
        log_error(_("could not allocate %lu bytes of memory"), dwNewAclSize);
        goto cleanup;
    }

    if (!InitializeAcl(pacl, dwNewAclSize, ACL_REVISION)) {
        log_error(_("could not initialize ACL: error code %lu"), GetLastError());
        goto cleanup;
    }

    /* Loop through the existing ACEs, and build the new ACL */
    for (i = 0; i < (int)asi.AceCount; i++) {
        if (ptdd != NULL && !GetAce(ptdd->DefaultDacl, i, (LPVOID*)&pace)) {
            log_error(_("could not get ACE: error code %lu"), GetLastError());
            goto cleanup;
        }

        if (!AddAce(pacl, ACL_REVISION, MAXDWORD, pace, ((PACE_HEADER)pace)->AceSize)) {
            log_error(_("could not add ACE: error code %lu"), GetLastError());
            goto cleanup;
        }
    }

    /* Add the new ACE for the current user */
    if (!AddAccessAllowedAceEx(pacl, ACL_REVISION, OBJECT_INHERIT_ACE, GENERIC_ALL, pTokenUser->User.Sid)) {
        log_error(_("could not add access allowed ACE: error code %lu"), GetLastError());
        goto cleanup;
    }

    /* Set the new DACL in the token */
    tddNew.DefaultDacl = pacl;

    if (!SetTokenInformation(hToken, tic, (LPVOID)&tddNew, dwNewAclSize)) {
        log_error(_("could not set token information: error code %lu"), GetLastError());
        goto cleanup;
    }

    ret = TRUE;

cleanup:
    if (pTokenUser) {
        LocalFree((HLOCAL)pTokenUser);
    }

    if (pacl) {
        LocalFree((HLOCAL)pacl);
    }

    if (ptdd != NULL) {
        LocalFree((HLOCAL)ptdd);
    }

    return ret;
}

/*
 * GetTokenUser(HANDLE hToken, PTOKEN_USER *ppTokenUser)
 *
 * Get the users token information from a process token.
 *
 * The caller of this function is responsible for calling LocalFree() on the
 * returned TOKEN_USER memory.
 */
static BOOL GetTokenUser(HANDLE hToken, PTOKEN_USER* ppTokenUser)
{
    DWORD dwLength;

    *ppTokenUser = NULL;

    if (!GetTokenInformation(hToken, TokenUser, NULL, 0, &dwLength)) {
        if (GetLastError() == ERROR_INSUFFICIENT_BUFFER) {
            *ppTokenUser = (PTOKEN_USER)LocalAlloc(LPTR, dwLength);

            if (*ppTokenUser == NULL) {
                log_error(_("could not allocate %lu bytes of memory"), dwLength);
                return FALSE;
            }
        } else {
            log_error(_("could not get token information buffer size: error code %lu"), GetLastError());
            return FALSE;
        }
    }

    if (!GetTokenInformation(hToken, TokenUser, *ppTokenUser, dwLength, &dwLength)) {
        LocalFree(*ppTokenUser);
        *ppTokenUser = NULL;

        log_error(_("could not get token information: error code %lu"), GetLastError());
        return FALSE;
    }

    /* Memory in *ppTokenUser is LocalFree():d by the caller */
    return TRUE;
}

#endif
