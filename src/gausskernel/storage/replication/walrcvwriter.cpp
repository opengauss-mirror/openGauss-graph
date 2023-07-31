/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2013, PostgreSQL Global Development Group
 *
 *
 * walrcvwriter.cpp
 *   functions for xlog receive management
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/replication/walrcvwriter.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "catalog/pg_tablespace.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "replication/walreceiver.h"
#include "replication/dataqueue.h"
#include "replication/datareceiver.h"
#include "replication/walsender.h"
#include "replication/dcf_replication.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/smgr/smgr.h"
#include "utils/guc.h"
#include "access/xlog.h"
#include "access/multi_redo_api.h"

#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "gssignal/gs_signal.h"
#include "gs_bbox.h"

#ifdef ENABLE_MULTIPLE_NODES
#include "postmaster/barrier_preparse.h"
#endif

/*
 * These variables are used similarly to openLogFile/SegNo/Off,
 * but for walreceiver to write the XLOG. recvFileTLI is the TimeLineID
 * corresponding the filename of recvFile.
 */
static volatile int recvFile = -1;
static volatile TimeLineID recvFileTLI = 0;
static volatile XLogSegNo recvSegNo = 0;
static volatile uint32 recvOff = 0;

#define MAX_DUMMY_DATA_FILE (RELSEG_SIZE * BLCKSZ)

/* max dummy data write file (default: 1GB) */
static int ws_dummy_data_writer_file_fd = -1;
static uint32 ws_dummy_data_writer_file_offset = 0;
bool ws_dummy_data_writer_use_file = false;

/* Signal handlers */
static void WSDataWriteOnDummyStandby(const char *buf, uint32 nbytes);
static void WSDataRcvWriteOnDummyStandby(const char *buf, uint32 nbytes);
static void walrcvWriterSigHupHandler(SIGNAL_ARGS);
static void walrcvWriterQuickDie(SIGNAL_ARGS);
static void reqShutdownHandler(SIGNAL_ARGS);

void SetWalRcvWriterPID(ThreadId tid)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    walrcv->writerPid = tid;
    SpinLockRelease(&walrcv->mutex);
}

static void setWalRcvWriterLatch(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    walrcv->walrcvWriterLatch = &t_thrd.proc->procLatch;
    walrcv->writerPid = t_thrd.proc_cxt.MyProcPid;
    SpinLockRelease(&walrcv->mutex);
}

static void emptyWalRcvWriterLatch(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    walrcv->walrcvWriterLatch = NULL;
    walrcv->writerPid = 0;
    SpinLockRelease(&walrcv->mutex);
}

bool WalRcvWriterInProgress(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    SpinLockAcquire(&walrcv->mutex);
    if (walrcv->writerPid == 0) {
        SpinLockRelease(&walrcv->mutex);
        return false;
    }
    SpinLockRelease(&walrcv->mutex);
    return true;
}

WalRcvCtlBlock *getCurrentWalRcvCtlBlock(void)
{
    WalRcvCtlBlock *walrcb = NULL;
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    SpinLockAcquire(&walrcv->mutex);
    walrcb = walrcv->walRcvCtlBlock;
    SpinLockRelease(&walrcv->mutex);
    return walrcb;
}

/*
 * Write XLOG data to disk.
 */
static void XLogWalRcvWrite(WalRcvCtlBlock *walrcb, char *buf, Size nbytes, XLogRecPtr recptr)
{
    int startoff;
    int byteswritten;
    int nRetCode = 0;
    Size write_bytes = nbytes;

    while (nbytes > 0) {
        int segbytes;
        /* use volatile pointer to prevent code rearrangement */
        volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

        if (recvFile < 0 || !XLByteInSeg(recptr, recvSegNo)) {
            bool use_existent = false;

            /*
             * fsync() and close current file before we switch to next one. We
             * would otherwise have to reopen this file to fsync it later
             */
            if (recvFile >= 0) {
                char xlogfname[MAXFNAMELEN];

                /*
                 * XLOG segment files will be re-read by recovery in startup
                 * process soon, so we don't advise the OS to release cache
                 * pages associated with the file like XLogFileClose() does.
                 */
                if (close(recvFile) != 0)
                    ereport(PANIC, (errcode_for_file_access(),
                                    errmsg("could not close log file %s: %m",
                                           XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, recvSegNo))));

                /*
                 * Create .done file forcibly to prevent the restored segment from
                 * being archived again later.
                 */
                XLogFileName(xlogfname, MAXFNAMELEN, recvFileTLI, recvSegNo);
                XLogArchiveForceDone(xlogfname);
            }
            recvFile = -1;

            /* Create/use new log file */
            XLByteToSeg(recptr, recvSegNo);
            use_existent = true;
            recvFile = XLogFileInit(recvSegNo, &use_existent, true);
            recvFileTLI = t_thrd.xlog_cxt.ThisTimeLineID;
            recvOff = 0;
        }

        /* Calculate the start offset of the received logs */
        startoff = recptr % XLogSegSize;

        if (startoff + nbytes > XLogSegSize)
            segbytes = XLogSegSize - startoff;
        else
            segbytes = nbytes;

        /* Need to seek in the file? */
        if (recvOff != (uint32)startoff) {
            if (lseek(recvFile, (off_t)startoff, SEEK_SET) < 0)
                ereport(PANIC, (errcode_for_file_access(),
                                errmsg("could not seek in log file %s to offset %d: %m",
                                       XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, recvSegNo), startoff)));
            recvOff = startoff;
        }

        /* OK to write the logs */
        errno = 0;

        byteswritten = write(recvFile, buf, segbytes);
        if (byteswritten <= 0) {
            /* if write didn't set errno, assume no disk space */
            if (errno == 0)
                errno = ENOSPC;
            ereport(PANIC,
                    (errcode_for_file_access(),
                     errmsg("could not write to log file %s at offset %u, length %lu: %m",
                            XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, recvSegNo), recvOff, INT2ULONG(segbytes))));
        }
        if (recvOff == (uint32)0 && segbytes >= (int)sizeof(XLogPageHeaderData)) {
            if (((XLogPageHeader)buf)->xlp_magic == XLOG_PAGE_MAGIC &&
                (recvSegNo * XLogSegSize) != ((XLogPageHeader)buf)->xlp_pageaddr) {
                ereport(PANIC, (errcode_for_file_access(),
                                errmsg("unexpected page addr %lu of log file %s", ((XLogPageHeader)buf)->xlp_pageaddr,
                                       XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, recvSegNo))));
            }
        }
        /* Update state for write */
        XLByteAdvance(recptr, byteswritten);

        recvOff += byteswritten;
        nbytes -= byteswritten;
        buf += byteswritten;

        issue_xlog_fsync(recvFile, recvSegNo);

        t_thrd.walrcvwriter_cxt.walStreamWrite = recptr;

        SpinLockAcquire(&walrcb->mutex);
        walrcb->writePtr = walrcb->flushPtr = recptr;
        SpinLockRelease(&walrcb->mutex);

        /* Update shared-memory status */
        SpinLockAcquire(&walrcv->mutex);
        if (XLByteLT(walrcv->receivedUpto, t_thrd.walrcvwriter_cxt.walStreamWrite)) {
            walrcv->latestChunkStart = walrcv->receivedUpto;
            walrcv->receivedUpto = t_thrd.walrcvwriter_cxt.walStreamWrite;
        }
        SpinLockRelease(&walrcv->mutex);

#ifdef ENABLE_MULTIPLE_NODES
        WakeUpBarrierPreParseBackend();

#endif
        /* Signal the startup process and walsender that new WAL has arrived */
        WakeupRecovery();
        if (AllowCascadeReplication())
            WalSndWakeup();

        /* Report XLOG streaming progress in PS display */
        if (u_sess->attr.attr_common.update_process_title) {
            char activitymsg[50];
            nRetCode = snprintf_s(activitymsg, sizeof(activitymsg), sizeof(activitymsg) - 1,
                                  "walrcvwriter streaming %X/%X",
                                  (uint32)(t_thrd.walrcvwriter_cxt.walStreamWrite >> 32),
                                  (uint32)t_thrd.walrcvwriter_cxt.walStreamWrite);
            securec_check_ss(nRetCode, "\0", "\0");
            set_ps_display(activitymsg, false);
        }

        ereport(DEBUG2, (errmsg("write xlog done: start %X/%X %lu bytes", (uint32)(recptr >> 32), (uint32)recptr,
                                write_bytes)));
    }
#ifndef ENABLE_MULTIPLE_NODES
    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
        UpdateRecordIdxState();
    }
#endif
}

void WalRcvXLogClose(void)
{
    LWLockAcquire(WALWriteLock, LW_EXCLUSIVE);

    if (recvFile >= 0) {
        /*
         * XLOG segment files will be re-read by recovery in startup
         * process soon, so we don't advise the OS to release cache
         * pages associated with the file like XLogFileClose() does.
         */
        if (close(recvFile) != 0)
            ereport(PANIC,
                    (errcode_for_file_access(), errmsg("could not close log file %s: %m",
                                                       XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, recvSegNo))));
    }
    recvFile = -1;

    LWLockRelease(WALWriteLock);
}

static void WSDataWriteOnDummyStandby(const char *buf, uint32 nbytes)
{
    ssize_t write_len = -1;
    char path[MAXPGPATH] = {0};
    int nRet = 0;

    /*
     * get  the file num to store:
     *  1. If MAX_DUMMY_DATA_FILE <= wal_data_writer_file_offset, open
     *     the next file to store
     *  2. If the current file can not store the nbytes, discard the current file space,
     *     open the next file to store.
     */
    if (ws_dummy_data_writer_file_offset >= MAX_DUMMY_DATA_FILE ||
        (MAX_DUMMY_DATA_FILE - ws_dummy_data_writer_file_offset < (uint32)(nbytes + sizeof(nbytes)))) {
        ereport(DEBUG2, (errmsg("data file num %u", t_thrd.walrcvwriter_cxt.ws_dummy_data_writer_file_num)));
        t_thrd.walrcvwriter_cxt.ws_dummy_data_writer_file_num++;
        ws_dummy_data_writer_file_offset = 0;

        if (ws_dummy_data_writer_file_fd >= 0) {
            close(ws_dummy_data_writer_file_fd);
            ws_dummy_data_writer_file_fd = -1;
        }
    }

    /* open the file */
    if (ws_dummy_data_writer_file_fd < 0) {
        /* get the file path */
        nRet = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "base/dummy_standby/%u",
                          t_thrd.walrcvwriter_cxt.ws_dummy_data_writer_file_num);
        securec_check_ss_c(nRet, "\0", "\0");

        ws_dummy_data_writer_file_fd = open(path, O_RDWR | O_CREAT | PG_BINARY, S_IRUSR | S_IWUSR);
        if (ws_dummy_data_writer_file_fd < 0)
            ereport(PANIC, (errcode_for_file_access(),
                            errmsg("could not create data file \"%s\", dummy_data_writer_file_fd=%d: %m", path,
                                   ws_dummy_data_writer_file_fd)));
    }

    errno = 0;
    /* OK to write the data */
    write_len = write(ws_dummy_data_writer_file_fd, &nbytes, sizeof(uint32));
    if (write_len < (ssize_t)sizeof(uint32)) {
        /* if write didn't set errno, assume no disk space */
        if (errno == 0) {
            errno = ENOSPC;
        }
        ereport(PANIC, (errcode_for_file_access(),
                        errmsg("could not write to data file %s buffer len %u, length %u: %m", path, nbytes, nbytes)));
    }

    errno = 0;
    write_len = write(ws_dummy_data_writer_file_fd, buf, nbytes);
    if (write_len < nbytes) {
        /* if write didn't set errno, assume no disk space */
        if (errno == 0)
            errno = ENOSPC;
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not write to data file %s "
                                                          "at offset %u, length %u: %m",
                                                          path, (uint32)ws_dummy_data_writer_file_offset, nbytes)));
    }

    ws_dummy_data_writer_file_offset = ws_dummy_data_writer_file_offset + nbytes + sizeof(nbytes);

    ereport(u_sess->attr.attr_storage.HaModuleDebug ? LOG : DEBUG2,
            (errmsg("the dummy data write info: writer_file_num %u %u bytes",
                    t_thrd.walrcvwriter_cxt.ws_dummy_data_writer_file_num, nbytes)));

    /* use fdatasync to make sure the received data to flush the disk */
    if (pg_fdatasync(ws_dummy_data_writer_file_fd) != 0)
        ereport(PANIC, (errcode_for_file_access(),
                        errmsg("could not fdatasync data file num %u, fd %d: %m",
                               t_thrd.walrcvwriter_cxt.ws_dummy_data_writer_file_num, ws_dummy_data_writer_file_fd)));
}

/*
 * Write data to disk of DummyStandby.
 */
static void WSDataRcvWriteOnDummyStandby(const char *buf, uint32 nbytes)
{
    ws_dummy_data_writer_use_file = true;
    WSDataWriteOnDummyStandby(buf, (uint32)nbytes);
    ws_dummy_data_writer_use_file = false;
}

/*
 * close data fd for rm data file
 */
void CloseWSDataFileOnDummyStandby(void)
{
    close(ws_dummy_data_writer_file_fd);
    ws_dummy_data_writer_file_fd = -1;
    ws_dummy_data_writer_file_offset = 0;
}

void InitWSDataNumOnDummyStandby(void)
{
    DIR *dir = NULL;
    struct dirent *de = NULL;
    int max_num_file = 0;
    int min_num_file = 0;
    char *dirpath = DUMMY_STANDBY_DATADIR;

    /* open the dir of base/dummy_standby */
    errno = 0;
    dir = AllocateDir(dirpath);
    if (dir == NULL && errno == ENOENT) {
        if (mkdir(dirpath, S_IRWXU) < 0) {
            /* Failure other than not exists */
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", dirpath)));
        }
        dir = AllocateDir(dirpath);
    }
    if (dir == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", dirpath)));
        return;
    }

    /* loop read the file name of base/dummy_standby */
    while ((de = ReadDir(dir, dirpath)) != NULL) {
        /* Skip special stuff */
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        min_num_file = atoi(de->d_name);
        break;
    }

    FreeDir(dir);
    dir = AllocateDir(dirpath);

    /* loop read the file name of base/dummy_standby */
    while ((de = ReadDir(dir, dirpath)) != NULL) {
        /* Skip special stuff */
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        /* search the max num file */
        max_num_file = Max(atoi(de->d_name), max_num_file);
        min_num_file = Min(atoi(de->d_name), min_num_file);
        ereport(DEBUG5, (errmsg("InitDummyDataNum de->d_name=%s;   max_num_path=%d; min_num_file=%d.", de->d_name,
                                max_num_file, min_num_file)));
    }

    t_thrd.walsender_cxt.ws_dummy_data_read_file_num = min_num_file;
    t_thrd.walrcvwriter_cxt.ws_dummy_data_writer_file_num = max_num_file;
    FreeDir(dir);
}

/*
 * Write all the wal data to disk.
 */
int WalDataRcvWrite(void)
{
    /* Use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    char *writeBuf = NULL;
    char *cur_buf = NULL;
    uint32 left_nbytes = 0;
    uint32 get_queue_nbytes = 0;
    uint32 remainbytes = 0;
    uint32 cur_data_nbytes = 0;
    DataQueuePtr start_pos = { 0, 0 };
    DataQueuePtr end_pos = { 0, 0 };
    WalRcvCtlBlock *walrcb = getCurrentWalRcvCtlBlock();
    errno_t errorno = EOK;
    XLogRecPtr recptr = InvalidXLogRecPtr;
    char data_flag;

    if (!g_instance.attr.attr_storage.enable_mix_replication)
        return walRcvWrite(walrcb);
    else if (!t_thrd.xlog_cxt.InRecovery)
        t_thrd.xlog_cxt.InRecovery = true;

    /* avoid concurrent WalDataRcvWrite by WalRcvWriteLock */
    START_CRIT_SECTION();
    LWLockAcquire(RcvWriteLock, LW_EXCLUSIVE);

    SpinLockAcquire(&walrcv->mutex);
    start_pos.queueid = walrcv->local_write_pos.queueid;
    start_pos.queueoff = walrcv->local_write_pos.queueoff;
    SpinLockRelease(&walrcv->mutex);

    get_queue_nbytes = GetFromDataQueue(writeBuf, g_instance.attr.attr_storage.DataQueueBufSize * 1024, start_pos,
                                        end_pos, true, t_thrd.dataqueue_cxt.DataWriterQueue);
    if (get_queue_nbytes == 0) {
        LWLockRelease(RcvWriteLock);
        END_CRIT_SECTION();
        return 0;
    }

    cur_buf = writeBuf;
    left_nbytes = get_queue_nbytes;

    while (left_nbytes > 0) {
        errorno = memcpy_s(&cur_data_nbytes, sizeof(uint32), cur_buf, sizeof(uint32));
        securec_check(errorno, "\0", "\0");
        data_flag = *(cur_buf + sizeof(uint32));

        switch (data_flag) {
            case 'w':
                Assert(cur_data_nbytes > (sizeof(uint32) + 1 + sizeof(XLogRecPtr)));
                errorno = memcpy_s(&recptr, sizeof(XLogRecPtr), cur_buf + sizeof(uint32) + 1, sizeof(XLogRecPtr));
                securec_check(errorno, "\0", "\0");
                Assert(!XLogRecPtrIsInvalid(recptr));

                (void)WSWalRcvWrite(walrcb, cur_buf + sizeof(uint32) + 1 + sizeof(XLogRecPtr),
                                    cur_data_nbytes - (sizeof(uint32) + 1 + sizeof(XLogRecPtr)), recptr);

                break;
            case 'd':
                if (u_sess->attr.attr_storage.HaModuleDebug)
                    WSDataRcvCheck(cur_buf, cur_data_nbytes);

                /* Write data. */
                if (dummyStandbyMode)
                    WSDataRcvWriteOnDummyStandby(cur_buf, cur_data_nbytes);
                else {
                    /* skip the data message prefix (total_len + 'd' + start_ptr + end_ptr)
                     *
                     * 1. total_len has already been parsed as cur_data_nbytes
                     */
                    cur_buf += sizeof(uint32);
                    /* 2. check and skip tag 'd' */
                    Assert(cur_buf[0] == 'd');
                    cur_buf += 1;
                    /* 3. skip the start_ptr and end_ptr */
                    cur_buf += sizeof(XLogRecPtr) * 2;
                    /* 4. calculate the post-proccess cur_data_nbytes */
                    cur_data_nbytes -= WS_DATA_MSG_PREFIX_LEN;
                    left_nbytes -= WS_DATA_MSG_PREFIX_LEN;

                    remainbytes = DoDataWrite(cur_buf, cur_data_nbytes);
                    Assert(remainbytes == 0);
                    cur_data_nbytes -= remainbytes;
                }
                /* Close all open files */
                smgrcloseall();
                break;
            default:
                Assert(false);
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unexpected wal data type %c", data_flag)));
                break;
        }

        cur_buf += cur_data_nbytes;
        left_nbytes -= cur_data_nbytes;
    }
    Assert(left_nbytes == 0);

    /* Update share memory from the the next operation. */
    SpinLockAcquire(&walrcv->mutex);
    walrcv->local_write_pos.queueid = start_pos.queueid;
    walrcv->local_write_pos.queueoff = start_pos.queueoff;
    DQByteAdvance(walrcv->local_write_pos, get_queue_nbytes);
    SpinLockRelease(&walrcv->mutex);

    PopFromDataQueue(((WalRcvData *)walrcv)->local_write_pos, t_thrd.dataqueue_cxt.DataWriterQueue);

    LWLockRelease(RcvWriteLock);
    END_CRIT_SECTION();

    return (int)get_queue_nbytes;
}

/*
 * Flush the log to disk.
 *
 * If we're in the midst of dying, it's unwise to do anything that might throw
 * an error, so we skip sending a reply in that case.
 */
int walRcvWrite(WalRcvCtlBlock *walrcb)
{
    int64 walfreeoffset;
    int64 walwriteoffset;
    char *walrecvbuf = NULL;
    XLogRecPtr startptr;
    int64 recBufferSize = g_instance.attr.attr_storage.WalReceiverBufSize * 1024;
    int nbytes = 0;

    if (walrcb == NULL)
        return 0;

    START_CRIT_SECTION();

    LWLockAcquire(WALWriteLock, LW_EXCLUSIVE);
    SpinLockAcquire(&walrcb->mutex);

    if (walrcb->walFreeOffset == walrcb->walWriteOffset) {
        SpinLockRelease(&walrcb->mutex);
        LWLockRelease(WALWriteLock);

        END_CRIT_SECTION();
        return 0;
    }
    walfreeoffset = walrcb->walFreeOffset;
    walwriteoffset = walrcb->walWriteOffset;
    walrecvbuf = walrcb->walReceiverBuffer;
    startptr = walrcb->walStart;
    SpinLockRelease(&walrcb->mutex);

    nbytes = (walfreeoffset < walwriteoffset) ? (recBufferSize - walwriteoffset) : (walfreeoffset - walwriteoffset);

    XLogWalRcvWrite(walrcb, walrecvbuf + walwriteoffset, nbytes, startptr);
    XLByteAdvance(startptr, nbytes);
    ereport(DEBUG5,
            (errmsg("walRcvWrite: write len:%d, at %u,%X", nbytes, (uint32)(startptr >> 32), (uint32)startptr)));

    SpinLockAcquire(&walrcb->mutex);
    walrcb->walWriteOffset += nbytes;
    walrcb->walStart = startptr;

    if (walrcb->walWriteOffset == recBufferSize) {
        walrcb->walWriteOffset = 0;
        if (walrcb->walFreeOffset == recBufferSize) {
            walrcb->walFreeOffset = 0;
        }
    }
    walfreeoffset = walrcb->walFreeOffset;
    walwriteoffset = walrcb->walWriteOffset;
    SpinLockRelease(&walrcb->mutex);

    ereport(DEBUG5, (errmsg("walRcvWrite: nbytes(%d),walfreeoffset(%ld),walwriteoffset(%ld),startptr(%u:%u)", nbytes,
                            walfreeoffset, walwriteoffset, (uint32)(startptr >> 32), (uint32)startptr)));

    LWLockRelease(WALWriteLock);

    END_CRIT_SECTION();
    /* the max lsn to be replayed */
    g_instance.comm_cxt.predo_cxt.redoPf.local_max_lsn = startptr;
    return nbytes;
}

/*
 * The main difference between walRcvWrite() and WSWalRcvWrite() is that
 * for walRcvWrite() the wal source is from the walrcb and for WSWalRcvWrite()
 * the wal source is from the wal write queue.
 */
int WSWalRcvWrite(WalRcvCtlBlock *walrcb, char *buf, Size nbytes, XLogRecPtr start_ptr)
{
    if (walrcb == NULL) {
        ereport(NOTICE, (errmsg("have nothing of the wal receiver wal block info.")));
        return 0;
    }

    START_CRIT_SECTION();
    LWLockAcquire(WALWriteLock, LW_EXCLUSIVE);

    XLogWalRcvWrite(walrcb, buf, nbytes, start_ptr);

    LWLockRelease(WALWriteLock);
    END_CRIT_SECTION();

    return nbytes;
}

/*
 * Called when the WalRcvWriterMain is ending.
 */
static void ShutdownWalRcvWriter(int code, Datum arg)
{
    /* clear WriterPid */
    emptyWalRcvWriterLatch();
}

/* SIGUSR1: let latch facility handle the signal */
static void WalRcvWriterProcSigUsr1Handler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    latch_sigusr1_handler();

    errno = saveErrno;
}

void walrcvWriterMain(void)
{
    sigjmp_buf localSigjmpBuf;
    sigset_t oldSigMask;
    MemoryContext walrcvWriterContext;

    ereport(LOG, (errmsg("walrcvwriter thread started")));
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGHUP, walrcvWriterSigHupHandler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, reqShutdownHandler);
    (void)gspqsignal(SIGQUIT, walrcvWriterQuickDie); /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, WalRcvWriterProcSigUsr1Handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    on_shmem_exit(ShutdownWalRcvWriter, 0);

    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "WalReceive Writer",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     */
    walrcvWriterContext = AllocSetContextCreate(t_thrd.top_mem_cxt, "WalReceive Writer", ALLOCSET_DEFAULT_MINSIZE,
                                                ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(walrcvWriterContext);

    /* init the dummy standby data num to write */
    if (dummyStandbyMode) {
        InitWSDataNumOnDummyStandby();
        t_thrd.walrcvwriter_cxt.ws_dummy_data_writer_file_num++;
        ws_dummy_data_writer_file_offset = 0;
    }

    /*
     * If an exception is encountered, processing resumes here.
     *
     * See notes in postgres.c about the design of this coding.
     */
    int curTryCounter = 0;
    int *oldTryCounter = nullptr;
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        // We need restore the signal mask of current thread
        // 
        pthread_sigmask(SIG_SETMASK, &oldSigMask, NULL);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* abort async io, must before LWlock release */
        AbortAsyncListIO();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        /*
         * These operations are really just a minimal subset of
         * AbortTransaction().  We don't have very many resources to worry
         * about in pagewriter, but we do have LWLocks, buffers, and temp files.
         */
        LWLockReleaseAll();
        AbortBufferIO();
        UnlockBuffers();
        /* buffer pins are released here: */
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        /* we needn't bother with the other ResourceOwnerRelease phases */
        AtEOXact_Buffers(false);
        AtEOXact_Files();
        AtEOXact_HashTables(false);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(walrcvWriterContext);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(walrcvWriterContext);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  A write error is likely
         * to be repeated, and we don't want to be filling the error logs as
         * fast as we can.
         */
        pg_usleep(1000000L);

        /*
         * Close all open files after any error.  This is helpful on Windows,
         * where holding deleted files open causes various strange errors.
         * It's not clear we need it elsewhere, but shouldn't hurt.
         */
        smgrcloseall();
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /*
     * Use the recovery target timeline ID during recovery
     */
    if (!RecoveryInProgress())
        ereport(FATAL, (errmsg("cannot continue WAL streaming, recovery has already ended")));

    t_thrd.xlog_cxt.ThisTimeLineID = GetRecoveryTargetTLI();

    /*
     * register procLatch to walrcv shared memory
     */
    setWalRcvWriterLatch();

    t_thrd.walrcvwriter_cxt.walStreamWrite = GetXLogReplayRecPtr(NULL);

    /*
     * Loop forever
     */
    for (;;) {
        int rc;

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        if (t_thrd.walrcvwriter_cxt.gotSIGHUP) {
            t_thrd.walrcvwriter_cxt.gotSIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        while (!t_thrd.walrcvwriter_cxt.shutdownRequested && WalDataRcvWrite() > 0)
            ;

        if (t_thrd.walrcvwriter_cxt.shutdownRequested) {
            ereport(LOG, (errmsg("walrcvwriter thread shut down")));
            /*
             * From here on, elog(ERROR) should end with exit(1), not send
             * control back to the sigsetjmp block above
             */
            u_sess->attr.attr_common.ExitOnAnyError = true;
            /* Normal exit from the pagewriter is here */
            proc_exit(0); /* done */
        }

        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, (long)1000 /* ms */);
        if (rc & WL_POSTMASTER_DEATH) {
            ereport(LOG, (errmsg("walrcvwriter thread shut down with code 1")));
            gs_thread_exit(1);
        }
    }
}

static void walrcvWriterSigHupHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.walrcvwriter_cxt.gotSIGHUP = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = saveErrno;
}

static void walrcvWriterQuickDie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    /*
     * We DO NOT want to run proc_exit() callbacks -- we're here because
     * shared memory may be corrupted, so we don't want to try to clean up our
     * transaction.  Just nail the windows shut and get out of town.  Now that
     * there's an atexit callback to prevent third-party code from breaking
     * things by calling exit() directly, we have to reset the callbacks
     * explicitly to make this work as intended.
     */
    on_exit_reset();

    /*
     * Note we do exit(2) not exit(0).    This is to force the postmaster into a
     * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
     * backend.  This is necessary precisely because we don't clean up our
     * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
     * should ensure the postmaster sees this as a crash, too, but no harm in
     * being doubly sure.)
     */
    gs_thread_exit(2);
}

static void reqShutdownHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.walrcvwriter_cxt.shutdownRequested = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = saveErrno;
}
