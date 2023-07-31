/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * checkpoint_worker.h
 *    Describes the interface for callback methods from a worker thread to the manager.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/checkpoint/checkpoint_worker.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CHECKPOINT_WORKER_H
#define CHECKPOINT_WORKER_H

#include <mutex>
#include <set>
#include <atomic>
#include <vector>
#include <pthread.h>
#include <list>
#include "global.h"
#include "buffer.h"
#include "mm_gc_manager.h"

namespace MOT {
const int CHECKPOINT_BUFFER_SIZE = 4096 * 1000;

/**
 * @class CheckpointManagerCallbacks
 * @brief This class describes the interface for callback methods
 * from a worker thread to the manager.
 */
class CheckpointManagerCallbacks {
public:
    /**
     * @brief Checkpoint task completion callback
     * @param checkpointId The checkpoint's id.
     * @param table The table's pointer.
     * @param numSegs number of segments written.
     * @param success Indicates a success or a failure.
     */
    virtual void TaskDone(Table* table, uint32_t numSegs, bool success) = 0;

    /**
     * @brief Checks if the thread should terminate it work
     * @return True if the the thread should stop.
     */
    virtual bool ShouldStop() const = 0;

    /**
     * @brief Checkpoint task error callback
     * @param errCode The error's code.
     * @param errMsg The error's message.
     * @param optionalMsg An optional message to display.
     */
    virtual void OnError(int errCode, const char* errMsg, const char* optionalMsg = nullptr) = 0;

    virtual ~CheckpointManagerCallbacks()
    {}
};

/**
 * @class CheckpointWorkerPool
 * @brief this class implements the checkpointers working threads pool.
 */
class CheckpointWorkerPool {
public:
    CheckpointWorkerPool(int n, bool b, std::list<Table*>& l, uint32_t s, uint64_t id, CheckpointManagerCallbacks& m)
        : m_numWorkers(n), m_tasksList(l), m_checkpointId(id), m_na(b), m_cpManager(m), m_checkpointSegsize(s)
    {
        Start();
    }

    ~CheckpointWorkerPool();

    void Start();

    enum ErrCodes { SUCCESS = 0, FILE_IO = 1, MEMORY = 2, TABLE = 3, INDEX = 4, CALC = 5 };

    static constexpr uint16_t DELETE_LIST_SIZE = 1000;

private:
    /**
     * @brief The main worker function
     */
    void WorkerFunc();

    /**
     * @brief Appends a row to the buffer. The buffer will be flushed in case it is full.
     * @param buffer The buffer to fill.
     * @param row The row to write.
     * @param fd The file descriptor to write to.
     * @return Boolean value denoting success or failure.
     */
    bool Write(Buffer* buffer, Row* row, int fd);

    /**
     * @brief Checkpoints a row, according to whether a stable version exists or not.
     * @param buffer The buffer to fill.
     * @param sentinel The sentinel that holds to row.
     * @param fd The file descriptor to write to.
     * @param threadId The thread id.
     * @param isDeleted The row delete status.
     * @return -1 on error, 0 if nothing was written and 1 if the row was written.
     */
    int Checkpoint(Buffer* buffer, Sentinel* sentinel, int fd, uint16_t threadId, bool& isDeleted);

    /**
     * @brief Pops a task (table pointer) from the tasks queue.
     * @return the address of the pop'd table, or nullptr if the queue was empty.
     */
    Table* GetTask();

    /**
     * @brief Creates a checkpoint id for the current checkpoint
     * @return Boolean value denoting success or failure.
     */
    bool SetCheckpointId();

    /**
     * @brief Initializes a checkpoint file
     * @param fd The returned file descriptor of the file.
     * @param tableId The table id that is checkpointed.
     * @param seg The table's segment number
     * @param exId The table's external table id
     * @return Boolean value denoting success or failure.
     */
    bool BeginFile(int& fd, uint32_t tableId, int seg, uint64_t exId);

    /**
     * @brief Updates the file's header flushes and closes it.
     * @param fd The file descriptor of the file.
     * @param tableId The table id that is checkpointed.
     * @param numOps The number of operation that were save in the file.
     * @param exId The table's external table id
     * @return Boolean value denoting success or failure.
     */
    bool FinishFile(int& fd, uint32_t tableId, uint64_t numOps, uint64_t exId);

    void ExecuteMicroGcTransaction(
        Sentinel** deletedList, GcManager* gcSession, Table* table, uint16_t& deletedCounter, uint16_t limit);

    /**
     * @brief Writes table metadata to the metadata file.
     * @param table The table's pointer.
     * @return Returns the error code of type ErrCodes.
     */
    ErrCodes WriteTableMetadataFile(Table* table);

    /**
     * @brief Writes table data to the data file.
     * @param table The table's pointer.
     * @param buffer The buffer to fill.
     * @param deletedList Array to collect the sentinels deleted rows to be cleaned.
     * @param gcSession GC manager object.
     * @param threadId The thread id.
     * @param maxSegId The maximum segment ID of the table.
     * @param numOps The number of rows written.
     * @return Returns the error code of type ErrCodes.
     */
    ErrCodes WriteTableDataFile(Table* table, Buffer* buffer, Sentinel** deletedList, GcManager* gcSession,
        uint16_t threadId, uint32_t& maxSegId, uint64_t& numOps);

    bool FlushBuffer(int fd, Buffer* buffer);

    // Workers
    void* m_workers;

    volatile std::atomic<uint32_t> m_numWorkers;

    // Holds tables to checkpoint
    std::list<Table*>& m_tasksList;

    // Guards tasksList pops
    std::mutex m_tasksLock;

    // The directory in which checkpoint files will be saved in
    std::string m_workingDir;

    // Checkpoint's id
    uint64_t m_checkpointId;

    // The current NotAvailable bit
    bool m_na;

    // Checkpoint manager callbacks
    CheckpointManagerCallbacks& m_cpManager;

    // Size threshold
    uint32_t m_checkpointSegsize;
};
}  // namespace MOT

#endif  // CHECKPOINT_WORKER_H
