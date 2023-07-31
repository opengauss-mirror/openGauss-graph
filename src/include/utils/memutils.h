/* -------------------------------------------------------------------------
 *
 * memutils.h
 *	  This file contains declarations for memory allocation utility
 *	  functions.  These are functions that are not quite widely used
 *	  enough to justify going in utils/palloc.h, but are still part
 *	  of the API of the memory management subsystem.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/memutils.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef MEMUTILS_H
#define MEMUTILS_H

#include "nodes/memnodes.h"
#include "pgtime.h"
#include "storage/proc.h"

enum MemoryContextType {
    STANDARD_CONTEXT,  // postgres orignal context
    STACK_CONTEXT,     // a simple context, do not support free single pointer
    SHARED_CONTEXT,    // shared context used by different threads
    MEMALIGN_CONTEXT,  // the context only used to allocate the aligned memory
    MEMALIGN_SHRCTX,   // the shared context only used to allocate the aligned memory
};

/*
 * MaxAllocSize
 *		Quasi-arbitrary limit on size of allocations.
 *
 * Note:
 *		There is no guarantee that allocations smaller than MaxAllocSize
 *		will succeed.  Allocation requests larger than MaxAllocSize will
 *		be summarily denied.
 *
 * XXX This is deliberately chosen to correspond to the limiting size
 * of varlena objects under TOAST.	See VARSIZE_4B() and related macros
 * in postgres.h.  Many datatypes assume that any allocatable size can
 * be represented in a varlena header.
 *
 * XXX Also, various places in aset.c assume they can compute twice an
 * allocation's size without overflow, so beware of raising this.
 */
#define MaxAllocSize ((Size)0x3fffffff) /* 1 gigabyte - 1 */

#define MaxBuildAllocSize ((Size)0x3ffffff) /* 64MB - 1 */

#define AllocSizeIsValid(size) ((Size)(size) <= MaxAllocSize)

#define MaxAllocHugeSize ((Size)-1 >> 1) /* SIZE_MAX / 2 */

#define AllocHugeSizeIsValid(size) ((Size)(size) <= MaxAllocHugeSize)

/*
 * All chunks allocated by any memory context manager are required to be
 * preceded by a StandardChunkHeader at a spacing of STANDARDCHUNKHEADERSIZE.
 * A currently-allocated chunk must contain a backpointer to its owning
 * context as well as the allocated size of the chunk.	The backpointer is
 * used by pfree() and repalloc() to find the context to call.	The allocated
 * size is not absolutely essential, but it's expected to be needed by any
 * reasonable implementation.
 */
typedef struct StandardChunkHeader {
    MemoryContext context; /* owning context */
    Size size;             /* size of data space allocated in chunk */
#ifdef MEMORY_CONTEXT_CHECKING
    /* when debugging memory usage, also store actual requested size */
    Size requested_size;
#endif
#ifdef MEMORY_CONTEXT_TRACK
    const char* file;
    int line;
#endif
} StandardChunkHeader;

#define STANDARDCHUNKHEADERSIZE MAXALIGN(sizeof(StandardChunkHeader))

// Process wise memory context
//
extern MemoryContext AdioSharedContext;
extern MemoryContext ProcSubXidCacheContext;
extern MemoryContext PmTopMemoryContext;
extern MemoryContext StreamInfoContext;
extern PGDLLIMPORT MemoryContext TopTransactionContext;

extern THR_LOCAL PGDLLIMPORT MemoryContext ErrorContext;

/*
 * Memory-context-type-independent functions in mcxt.c
 */
extern void MemoryContextInit(void);
extern void MemoryContextReset(MemoryContext context);
extern void MemoryContextDelete(MemoryContext context);
extern void MemoryContextResetChildren(MemoryContext context);
extern void MemoryContextDeleteChildren(MemoryContext context, List* context_list = NULL);
extern void MemoryContextDestroyAtThreadExit(MemoryContext context);
extern void MemoryContextResetAndDeleteChildren(MemoryContext context);
extern void MemoryContextSetParent(MemoryContext context, MemoryContext new_parent);
extern Size GetMemoryChunkSpace(void* pointer);
extern MemoryContext GetMemoryChunkContext(void* pointer);
extern MemoryContext MemoryContextGetParent(MemoryContext context);
extern bool MemoryContextIsEmpty(MemoryContext context);
extern void MemoryContextStats(MemoryContext context);
extern void MemoryContextSeal(MemoryContext context);
extern void MemoryContextUnSeal(MemoryContext context);
extern void MemoryContextUnSealChildren(MemoryContext context);
extern void MemoryContextAllowInCriticalSection(MemoryContext context, bool allow);

#ifdef MEMORY_CONTEXT_CHECKING
extern void MemoryContextCheck(MemoryContext context, bool own_by_session);
#define MemoryContextCheck2(mctx)
#endif

extern bool MemoryContextContains(MemoryContext context, void* pointer);

extern MemoryContext MemoryContextOriginal(const char* node);

/*
 * This routine handles the context-type-independent part of memory
 * context creation.  It's intended to be called from context-type-
 * specific creation routines, and noplace else.
 */
extern MemoryContext MemoryContextCreate(
    NodeTag tag, Size size, MemoryContext parent, const char* name, const char* file, int line);

/* Interface for PosgGIS and has the same structure with PG's MemoryContextCreate*/
extern MemoryContext MemoryContextCreate(
    NodeTag tag, Size size, MemoryContextMethods* methods, MemoryContext parent, const char* name);

/*
 * Memory-context-type-specific functions
 */
#define DEFAULT_MEMORY_CONTEXT_MAX_SIZE 0                  /* 0 MB as default value for AllocSetContextCreat function */
#define SHARED_MEMORY_CONTEXT_MAX_SIZE (100 * 1024 * 1024) /* 100 MB */
/* aset.c */
extern MemoryContext AllocSetContextCreate(MemoryContext parent, const char* name, Size minContextSize,
    Size initBlockSize, Size maxBlockSize, MemoryContextType type = STANDARD_CONTEXT,
    Size maxSize = DEFAULT_MEMORY_CONTEXT_MAX_SIZE, bool isSession = false);

/*
 * Recommended default alloc parameters, suitable for "ordinary" contexts
 * that might hold quite a lot of data.
 */
#define ALLOCSET_DEFAULT_MINSIZE 0
#define ALLOCSET_DEFAULT_INITSIZE (8 * 1024)
#define ALLOCSET_DEFAULT_MAXSIZE (8 * 1024 * 1024)
#define ALLOCSET_DEFAULT_SIZES ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE

/*
 * Recommended alloc parameters for "small" contexts that are not expected
 * to contain much data (for example, a context to contain a query plan).
 */
#define ALLOCSET_SMALL_MINSIZE 0
#define ALLOCSET_SMALL_INITSIZE (1 * 1024)
#define ALLOCSET_SMALL_MAXSIZE (8 * 1024)
#define ALLOCSET_SMALL_SIZES \
	ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE

/* default grow ratio for sort and materialize when it spreads */
#define DEFAULT_GROW_RATIO 2.0

/* default auto spread min ratio, and memory spread under this ratio is abondaned */
#define MEM_AUTO_SPREAD_MIN_RATIO 0.1

// AutoContextSwitch
//      Auto object for memoryContext switch
//
class AutoContextSwitch {
public:
    AutoContextSwitch(MemoryContext memContext)
    {
        if (memContext == NULL) {
            m_oldMemContext = CurrentMemoryContext;
        } else {
            m_oldMemContext = MemoryContextSwitchTo(memContext);
        }
    };

    ~AutoContextSwitch()
    {
        MemoryContextSwitchTo(m_oldMemContext);
    }

private:
    MemoryContext m_oldMemContext;
};

/* In case we haven't init u_sess->when we try to create memory context and alloc memory */
#define IS_USESS_AVAILABLE (likely(u_sess != NULL))
#define GS_MP_INITED (t_thrd.utils_cxt.gs_mp_inited)
#define MEMORY_TRACKING_MODE (IS_USESS_AVAILABLE ? u_sess->attr.attr_memory.memory_tracking_mode : 0)
#define MEMORY_TRACKING_QUERY_PEAK (IS_USESS_AVAILABLE ? (u_sess->attr.attr_memory.memory_tracking_mode == MEMORY_TRACKING_PEAKMEMORY ) : 0)
#define ENABLE_MEMORY_CONTEXT_CONTROL \
    (IS_USESS_AVAILABLE ? u_sess->attr.attr_memory.enable_memory_context_control : false)
#define MEMORY_FAULT_PERCENT (IS_USESS_AVAILABLE ? u_sess->attr.attr_resource.memory_fault_percent : 0)
#define STATEMENT_MAX_MEM (IS_USESS_AVAILABLE ? u_sess->attr.attr_sql.statement_max_mem : 0)

#endif /* MEMUTILS_H */
