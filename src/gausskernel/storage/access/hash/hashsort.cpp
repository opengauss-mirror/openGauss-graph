/* -------------------------------------------------------------------------
 *
 * hashsort.cpp
 *    Sort tuples for insertion into a new hash index.
 *
 * When building a very large hash index, we pre-sort the tuples by bucket
 * number to improve locality of access to the index, and thereby avoid
 * thrashing.  We use tuplesort.c to sort the given index tuples into order.
 *
 * Note: if the number of rows in the table has been underestimated,
 * bucket splits may occur during the index build.	In that case we'd
 * be inserting into two or more buckets for each possible masked-off
 * hash code value.  That's no big problem though, since we'll still have
 * plenty of locality of access.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/hash/hashsort.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "utils/tuplesort.h"

/*
 * Status record for spooling/sorting phase.
 */
struct HSpool {
    Tuplesortstate *sortstate; /* state data for tuplesort.c */
    Relation index;
};

/*
 * create and initialize a spool structure
 */
HSpool *_h_spoolinit(Relation index, uint32 num_buckets, void *meminfo)
{
    HSpool *hspool = (HSpool *)palloc0(sizeof(HSpool));
    uint32 hash_mask;
    UtilityDesc *desc = (UtilityDesc *)meminfo;
    int work_mem = (desc->query_mem[0] > 0) ? desc->query_mem[0] : u_sess->attr.attr_memory.maintenance_work_mem;
    int max_mem = (desc->query_mem[1] > 0) ? desc->query_mem[1] : 0;

    hspool->index = index;

    /*
     * Determine the bitmask for hash code values.	Since there are currently
     * num_buckets buckets in the index, the appropriate mask can be computed
     * as follows.
     *
     * Note: at present, the passed-in num_buckets is always a power of 2, so
     * we could just compute num_buckets - 1.  We prefer not to assume that
     * here, though.
     */
    hash_mask = (((uint32)1) << _hash_log2(num_buckets)) - 1;

    /*
     * We size the sort area as maintenance_work_mem rather than work_mem to
     * speed index creation.  This should be OK since a single backend can't
     * run multiple index creations in parallel.
     */
    hspool->sortstate = tuplesort_begin_index_hash(index, hash_mask, work_mem, false, max_mem);

    return hspool;
}

/*
 * clean up a spool structure and its substructures.
 */
void _h_spooldestroy(HSpool *hspool)
{
    tuplesort_end(hspool->sortstate);
    pfree(hspool);
}

/*
 * spool an index entry into the sort file.
 */
void _h_spool(HSpool *hspool, ItemPointer self, Datum *values, const bool *isnull)
{
    tuplesort_putindextuplevalues(hspool->sortstate, hspool->index, self, values, isnull);
}

/*
 * given a spool loaded by successive calls to _h_spool,
 * create an entire index.
 */
void _h_indexbuild(HSpool *hspool)
{
    IndexTuple itup;
    bool should_free = false;

    tuplesort_performsort(hspool->sortstate);

    while ((itup = tuplesort_getindextuple(hspool->sortstate, true, &should_free)) != NULL) {
        _hash_doinsert(hspool->index, itup);
        if (should_free)
            pfree(itup);
    }
}
