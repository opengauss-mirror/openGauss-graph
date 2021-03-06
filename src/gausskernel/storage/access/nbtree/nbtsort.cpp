/* -------------------------------------------------------------------------
 *
 * nbtsort.cpp
 *    Build a btree from sorted input by loading leaf pages sequentially.
 *
 * NOTES
 *
 * We use tuplesort.c to sort the given index tuples into order.
 * Then we scan the index tuples in order and build the btree pages
 * for each level.	We load source tuples into leaf-level pages.
 * Whenever we fill a page at one level, we add a link to it to its
 * parent level (starting a new parent level if necessary).  When
 * done, we write out each final page on each level, adding it to
 * its parent level.  When we have only one page on a level, it must be
 * the root -- it can be attached to the btree metapage and we are done.
 *
 * This code is moderately slow (~10% slower) compared to the regular
 * btree (insertion) build code on sorted or well-clustered data.  On
 * random data, however, the insertion build code is unusable -- the
 * difference on a 60MB heap is a factor of 15 because the random
 * probes into the btree thrash the buffer pool.  (NOTE: the above
 * "10%" estimate is probably obsolete, since it refers to an old and
 * not very good external sort implementation that used to exist in
 * this module.  tuplesort.c is almost certainly faster.)
 *
 * It is not wise to pack the pages entirely full, since then *any*
 * insertion would cause a split (and not only of the leaf page; the need
 * for a split would cascade right up the tree).  The steady-state load
 * factor for btrees is usually estimated at 70%.  We choose to pack leaf
 * pages to the user-controllable fill factor (default 90%) while upper pages
 * are always packed to 70%.  This gives us reasonable density (there aren't
 * many upper pages if the keys are reasonable-size) without risking a lot of
 * cascading splits during early insertions.
 *
 * Formerly the index pages being built were kept in shared buffers, but
 * that is of no value (since other backends have no interest in them yet)
 * and it created locking problems for CHECKPOINT, because the upper-level
 * pages were held exclusive-locked for long periods.  Now we just build
 * the pages in local memory and smgrwrite or smgrextend them as we finish
 * them.  They will need to be re-read into shared buffers on first use after
 * the build finishes.
 *
 * Since the index will never be used unless it is completely built,
 * from a crash-recovery point of view there is no need to WAL-log the
 * steps of the build.	After completing the index build, we can just sync
 * the whole file to disk using smgrimmedsync() before exiting this module.
 * This can be seen to be sufficient for crash recovery by considering that
 * it's effectively equivalent to what would happen if a CHECKPOINT occurred
 * just after the index build.	However, it is clearly not sufficient if the
 * DBA is using the WAL log for PITR or replication purposes, since another
 * machine would not be able to reconstruct the index from WAL.  Therefore,
 * we log the completed index pages to WAL if and only if WAL archiving is
 * active.
 *
 * This code isn't concerned about the FSM at all. The caller is responsible
 * for initializing that.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/nbtree/nbtsort.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/smgr.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/aiomem.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/tuplesort.h"
#include "commands/tablespace.h"
#include "access/transam.h"
#include "utils/builtins.h"


/*
 * Status record for spooling/sorting phase.  (Note we may have two of
 * these due to the special requirements for uniqueness-checking with
 * dead tuples.)
 */
struct BTSpool {
    Tuplesortstate *sortstate; /* state data for tuplesort.c */
    Relation index;
    bool isunique;
};

static Page _bt_blnewpage(uint32 level);
static void _bt_slideleft(Page page);
static void _bt_sortaddtup(Page page, Size itemsize, IndexTuple itup, OffsetNumber itup_off);
static void _bt_load(BTWriteState *wstate, BTSpool *btspool, BTSpool *btspool2);

/*
 * Interface routines
 *
 * create and initialize a spool structure
 */
BTSpool *_bt_spoolinit(Relation index, bool isunique, bool isdead, void *meminfo)
{
    BTSpool *btspool = (BTSpool *)palloc0(sizeof(BTSpool));
    int btKbytes;
    UtilityDesc *desc = (UtilityDesc *)meminfo;
    int maxKbytes = isdead ? 0 : desc->query_mem[1];

    btspool->index = index;
    btspool->isunique = isunique;

    /*
     * We size the sort area as maintenance_work_mem rather than work_mem to
     * speed index creation.  This should be OK since a single backend can't
     * run multiple index creations in parallel.  Note that creation of a
     * unique index actually requires two BTSpool objects.	We expect that the
     * second one (for dead tuples) won't get very full, so we give it only
     * work_mem.
     */
    if (desc->query_mem[0] > 0)
        btKbytes = isdead ? SIMPLE_THRESHOLD : desc->query_mem[0];
    else
        btKbytes = isdead ? u_sess->attr.attr_memory.work_mem : u_sess->attr.attr_memory.maintenance_work_mem;
    btspool->sortstate = tuplesort_begin_index_btree(index, isunique, btKbytes, false, maxKbytes);

    /* We seperate 32MB for spool2, so cut this from the estimation */
    if (isdead) {
        desc->query_mem[0] -= SIMPLE_THRESHOLD;
    }

    return btspool;
}

/*
 * clean up a spool structure and its substructures.
 */
void _bt_spooldestroy(BTSpool *btspool)
{
    tuplesort_end(btspool->sortstate);
    pfree(btspool);
    btspool = NULL;
}

/*
 * spool an index entry into the sort file.
 */
void _bt_spool(BTSpool *btspool, ItemPointer self, Datum *values, const bool *isnull)
{
    tuplesort_putindextuplevalues(btspool->sortstate, btspool->index, self, values, isnull);
}

/*
 * given a spool loaded by successive calls to _bt_spool,
 * create an entire btree.
 */
void _bt_leafbuild(BTSpool *btspool, BTSpool *btspool2)
{
    BTWriteState wstate;

#ifdef BTREE_BUILD_STATS
    if (u_sess->attr.attr_resource.log_btree_build_stats) {
        ShowUsage("BTREE BUILD (Spool) STATISTICS");
        ResetUsage();
    }
#endif /* BTREE_BUILD_STATS */

    tuplesort_performsort(btspool->sortstate);
    if (btspool2 != NULL)
        tuplesort_performsort(btspool2->sortstate);

    wstate.index = btspool->index;

    /*
     * We need to log index creation in WAL iff WAL archiving/streaming is
     * enabled UNLESS the index isn't WAL-logged anyway.
     */
    wstate.btws_use_wal = XLogIsNeeded() && RelationNeedsWAL(wstate.index);

    /* reserve the metapage */
    wstate.btws_pages_alloced = BTREE_METAPAGE + 1;
    wstate.btws_pages_written = 0;
    wstate.btws_zeropage = NULL; /* until needed */

    _bt_load(&wstate, btspool, btspool2);
}

/*
 * Internal routines.
 *
 * allocate workspace for a new, clean btree page, not linked to any siblings.
 */
static Page _bt_blnewpage(uint32 level)
{
    Page page;
    BTPageOpaqueInternal opaque;

    ADIO_RUN()
    {
        page = (Page)adio_align_alloc(BLCKSZ);
    }
    ADIO_ELSE()
    {
        page = (Page)palloc(BLCKSZ);
    }
    ADIO_END();

    /* Zero the page and set up standard page header info */
    _bt_pageinit(page, BLCKSZ);

    /* Initialize BT opaque state */
    opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    opaque->btpo_prev = opaque->btpo_next = P_NONE;
    opaque->btpo.level = level;
    opaque->btpo_flags = (level > 0) ? 0 : BTP_LEAF;
    opaque->btpo_cycleid = 0;

    /* Make the P_HIKEY line pointer appear allocated */
    ((PageHeader)page)->pd_lower += sizeof(ItemIdData);

    return page;
}

/*
 * emit a completed btree page, and release the working storage.
 */
static void _bt_blwritepage(BTWriteState *wstate, Page page, BlockNumber blkno)
{
    bool need_free = false;
    errno_t errorno = EOK;

    if (blkno >= wstate->btws_pages_written) {
        // check tablespace size limitation when extending BTREE file.
        STORAGE_SPACE_OPERATION(wstate->index, ((uint64)BLCKSZ) * (blkno - wstate->btws_pages_written + 1));
    }

    /* Ensure rd_smgr is open (could have been closed by relcache flush!) */
    RelationOpenSmgr(wstate->index);

    /* XLOG stuff */
    if (wstate->btws_use_wal) {
        /* We use the heap NEWPAGE record type for this */
        log_newpage(&wstate->index->rd_node, MAIN_FORKNUM, blkno, page, true);
    }

    /*
     * If we have to write pages nonsequentially, fill in the space with
     * zeroes until we come back and overwrite.  This is not logically
     * necessary on standard Unix filesystems (unwritten space will read as
     * zeroes anyway), but it should help to avoid fragmentation. The dummy
     * pages aren't WAL-logged though.
     */
    while (blkno > wstate->btws_pages_written) {
        if (!wstate->btws_zeropage) {
            ADIO_RUN()
            {
                wstate->btws_zeropage = (Page)adio_align_alloc(BLCKSZ);
                errorno = memset_s(wstate->btws_zeropage, BLCKSZ, 0, BLCKSZ);
                securec_check_c(errorno, "", "");
            }
            ADIO_ELSE()
            {
                wstate->btws_zeropage = (Page)palloc0(BLCKSZ);
            }
            ADIO_END();
            need_free = true;
        }

        /* don't set checksum for all-zero page */
        ADIO_RUN()
        {
            /* pass null buffer to lower levels to use fallocate, systables do not use fallocate,
             * relation id can distinguish systable or use table. "FirstNormalObjectId".
             * but unfortunately , but in standby, there is no relation id, so relation id has no work.
             * relation file node can not help becasue operation vacuum full or set table space can
             * change systable file node
             */
            if (u_sess->attr.attr_sql.enable_fast_allocate) {
                smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM, wstate->btws_pages_written++, NULL, true);
            } else {
                smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM, wstate->btws_pages_written++,
                           (char *)(char *)wstate->btws_zeropage, true);
            }
        }
        ADIO_ELSE()
        {
            smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM, wstate->btws_pages_written++,
                       (char *)wstate->btws_zeropage, true);
        }
        ADIO_END();
    }

    char *bufToWrite = PageDataEncryptIfNeed(page);

    PageSetChecksumInplace((Page)bufToWrite, blkno);

    /*
     * Now write the page.	There's no need for smgr to schedule an fsync for
     * this write; we'll do it ourselves before ending the build.
     */
    if (blkno == wstate->btws_pages_written) {
        /* extending the file... */
        smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM, blkno, (char *)bufToWrite, true);
        wstate->btws_pages_written++;
    } else {
        /* overwriting a block we zero-filled before */
        smgrwrite(wstate->index->rd_smgr, MAIN_FORKNUM, blkno, (char *)bufToWrite, true);
    }

    ADIO_RUN()
    {
        if (need_free) {
            adio_align_free(wstate->btws_zeropage);
            wstate->btws_zeropage = NULL;
        }
        adio_align_free(page);
    }
    ADIO_ELSE()
    {
        if (need_free) {
            pfree(wstate->btws_zeropage);
            wstate->btws_zeropage = NULL;
        }
        pfree(page);
        page = NULL;
    }
    ADIO_END();
}

/*
 * allocate and initialize a new BTPageState.  the returned structure
 * is suitable for immediate use by _bt_buildadd.
 */
BTPageState *_bt_pagestate(BTWriteState *wstate, uint32 level)
{
    BTPageState *state = (BTPageState *)palloc0(sizeof(BTPageState));

    /* create initial page for level */
    state->btps_page = _bt_blnewpage(level);

    /* and assign it a page position */
    state->btps_blkno = wstate->btws_pages_alloced++;

    state->btps_minkey = NULL;
    /* initialize lastoff so first item goes into P_FIRSTKEY */
    state->btps_lastoff = P_HIKEY;
    state->btps_level = level;
    /* set "full" threshold based on level.  See notes at head of file. */
    if (level > 0) {
        state->btps_full = (BLCKSZ * (100 - BTREE_NONLEAF_FILLFACTOR) / 100);
    } else {
        state->btps_full = (Size)RelationGetTargetPageFreeSpace(wstate->index, BTREE_DEFAULT_FILLFACTOR);
    }
    /* no parent level, yet */
    state->btps_next = NULL;

    return state;
}

/*
 * @brief: slide an array of ItemIds back one slot (from P_FIRSTKEY to P_HIKEY,
 * overwriting P_HIKEY). we need to do this when we discover that we have built
 * an ItemId array in what has turned out to be a P_RIGHTMOST page.
 */
static void _bt_slideleft(Page page)
{
    OffsetNumber off;
    OffsetNumber maxoff;
    ItemId previi;
    ItemId thisii;

    if (!PageIsEmpty(page)) {
        maxoff = PageGetMaxOffsetNumber(page);
        previi = PageGetItemId(page, P_HIKEY);
        for (off = P_FIRSTKEY; off <= maxoff; off = OffsetNumberNext(off)) {
            thisii = PageGetItemId(page, off);
            *previi = *thisii;
            previi = thisii;
        }
        ((PageHeader)page)->pd_lower -= sizeof(ItemIdData);
    }
}

/*
 * Add an item to a page being built.
 *
 * The main difference between this routine and a bare PageAddItem call
 * is that this code knows that the leftmost data item on a non-leaf
 * btree page doesn't need to have a key.  Therefore, it strips such
 * items down to just the item header.
 *
 * This is almost like nbtinsert.c's _bt_pgaddtup(), but we can't use
 * that because it assumes that P_RIGHTMOST() will return the correct
 * answer for the page.  Here, we don't know yet if the page will be
 * rightmost.  Offset P_FIRSTKEY is always the first data key.
 */
static void _bt_sortaddtup(Page page, Size itemsize, IndexTuple itup, OffsetNumber itup_off)
{
    BTPageOpaqueInternal opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    IndexTupleData trunctuple;

    if (!P_ISLEAF(opaque) && itup_off == P_FIRSTKEY) {
        trunctuple = *itup;
        trunctuple.t_info = sizeof(IndexTupleData);
        if (t_thrd.proc->workingVersionNum >= SUPPORT_GPI_VERSION_NUM) {
            BTreeTupleSetNAtts(&trunctuple, 0);
        }
        itup = &trunctuple;
        itemsize = sizeof(IndexTupleData);
    }

    if (PageAddItem(page, (Item)itup, itemsize, itup_off, false, false) == InvalidOffsetNumber)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to add item to the index page")));
}

/* ----------
 * Add an item to a disk page from the sort output.
 *
 * We must be careful to observe the page layout conventions of nbtsearch.c:
 * - rightmost pages start data items at P_HIKEY instead of at P_FIRSTKEY.
 * - on non-leaf pages, the key portion of the first item need not be
 *	 stored, we should store only the link.
 *
 * A leaf page being built looks like:
 *
 * +----------------+---------------------------------+
 * | PageHeaderData | linp0 linp1 linp2 ...			  |
 * +-----------+----+---------------------------------+
 * | ... linpN |									  |
 * +-----------+--------------------------------------+
 * |	 ^ last										  |
 * |												  |
 * +-------------+------------------------------------+
 * |			 | itemN ...						  |
 * +-------------+------------------+-----------------+
 * |		  ... item3 item2 item1 | "special space" |
 * +--------------------------------+-----------------+
 *
 * Contrast this with the diagram in bufpage.h; note the mismatch
 * between linps and items.  This is because we reserve linp0 as a
 * placeholder for the pointer to the "high key" item; when we have
 * filled up the page, we will set linp0 to point to itemN and clear
 * linpN.  On the other hand, if we find this is the last (rightmost)
 * page, we leave the items alone and slide the linp array over.
 *
 * 'last' pointer indicates the last offset added to the page.
 * ----------
 */
void _bt_buildadd(BTWriteState *wstate, BTPageState *state, IndexTuple itup)
{
    Page npage;
    BlockNumber nblkno;
    OffsetNumber last_off;
    Size pgspc;
    Size itupsz;

    /*
     * This is a handy place to check for cancel interrupts during the btree
     * load phase of index creation.
     */
    CHECK_FOR_INTERRUPTS();

    npage = state->btps_page;
    nblkno = state->btps_blkno;
    last_off = state->btps_lastoff;

    pgspc = PageGetFreeSpace(npage);
    itupsz = IndexTupleDSize(*itup);
    itupsz = MAXALIGN(itupsz);
    /*
     * Check whether the item can fit on a btree page at all. (Eventually, we
     * ought to try to apply TOAST methods if not.) We actually need to be
     * able to fit three items on every page, so restrict any one item to 1/3
     * the per-page available space. Note that at this point, itupsz doesn't
     * include the ItemId.
     *
     * NOTE: similar code appears in _bt_insertonpg() to defend against
     * oversize items being inserted into an already-existing index. But
     * during creation of an index, we don't go through there.
     */
    if (itupsz > (Size)BTMaxItemSize(npage))
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("index row size %lu exceeds maximum %lu for index \"%s\"", (unsigned long)itupsz,
                               (unsigned long)BTMaxItemSize(npage), RelationGetRelationName(wstate->index)),
                        errhint("Values larger than 1/3 of a buffer page cannot be indexed.\n"
                                "Consider a function index of an MD5 hash of the value, "
                                "or use full text indexing.")));

    /*
     * Check to see if page is "full".	It's definitely full if the item won't
     * fit.  Otherwise, compare to the target freespace derived from the
     * fillfactor.	However, we must put at least two items on each page, so
     * disregard fillfactor if we don't have that many.
     */
    if (pgspc < itupsz || (pgspc < state->btps_full && last_off > P_FIRSTKEY)) {
        /*
         * Finish off the page and write it out.
         */
        Page opage = npage;
        BlockNumber oblkno = nblkno;
        ItemId ii;
        ItemId hii;
        IndexTuple oitup;
        IndexTuple keytup;
        BTPageOpaqueInternal opageop = (BTPageOpaqueInternal) PageGetSpecialPointer(opage);

        /* Create new page of same level */
        npage = _bt_blnewpage(state->btps_level);

        /* and assign it a page position */
        nblkno = wstate->btws_pages_alloced++;

        /*
         * We copy the last item on the page into the new page, and then
         * rearrange the old page so that the 'last item' becomes its high key
         * rather than a true data item.  There had better be at least two
         * items on the page already, else the page would be empty of useful
         * data.
         */
        Assert(last_off > P_FIRSTKEY);
        ii = PageGetItemId(opage, last_off);
        oitup = (IndexTuple)PageGetItem(opage, ii);
        _bt_sortaddtup(npage, ItemIdGetLength(ii), oitup, P_FIRSTKEY);

        /*
         * Move 'last' into the high key position on opage
         */
        hii = PageGetItemId(opage, P_HIKEY);
        *hii = *ii;
        ItemIdSetUnused(ii); /* redundant */
        ((PageHeader)opage)->pd_lower -= sizeof(ItemIdData);
        int indnatts = IndexRelationGetNumberOfAttributes(wstate->index);
        int indnkeyatts = IndexRelationGetNumberOfKeyAttributes(wstate->index);

        if (indnkeyatts != indnatts && P_ISLEAF(opageop)) {
            /*
             * We truncate included attributes of high key here.  Subsequent
             * insertions assume that hikey is already truncated, and so they
             * need not worry about it, when copying the high key into the
             * parent page as a downlink.
             *
             * The code above have just rearranged item pointers, but it
             * didn't save any space.  In order to save the space on page we
             * have to truly shift index tuples on the page.  But that's not
             * so bad for performance, because we operating pd_upper and don't
             * have to shift much of tuples memory.  Shift of ItemId's is
             * rather cheap, because they are small.
             */
            keytup = _bt_nonkey_truncate(wstate->index, oitup);
            /* delete "wrong" high key, insert keytup as P_HIKEY. */
            PageIndexTupleDelete(opage, P_HIKEY);
            _bt_sortaddtup(opage, IndexTupleSize(keytup), keytup, P_HIKEY);
        }
        /*
         * Link the old page into its parent, using its minimum key. If we
         * don't have a parent, we have to create one; this adds a new btree
         * level.
         */
        if (state->btps_next == NULL)
            state->btps_next = _bt_pagestate(wstate, state->btps_level + 1);

        Assert(state->btps_minkey != NULL);
        if (t_thrd.proc->workingVersionNum < SUPPORT_GPI_VERSION_NUM) {
            ItemPointerSet(&(state->btps_minkey->t_tid), oblkno, P_HIKEY);;
        } else {
            BTreeInnerTupleSetDownLink(state->btps_minkey, oblkno);
        }
        _bt_buildadd(wstate, state->btps_next, state->btps_minkey);
        pfree(state->btps_minkey);
        state->btps_minkey = NULL;

        /*
         * Save a copy of the minimum key for the new page.  We have to copy
         * it off the old page, not the new one, in case we are not at leaf
         * level.  Despite oitup is already initialized, it's important to get
         * high key from the page, since we could have replaced it with
         * truncated copy.	See comment above.
         */
        oitup = (IndexTuple) PageGetItem(opage, PageGetItemId(opage, P_HIKEY));
        state->btps_minkey = CopyIndexTuple(oitup);

        /*
         * Set the sibling links for both pages.
         */
        {
            BTPageOpaqueInternal oopaque = (BTPageOpaqueInternal)PageGetSpecialPointer(opage);
            BTPageOpaqueInternal nopaque = (BTPageOpaqueInternal)PageGetSpecialPointer(npage);

            oopaque->btpo_next = nblkno;
            nopaque->btpo_prev = oblkno;
            nopaque->btpo_next = P_NONE; /* redundant */
        }

        /*
         * Write out the old page.	We never need to touch it again, so we can
         * free the opage workspace too.
         */
        _bt_blwritepage(wstate, opage, oblkno);

        /*
         * Reset last_off to point to new page
         */
        last_off = P_FIRSTKEY;
    }

    /*
     * If the new item is the first for its page, stash a copy for later. Note
     * this will only happen for the first item on a level; on later pages,
     * the first item for a page is copied from the prior page in the code
     * above. Since the minimum key for an entire level is only used as a
     * minus infinity downlink, and never as a high key, there is no need to
     * truncate away non-key attributes at this point.
     */
    if (last_off == P_HIKEY) {
        Assert(state->btps_minkey == NULL);
        state->btps_minkey = CopyIndexTuple(itup);
        /* _bt_sortaddtup() will perform full truncation later */
        if (t_thrd.proc->workingVersionNum >= SUPPORT_GPI_VERSION_NUM) {
            BTreeTupleSetNAtts(state->btps_minkey, 0);
        }
    }

    /*
     * Add the new item into the current page.
     */
    last_off = OffsetNumberNext(last_off);
    _bt_sortaddtup(npage, itupsz, itup, last_off);

    state->btps_page = npage;
    state->btps_blkno = nblkno;
    state->btps_lastoff = last_off;
}

/*
 * Finish writing out the completed btree.
 */
void _bt_uppershutdown(BTWriteState *wstate, BTPageState *state)
{
    BTPageState *s = NULL;
    BlockNumber rootblkno = P_NONE;
    uint32 rootlevel = 0;
    Page metapage;

    /*
     * Each iteration of this loop completes one more level of the tree.
     */
    for (s = state; s != NULL; s = s->btps_next) {
        BlockNumber blkno;
        BTPageOpaqueInternal opaque;

        blkno = s->btps_blkno;
        opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(s->btps_page);

        /*
         * We have to link the last page on this level to somewhere.
         *
         * If we're at the top, it's the root, so attach it to the metapage.
         * Otherwise, add an entry for it to its parent using its minimum key.
         * This may cause the last page of the parent level to split, but
         * that's not a problem -- we haven't gotten to it yet.
         */
        if (s->btps_next == NULL) {
            opaque->btpo_flags |= BTP_ROOT;
            rootblkno = blkno;
            rootlevel = s->btps_level;
        } else {
            Assert(s->btps_minkey != NULL);
            if (t_thrd.proc->workingVersionNum < SUPPORT_GPI_VERSION_NUM) {
                ItemPointerSet(&(s->btps_minkey->t_tid), blkno, P_HIKEY);
            } else {
                BTreeInnerTupleSetDownLink(s->btps_minkey, blkno);
            }
            _bt_buildadd(wstate, s->btps_next, s->btps_minkey);
            pfree(s->btps_minkey);
            s->btps_minkey = NULL;
        }

        /*
         * This is the rightmost page, so the ItemId array needs to be slid
         * back one slot.  Then we can dump out the page.
         */
        _bt_slideleft(s->btps_page);
        _bt_blwritepage(wstate, s->btps_page, s->btps_blkno);
        s->btps_page = NULL; /* writepage freed the workspace */
    }

    /*
     * As the last step in the process, construct the metapage and make it
     * point to the new root (unless we had no data at all, in which case it's
     * set to point to "P_NONE").  This changes the index to the "valid" state
     * by filling in a valid magic number in the metapage.
     */
    // free in function _bt_blwritepage()
    ADIO_RUN()
    {
        metapage = (Page)adio_align_alloc(BLCKSZ);
    }
    ADIO_ELSE()
    {
        metapage = (Page)palloc(BLCKSZ);
    }
    ADIO_END();
    _bt_initmetapage(metapage, rootblkno, rootlevel);
    _bt_blwritepage(wstate, metapage, BTREE_METAPAGE);
}

/*
 * Read tuples in correct sort order from tuplesort, and load them into
 * btree leaves.
 */
static void _bt_load(BTWriteState *wstate, BTSpool *btspool, BTSpool *btspool2)
{
    BTPageState *state = NULL;
    bool merge = (btspool2 != NULL);
    IndexTuple itup = NULL;
    IndexTuple itup2 = NULL;
    bool should_free = false;
    bool should_free2 = false;
    bool load1 = false;
    TupleDesc tupdes = RelationGetDescr(wstate->index);
    int keysz = IndexRelationGetNumberOfKeyAttributes(wstate->index);
    ScanKey indexScanKey = NULL;

    if (merge) {
        /*
         * Another BTSpool for dead tuples exists. Now we have to merge
         * btspool and btspool2.
         *
         * the preparation of merge
         */
        itup = tuplesort_getindextuple(btspool->sortstate, true, &should_free);
        itup2 = tuplesort_getindextuple(btspool2->sortstate, true, &should_free2);
        indexScanKey = _bt_mkscankey_nodata(wstate->index);

        for (;;) {
            if (itup == NULL && itup2 == NULL) {
                break;
            }
            load1 = _index_tuple_compare(tupdes, indexScanKey, keysz, itup, itup2);

            /* When we see first tuple, create first index page */
            if (state == NULL)
                state = _bt_pagestate(wstate, 0);

            if (load1) {
                _bt_buildadd(wstate, state, itup);
                if (should_free) {
                    pfree(itup);
                    itup = NULL;
                }
                itup = tuplesort_getindextuple(btspool->sortstate, true, &should_free);
            } else {
                _bt_buildadd(wstate, state, itup2);
                if (should_free2) {
                    pfree(itup2);
                    itup2 = NULL;
                }
                itup2 = tuplesort_getindextuple(btspool2->sortstate, true, &should_free2);
            }
        }
        _bt_freeskey(indexScanKey);
    } else {
        /* merge is unnecessary */
        while ((itup = tuplesort_getindextuple(btspool->sortstate, true, &should_free)) != NULL) {
            /* When we see first tuple, create first index page */
            if (state == NULL)
                state = _bt_pagestate(wstate, 0);

            _bt_buildadd(wstate, state, itup);
            if (should_free) {
                pfree(itup);
                itup = NULL;
            }
        }
    }

    /* Close down final pages and write the metapage */
    _bt_uppershutdown(wstate, state);

    /*
     * If the index is WAL-logged, we must fsync it down to disk before it's
     * safe to commit the transaction.	(For a non-WAL-logged index we don't
     * care since the index will be uninteresting after a crash anyway.)
     *
     * It's obvious that we must do this when not WAL-logging the build. It's
     * less obvious that we have to do it even if we did WAL-log the index
     * pages.  The reason is that since we're building outside shared buffers,
     * a CHECKPOINT occurring during the build has no way to flush the
     * previously written data to disk (indeed it won't know the index even
     * exists).  A crash later on would replay WAL from the checkpoint,
     * therefore it wouldn't replay our earlier WAL entries. If we do not
     * fsync those pages here, they might still not be on disk when the crash
     * occurs.
     */
    if (RelationNeedsWAL(wstate->index)) {
        RelationOpenSmgr(wstate->index);
        smgrimmedsync(wstate->index->rd_smgr, MAIN_FORKNUM);
    }
}

/*
 * if itup <= itup2, return true;
 * if itup > itup2, return false.
 * itup == NULL && itup2 == NULL take care by caller
 */
bool _index_tuple_compare(TupleDesc tupdes, ScanKey indexScanKey, int keysz, IndexTuple itup, IndexTuple itup2)
{
    /* defaultly load itup, including the itup != NULL && itup2 == NULL case. */
    bool result = true;
    int i;

    if (itup == NULL && itup2 != NULL) {
        return false;
    }
    if (itup != NULL && itup2 == NULL) {
        return true;
    }
    Assert(itup != NULL && itup2 != NULL);
    for (i = 1; i <= keysz; i++) {
        ScanKey entry;
        Datum attrDatum1, attrDatum2;
        bool isNull1 = false;
        bool isNull2 = false;
        int32 compare;

        entry = indexScanKey + i - 1;
        attrDatum1 = index_getattr(itup, i, tupdes, &isNull1);
        attrDatum2 = index_getattr(itup2, i, tupdes, &isNull2);
        if (isNull1) {
            if (isNull2)
                compare = 0; /* NULL "=" NULL */
            else if (entry->sk_flags & SK_BT_NULLS_FIRST)
                compare = -1; /* NULL "<" NOT_NULL */
            else
                compare = 1; /* NULL ">" NOT_NULL */
        } else if (isNull2) {
            if (entry->sk_flags & SK_BT_NULLS_FIRST)
                compare = 1; /* NOT_NULL ">" NULL */
            else
                compare = -1; /* NOT_NULL "<" NULL */
        } else {
            compare = DatumGetInt32(FunctionCall2Coll(&entry->sk_func, entry->sk_collation, attrDatum1, attrDatum2));

            if (entry->sk_flags & SK_BT_DESC)
                compare = -compare;
        }

        // check compare value, if 0 continue, else break.
        if (compare > 0) {
            result = false;
            break;
        } else if (compare < 0)
            break;
    }
    return result;
}

List *insert_ordered_index(List *list, TupleDesc tupdes, ScanKey indexScanKey, int keysz, IndexTuple itup,
                           BlockNumber heapModifiedOffset, IndexScanDesc srcIdxRelScan)
{
    ListCell *prev = NULL;
    IndexTuple itup2 = NULL;
    BTOrderedIndexListElement *ele = NULL;

    // ignore null index tuple, but user should assure the validity of indexseq
    if (itup == NULL)
        return list;

    ele = (BTOrderedIndexListElement *)palloc0fast(sizeof(BTOrderedIndexListElement));
    if (list == NIL) {
        ele->itup = itup;
        ele->heapModifiedOffset = heapModifiedOffset;
        ele->indexScanDesc = srcIdxRelScan;
        return lcons(ele, list);
    }
    itup2 = ((BTOrderedIndexListElement *)linitial(list))->itup;
    if (itup == NULL && itup2 == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("index compare error, both are NULL")));
    }
    /* Does the datum belong at the front? */
    if (_index_tuple_compare(tupdes, indexScanKey, keysz, itup, itup2)) {
        ele->itup = itup;
        ele->heapModifiedOffset = heapModifiedOffset;
        ele->indexScanDesc = srcIdxRelScan;
        return lcons(ele, list);
    }
    /* No, so find the entry it belongs after */
    prev = list_head(list);
    for (;;) {
        ListCell *curr = lnext(prev);
        if (curr == NULL) {
            break;
        }
        itup2 = ((BTOrderedIndexListElement *)lfirst(curr))->itup;
        if (itup == NULL && itup2 == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("index compare error, both are NULL")));
        }
        if (_index_tuple_compare(tupdes, indexScanKey, keysz, itup, itup2)) {
            break; /* it belongs after 'prev', before 'curr' */
        }

        prev = curr;
    }

    /* Insert datum into list after 'prev' */
    ele->itup = itup;
    ele->heapModifiedOffset = heapModifiedOffset;
    ele->indexScanDesc = srcIdxRelScan;
    lappend_cell(list, prev, ele);
    return list;
}
