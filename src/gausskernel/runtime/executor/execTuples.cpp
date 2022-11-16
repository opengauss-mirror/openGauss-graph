/* -------------------------------------------------------------------------
 *
 * execTuples.cpp
 *	  Routines dealing with TupleTableSlots.  These are used for resource
 *	  management associated with tuples (eg, releasing buffer pins for
 *	  tuples in disk buffers, or freeing the memory occupied by transient
 *	  tuples).	Slots also provide access abstraction that lets us implement
 *	  "virtual" tuples to reduce data-copying overhead.
 *
 *	  Routines dealing with the type information for tuples. Currently,
 *	  the type information for a tuple is an array of FormData_pg_attribute.
 *	  This information is needed by routines manipulating tuples
 *	  (getattribute, formtuple, etc.).
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/execTuples.cpp
 *
 * -------------------------------------------------------------------------
 * INTERFACE ROUTINES
 *
 *	 SLOT CREATION/DESTRUCTION
 *		MakeTupleTableSlot		- create an empty slot
 *		ExecAllocTableSlot		- create a slot within a tuple table
 *		ExecResetTupleTable		- clear and optionally delete a tuple table
 *		MakeSingleTupleTableSlot - make a standalone slot, set its descriptor
 *		ExecDropSingleTupleTableSlot - destroy a standalone slot
 *
 *	 SLOT ACCESSORS
 *		ExecSetSlotDescriptor	- set a slot's tuple descriptor
 *		ExecStoreTuple			- store a physical tuple in the slot
 *		ExecStoreMinimalTuple	- store a minimal physical tuple in the slot
 *		ExecClearTuple			- clear contents of a slot
 *		ExecStoreVirtualTuple	- mark slot as containing a virtual tuple
 *		ExecCopySlotTuple		- build a physical tuple from a slot
 *		ExecCopySlotMinimalTuple - build a minimal physical tuple from a slot
 *		ExecMaterializeSlot		- convert virtual to physical storage
 *		ExecCopySlot			- copy one slot's contents to another
 *
 *	 CONVENIENCE INITIALIZATION ROUTINES
 *		ExecInitResultTupleSlot    \	convenience routines to initialize
 *		ExecInitScanTupleSlot		\	the various tuple slots for nodes
 *		ExecInitExtraTupleSlot		/	which store copies of tuples.
 *		ExecInitNullTupleSlot	   /
 *
 *	 Routines that probably belong somewhere else:
 *		ExecTypeFromTL			- form a TupleDesc from a target list
 *
 *	 EXAMPLE OF HOW TABLE ROUTINES WORK
 *		Suppose we have a query such as SELECT emp.name FROM emp and we have
 *		a single SeqScan node in the query plan.
 *
 *		At ExecutorStart()
 *		----------------
 *		- ExecInitSeqScan() calls ExecInitScanTupleSlot() and
 *		  ExecInitResultTupleSlot() to construct TupleTableSlots
 *		  for the tuples returned by the access methods and the
 *		  tuples resulting from performing target list projections.
 *
 *		During ExecutorRun()
 *		----------------
 *		- SeqNext() calls ExecStoreTuple() to place the tuple returned
 *		  by the access methods into the scan tuple slot.
 *
 *		- ExecSeqScan() calls ExecStoreTuple() to take the result
 *		  tuple from ExecProject() and place it into the result tuple slot.
 *
 *		- ExecutePlan() calls ExecSelect(), which passes the result slot
 *		  to printtup(), which uses getallattrs() to extract the
 *		  individual Datums for printing.
 *
 *		At ExecutorEnd()
 *		----------------
 *		- EndPlan() calls ExecResetTupleTable() to clean up any remaining
 *		  tuples left over from executing the query.
 *
 *		The important thing to watch in the executor code is how pointers
 *		to the slots containing tuples are passed instead of the tuples
 *		themselves.  This facilitates the communication of related information
 *		(such as whether or not a tuple should be pfreed, what buffer contains
 *		this tuple, the tuple's tuple descriptor, etc).  It also allows us
 *		to avoid physically constructing projection tuples in many cases.
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "funcapi.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "storage/buf/bufmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/memutils.h"

#include "access/ustore/knl_utuple.h"
#ifdef GS_GRAPH
#include "executor/tuptable.h"
static inline void slot_deform_heap_tuple(TupleTableSlot *slot, HeapTuple tuple, uint32 *offp,
															  int natts);
static inline void tts_buffer_heap_store_tuple(TupleTableSlot *slot,
											   HeapTuple tuple,
											   Buffer buffer,
											   bool transfer_pin);
static void tts_heap_store_tuple(TupleTableSlot *slot, HeapTuple tuple, bool shouldFree);

#endif
static TupleDesc ExecTypeFromTLInternal(List* target_list, bool has_oid, bool skip_junk, bool mark_dropped = false, TableAmType tam = TAM_HEAP);

/* ----------------------------------------------------------------
 *				  tuple table create/delete functions
 * ----------------------------------------------------------------
 */
/* --------------------------------
 *		MakeTupleTableSlot
 *
 *		Basic routine to make an empty TupleTableSlot.
 * --------------------------------
 */
TupleTableSlot* MakeTupleTableSlot(bool has_tuple_mcxt, TableAmType tupslotTableAm)
{
    TupleTableSlot* slot = makeNode(TupleTableSlot);
    Assert(tupslotTableAm == TAM_HEAP || tupslotTableAm == TAM_USTORE);

    slot->tts_isempty = true;
    slot->tts_shouldFree = false;
    slot->tts_shouldFreeMin = false;
    slot->tts_tuple = NULL;
    slot->tts_tupleDescriptor = NULL;
#ifdef PGXC
    slot->tts_shouldFreeRow = false;
    slot->tts_dataRow = NULL;
    slot->tts_dataLen = -1;
    slot->tts_attinmeta = NULL;
#endif
    slot->tts_mcxt = CurrentMemoryContext;
    slot->tts_buffer = InvalidBuffer;
    slot->tts_nvalid = 0;
    slot->tts_values = NULL;
    slot->tts_isnull = NULL;
    slot->tts_mintuple = NULL;
#ifdef ENABLE_MULTIPLE_NODES
    slot->tts_per_tuple_mcxt = has_tuple_mcxt ? AllocSetContextCreate(slot->tts_mcxt,
        "SlotPerTupleMcxt",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE) : NULL;
#endif
    slot->tts_tupslotTableAm = tupslotTableAm;

    return slot;
}

/* --------------------------------
 *		ExecAllocTableSlot
 *
 *		Create a tuple table slot within a tuple table (which is just a List).
 * --------------------------------
 */
TupleTableSlot* ExecAllocTableSlot(List** tuple_table, TableAmType tupslotTableAm)
{
    TupleTableSlot* slot;

    slot = MakeTupleTableSlot();

    *tuple_table = lappend(*tuple_table, slot);

    slot->tts_tupslotTableAm = tupslotTableAm;

    return slot;
}

/* --------------------------------
 *		ExecResetTupleTable
 *
 *		This releases any resources (buffer pins, tupdesc refcounts)
 *		held by the tuple table, and optionally releases the memory
 *		occupied by the tuple table data structure.
 *		It is expected that this routine be called by EndPlan().
 * --------------------------------
 */
void ExecResetTupleTable(List* tuple_table, /* tuple table */
    bool should_free)                       /* true if we should free memory */
{
    ListCell* lc = NULL;

    foreach (lc, tuple_table) {
        TupleTableSlot* slot = (TupleTableSlot*)lfirst(lc);

        /* Always release resources and reset the slot to empty */
        (void)ExecClearTuple(slot);
        if (slot->tts_tupleDescriptor) {
            ReleaseTupleDesc(slot->tts_tupleDescriptor);
            slot->tts_tupleDescriptor = NULL;
        }

        /* If shouldFree, release memory occupied by the slot itself */
        if (should_free) {
            if (slot->tts_values)
                pfree_ext(slot->tts_values);
            if (slot->tts_isnull)
                pfree_ext(slot->tts_isnull);
            pfree_ext(slot->tts_lobPointers);
            if (slot->tts_per_tuple_mcxt)
                MemoryContextDelete(slot->tts_per_tuple_mcxt);
            pfree_ext(slot);
        }
    }

    /* If shouldFree, release the list structure */
    if (should_free) {
        list_free_ext(tuple_table);
    }
}

TupleTableSlot* ExecMakeTupleSlot(Tuple tuple, TableScanDesc tableScan, TupleTableSlot* slot, TableAmType tableAm)
{
    if (unlikely(RELATION_CREATE_BUCKET(tableScan->rs_rd))) {
        tableScan = ((HBktTblScanDesc)tableScan)->currBktScan;
    }

    if (tuple != NULL) {
        Assert(tableScan != NULL);
        slot->tts_tupslotTableAm = tableAm;
        return ExecStoreTuple(tuple, /* tuple to store */
            slot, /* slot to store in */
            tableScan->rs_cbuf, /* buffer associated with this tuple */
            false); /* don't pfree this pointer */
    }

    return ExecClearTuple(slot);
}

/* --------------------------------
 *		MakeSingleTupleTableSlot
 *
 *		This is a convenience routine for operations that need a
 *		standalone TupleTableSlot not gotten from the main executor
 *		tuple table.  It makes a single slot and initializes it
 *		to use the given tuple descriptor.
 * --------------------------------
 */
TupleTableSlot* MakeSingleTupleTableSlot(TupleDesc tup_desc, bool allocSlotCxt, TableAmType tupslotTableAm)
{
    TupleTableSlot* slot = MakeTupleTableSlot(allocSlotCxt, tupslotTableAm);
    ExecSetSlotDescriptor(slot, tup_desc);
    return slot;
}

/* --------------------------------
 *		ExecDropSingleTupleTableSlot
 *
 *		Release a TupleTableSlot made with MakeSingleTupleTableSlot.
 *		DON'T use this on a slot that's part of a tuple table list!
 * --------------------------------
 */
void ExecDropSingleTupleTableSlot(TupleTableSlot* slot)
{
    /* This should match ExecResetTupleTable's processing of one slot */
    (void)ExecClearTuple(slot);
    if (slot->tts_tupleDescriptor != NULL) {
        ReleaseTupleDesc(slot->tts_tupleDescriptor);
    }

    if (slot->tts_values != NULL) {
        pfree_ext(slot->tts_values);
    }

    if (slot->tts_isnull != NULL) {
        pfree_ext(slot->tts_isnull);
    }

    pfree_ext(slot->tts_lobPointers);

    if (slot->tts_per_tuple_mcxt != NULL) {
        MemoryContextDelete(slot->tts_per_tuple_mcxt);
    }
    pfree_ext(slot);
}

/* ----------------------------------------------------------------
 *				  tuple table slot accessor functions
 * ----------------------------------------------------------------
 */
/* --------------------------------
 *		ExecSetSlotDescriptor
 *
 *		This function is used to set the tuple descriptor associated
 *		with the slot's tuple.  The passed descriptor must have lifespan
 *		at least equal to the slot's.  If it is a reference-counted descriptor
 *		then the reference count is incremented for as long as the slot holds
 *		a reference.
 * --------------------------------
 */
void ExecSetSlotDescriptor(TupleTableSlot* slot, /* slot to change */
    TupleDesc tup_desc)                           /* new tuple descriptor */
{
    /* For safety, make sure slot is empty before changing it */
    (void)ExecClearTuple(slot);

    /*
     * Release any old descriptor.	Also release old Datum/isnull arrays if
     * present (we don't bother to check if they could be re-used).
     */
    if (slot->tts_tupleDescriptor != NULL) {
        ReleaseTupleDesc(slot->tts_tupleDescriptor);
    }
#ifdef PGXC
    /* XXX there in no routine to release AttInMetadata instance */
    if (slot->tts_attinmeta != NULL) {
        slot->tts_attinmeta = NULL;
    }
#endif

    if (slot->tts_values != NULL) {
        pfree_ext(slot->tts_values);
    }
    if (slot->tts_isnull != NULL) {
        pfree_ext(slot->tts_isnull);
    }
    pfree_ext(slot->tts_lobPointers);
    /*
     * Install the new descriptor; if it's refcounted, bump its refcount.
     */
    slot->tts_tupleDescriptor = tup_desc;
    PinTupleDesc(tup_desc);

    /*
     * Allocate Datum/isnull arrays of the appropriate size.  These must have
     * the same lifetime as the slot, so allocate in the slot's own context.
     */
    slot->tts_values = (Datum*)MemoryContextAlloc(slot->tts_mcxt, tup_desc->natts * sizeof(Datum));
    slot->tts_isnull = (bool*)MemoryContextAlloc(slot->tts_mcxt, tup_desc->natts * sizeof(bool));
    slot->tts_lobPointers = (Datum*)MemoryContextAlloc(slot->tts_mcxt, tup_desc->natts * sizeof(Datum));
}

/* --------------------------------
 *		ExecStoreTuple
 *
 *		This function is used to store a physical tuple into a specified
 *		slot in the tuple table.
 *
 *		tuple:	tuple to store
 *		slot:	slot to store it in
 *		buffer: disk buffer if tuple is in a disk page, else InvalidBuffer
 *		shouldFree: true if ExecClearTuple should pfree_ext() the tuple
 *					when done with it
 *
 * If 'buffer' is not InvalidBuffer, the tuple table code acquires a pin
 * on the buffer which is held until the slot is cleared, so that the tuple
 * won't go away on us.
 *
 * shouldFree is normally set 'true' for tuples constructed on-the-fly.
 * It must always be 'false' for tuples that are stored in disk pages,
 * since we don't want to try to pfree those.
 *
 * Another case where it is 'false' is when the referenced tuple is held
 * in a tuple table slot belonging to a lower-level executor Proc node.
 * In this case the lower-level slot retains ownership and responsibility
 * for eventually releasing the tuple.	When this method is used, we must
 * be certain that the upper-level Proc node will lose interest in the tuple
 * sooner than the lower-level one does!  If you're not certain, copy the
 * lower-level tuple with heap_copytuple and let the upper-level table
 * slot assume ownership of the copy!
 *
 * Return value is just the passed-in slot pointer.
 *
 * NOTE: before PostgreSQL 8.1, this function would accept a NULL tuple
 * pointer and effectively behave like ExecClearTuple (though you could
 * still specify a buffer to pin, which would be an odd combination).
 * This saved a couple lines of code in a few places, but seemed more likely
 * to mask logic errors than to be really useful, so it's now disallowed.
 * --------------------------------
 */
TupleTableSlot* ExecStoreTuple(Tuple tuple, TupleTableSlot* slot, Buffer buffer, bool should_free)
{
    /*
     * sanity checks
     */
    Assert(tuple != NULL);
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);

    HeapTuple htup = (HeapTuple)tuple;
    if (slot->tts_tupslotTableAm == TAM_USTORE && htup->tupTableType == HEAP_TUPLE) {
        tuple = (Tuple)HeapToUHeap(slot->tts_tupleDescriptor, (HeapTuple)tuple);
    } else if (slot->tts_tupslotTableAm == TAM_HEAP && htup->tupTableType == UHEAP_TUPLE) {
        tuple = (Tuple)UHeapToHeap(slot->tts_tupleDescriptor, (UHeapTuple)tuple);
    }

    tableam_tslot_store_tuple(tuple, slot, buffer, should_free, false);

    return slot;
}

/* --------------------------------
 *		ExecStoreMinimalTuple
 *
 *		Like ExecStoreTuple, but insert a "minimal" tuple into the slot.
 *
 * No 'buffer' parameter since minimal tuples are never stored in relations.
 * --------------------------------
 */
TupleTableSlot* ExecStoreMinimalTuple(MinimalTuple mtup, TupleTableSlot* slot, bool should_free)
{
    /*
     * sanity checks
     */
    Assert(mtup != NULL);
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);

    /*
     * store the minimal tuple in the slot.
     */
    tableam_tslot_store_minimal_tuple(mtup, slot, should_free);

    return slot;
}

/* --------------------------------
 *		ExecClearTuple
 *
 *		This function is used to clear out a slot in the tuple table.
 *
 *		NB: only the tuple is cleared, not the tuple descriptor (if any).
 * --------------------------------
 */
TupleTableSlot* ExecClearTuple(TupleTableSlot* slot) /* return: slot passed slot in which to store tuple */
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);

    /*
     * clear the physical tuple or minimal tuple if present via TableAm.
     */
    if (slot->tts_shouldFree || slot->tts_shouldFreeMin) {
    	Assert(slot->tts_tupleDescriptor != NULL);
    	tableam_tslot_clear(slot);
    }

    /* 
     * tts_tuple may still be valid if tts_shouldFree is false, Original caller doesn't want this slot to free the tuple.
     */
    slot->tts_tuple = NULL;
    slot->tts_mintuple = NULL;
    slot->tts_shouldFree = false;
    slot->tts_shouldFreeMin = false;

#ifdef ENABLE_MULTIPLE_NODES
    if (slot->tts_shouldFreeRow) {
        pfree_ext(slot->tts_dataRow);
    }
    slot->tts_shouldFreeRow = false;
    slot->tts_dataRow = NULL;
    slot->tts_dataLen = -1;
    slot->tts_xcnodeoid = 0;
#endif

    /*
     * Drop the pin on the referenced buffer, if there is one.
     */
    if (BufferIsValid(slot->tts_buffer)) {
        ReleaseBuffer(slot->tts_buffer);
    }
    slot->tts_buffer = InvalidBuffer;

    /*
     * Mark it empty.
     */
    slot->tts_isempty = true;
    slot->tts_nvalid = 0;

    // Row uncompression use slot->tts_per_tuple_mcxt in some case, So we need
    // reset memory context. This memory context is introduced by PGXC and it only used
    // in function 'slot_deform_datarow'.  PGXC also do reset in function 'FetchTuple'.
    // So it is safe
    //
    ResetSlotPerTupleContext(slot);
    return slot;
}

/* --------------------------------
 *		ExecStoreVirtualTuple
 *			Mark a slot as containing a virtual tuple.
 *
 * The protocol for loading a slot with virtual tuple data is:
 *		* Call ExecClearTuple to mark the slot empty.
 *		* Store data into the Datum/isnull arrays.
 *		* Call ExecStoreVirtualTuple to mark the slot valid.
 * This is a bit unclean but it avoids one round of data copying.
 * --------------------------------
 */
TupleTableSlot* ExecStoreVirtualTuple(TupleTableSlot* slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);
    Assert(slot->tts_isempty);

    slot->tts_isempty = false;
    slot->tts_nvalid = slot->tts_tupleDescriptor->natts;

    if (slot->tts_tupslotTableAm != slot->tts_tupleDescriptor->tdTableAmType) {
        // XXX: Should tts_tupleDescriptor be cloned before changing its contents
        // as some time it can be direct reference to the rd_att in RelationData.
        slot->tts_tupleDescriptor->tdTableAmType = slot->tts_tupslotTableAm;
    }

    return slot;
}

/* --------------------------------
 *		ExecStoreAllNullTuple
 *			Set up the slot to contain a null in every column.
 *
 * At first glance this might sound just like ExecClearTuple, but it's
 * entirely different: the slot ends up full, not empty.
 * --------------------------------
 */
TupleTableSlot* ExecStoreAllNullTuple(TupleTableSlot* slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);

    /* Clear any old contents */
    (void)ExecClearTuple(slot);

    /*
     * Fill all the columns of the virtual tuple with nulls
     */
    errno_t rc = EOK;

    rc = memset_s(slot->tts_values,
        slot->tts_tupleDescriptor->natts * sizeof(Datum),
        0,
        slot->tts_tupleDescriptor->natts * sizeof(Datum));
    securec_check(rc, "\0", "\0");
    rc = memset_s(slot->tts_isnull,
        slot->tts_tupleDescriptor->natts * sizeof(bool),
        true,
        slot->tts_tupleDescriptor->natts * sizeof(bool));
    securec_check(rc, "\0", "\0");

    return ExecStoreVirtualTuple(slot);
}

/* --------------------------------
 *		ExecCopySlotTuple
 *			Obtain a copy of a slot's regular physical tuple.  The copy is
 *			palloc'd in the current memory context.
 *			The slot itself is undisturbed.
 *
 *		This works even if the slot contains a virtual or minimal tuple;
 *		however the "system columns" of the result will not be meaningful.
 * --------------------------------
 */
HeapTuple ExecCopySlotTuple(TupleTableSlot* slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);

    return tableam_tslot_copy_heap_tuple(slot);
}

/* --------------------------------
 *		ExecCopySlotMinimalTuple
 *			Obtain a copy of a slot's minimal physical tuple.  The copy is
 *			palloc'd in the current memory context.
 *			The slot itself is undisturbed.
 * --------------------------------
 */
MinimalTuple ExecCopySlotMinimalTuple(TupleTableSlot* slot, bool need_transform_anyarray)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);

    return tableam_tslot_copy_minimal_tuple(slot);
}

/* --------------------------------
 *		ExecFetchSlotTuple
 *			Fetch the slot's regular physical tuple.
 *
 *		If the slot contains a virtual tuple, we convert it to physical
 *		form.  The slot retains ownership of the physical tuple.
 *		If it contains a minimal tuple we convert to regular form and store
 *		that in addition to the minimal tuple (not instead of, because
 *		callers may hold pointers to Datums within the minimal tuple).
 *
 * The main difference between this and ExecMaterializeSlot() is that this
 * does not guarantee that the contained tuple is local storage.
 * Hence, the result must be treated as read-only.
 * --------------------------------
 */
HeapTuple ExecFetchSlotTuple(TupleTableSlot* slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);

    return tableam_tslot_get_heap_tuple(slot);
}

/* --------------------------------
 *		ExecFetchSlotMinimalTuple
 *			Fetch the slot's minimal physical tuple.
 *
 *		If the slot contains a virtual tuple, we convert it to minimal
 *		physical form.	The slot retains ownership of the minimal tuple.
 *		If it contains a regular tuple we convert to minimal form and store
 *		that in addition to the regular tuple (not instead of, because
 *		callers may hold pointers to Datums within the regular tuple).
 *
 * As above, the result must be treated as read-only.
 * --------------------------------
 */
MinimalTuple ExecFetchSlotMinimalTuple(TupleTableSlot* slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);

    return tableam_tslot_get_minimal_tuple(slot);
}

/* --------------------------------
 *		ExecFetchSlotTupleDatum
 *			Fetch the slot's tuple as a composite-type Datum.
 *
 *		We convert the slot's contents to local physical-tuple form,
 *		and fill in the Datum header fields.  Note that the result
 *		always points to storage owned by the slot.
 * --------------------------------
 */
Datum ExecFetchSlotTupleDatum(TupleTableSlot* slot)
{
    HeapTuple tup;
    HeapTupleHeader td;
    TupleDesc tup_desc;

    /* Make sure we can scribble on the slot contents ... */
    tup = ExecMaterializeSlot(slot);
    /* ... and set up the composite-Datum header fields, in case not done */
    td = tup->t_data;
    tup_desc = slot->tts_tupleDescriptor;
    HeapTupleHeaderSetDatumLength(td, tup->t_len);
    HeapTupleHeaderSetTypeId(td, tup_desc->tdtypeid);
    HeapTupleHeaderSetTypMod(td, tup_desc->tdtypmod);
    return PointerGetDatum(td);
}

/* --------------------------------
 *		ExecMaterializeSlot
 *			Force a slot into the "materialized" state.
 *
 *		This causes the slot's tuple to be a local copy not dependent on
 *		any external storage.  A pointer to the contained tuple is returned.
 *
 *		A typical use for this operation is to prepare a computed tuple
 *		for being stored on disk.  The original data may or may not be
 *		virtual, but in any case we need a private copy for heap_insert
 *		to scribble on.
 * --------------------------------
 */
HeapTuple ExecMaterializeSlot(TupleTableSlot* slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);

    return tableam_tslot_materialize(slot);
}

/* --------------------------------
 *		ExecCopySlot
 *			Copy the source slot's contents into the destination slot.
 *
 *		The destination acquires a private copy that will not go away
 *		if the source is cleared.
 *
 *		The caller must ensure the slots have compatible tupdescs.
 * --------------------------------
 */
TupleTableSlot* ExecCopySlot(TupleTableSlot* dst_slot, TupleTableSlot* src_slot)
{
    HeapTuple new_tuple;
    MemoryContext old_context;

    /*
     * There might be ways to optimize this when the source is virtual, but
     * for now just always build a physical copy.  Make sure it is in the
     * right context.
     */
    old_context = MemoryContextSwitchTo(dst_slot->tts_mcxt);
    new_tuple = ExecCopySlotTuple(src_slot);
    MemoryContextSwitchTo(old_context);

    return ExecStoreTuple(new_tuple, dst_slot, InvalidBuffer, true);
}

/* ----------------------------------------------------------------
 *				convenience initialization routines
 * ----------------------------------------------------------------
 */
/* --------------------------------
 *		ExecInit{Result,Scan,Extra}TupleSlot
 *
 *		These are convenience routines to initialize the specified slot
 *		in nodes inheriting the appropriate state.	ExecInitExtraTupleSlot
 *		is used for initializing special-purpose slots.
 * --------------------------------
 */
/* ----------------
 *		ExecInitResultTupleSlot
 * ----------------
 */
void ExecInitResultTupleSlot(EState* estate, PlanState* plan_state, TableAmType tam)
{
    plan_state->ps_ResultTupleSlot = ExecAllocTableSlot(&estate->es_tupleTable, tam);
}

/* ----------------
 *		ExecInitScanTupleSlot
 * ----------------
 */
void ExecInitScanTupleSlot(EState* estate, ScanState* scan_state, TableAmType tam)
{
    scan_state->ss_ScanTupleSlot = ExecAllocTableSlot(&estate->es_tupleTable, tam);
}

/* ----------------
 *		ExecInitExtraTupleSlot
 * ----------------
 */
TupleTableSlot* ExecInitExtraTupleSlot(EState* estate, TableAmType tam)
{
    return ExecAllocTableSlot(&estate->es_tupleTable, tam);
}

/* ----------------
 *		ExecInitNullTupleSlot
 *
 * Build a slot containing an all-nulls tuple of the given type.
 * This is used as a substitute for an input tuple when performing an
 * outer join.
 * ----------------
 */
TupleTableSlot* ExecInitNullTupleSlot(EState* estate, TupleDesc tup_type)
{
    TupleTableSlot* slot = ExecInitExtraTupleSlot(estate);

    ExecSetSlotDescriptor(slot, tup_type);

    return ExecStoreAllNullTuple(slot);
}

/* ----------------------------------------------------------------
 *		ExecTypeFromTL
 *
 *		Generate a tuple descriptor for the result tuple of a targetlist.
 *		(A parse/plan tlist must be passed, not an ExprState tlist.)
 *		Note that resjunk columns, if any, are included in the result.
 *
 *		Currently there are about 4 different places where we create
 *		TupleDescriptors.  They should all be merged, or perhaps
 *		be rewritten to call BuildDesc().
 * ----------------------------------------------------------------
 */
TupleDesc ExecTypeFromTL(List* target_list, bool has_oid, bool mark_dropped, TableAmType tam)
{
    return ExecTypeFromTLInternal(target_list, has_oid, false, mark_dropped, tam);
}

/* ----------------------------------------------------------------
 *		ExecCleanTypeFromTL
 *
 *		Same as above, but resjunk columns are omitted from the result.
 * ----------------------------------------------------------------
 */
TupleDesc ExecCleanTypeFromTL(List* target_list, bool has_oid, TableAmType tam)
{
    return ExecTypeFromTLInternal(target_list, has_oid, true, false, tam);
}

static TupleDesc ExecTypeFromTLInternal(List* target_list, bool has_oid, bool skip_junk, bool mark_dropped,  TableAmType tam)
{
    TupleDesc type_info;
    ListCell* l = NULL;
    int len;
    int cur_resno = 1;

    if (skip_junk)
        len = ExecCleanTargetListLength(target_list);
    else
        len = ExecTargetListLength(target_list);
    type_info = CreateTemplateTupleDesc(len, has_oid, tam);

    foreach (l, target_list) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        if (skip_junk && tle->resjunk)
            continue;

        TupleDescInitEntry(
            type_info, cur_resno, tle->resname, exprType((Node*)tle->expr), exprTypmod((Node*)tle->expr), 0);
        TupleDescInitEntryCollation(type_info, cur_resno, exprCollation((Node*)tle->expr));

        /* mark dropped column, maybe we can find another way some day */
        if (mark_dropped && strstr(tle->resname, "........pg.dropped.")) {
            type_info->attrs[cur_resno - 1]->attisdropped = true;
        }

        cur_resno++;
    }

    return type_info;
}

/*
 * ExecTypeFromExprList - build a tuple descriptor from a list of Exprs
 *
 * Caller must also supply a list of field names (String nodes).
 */
TupleDesc ExecTypeFromExprList(List* expr_list, List* names_list,  TableAmType tam)
{
    TupleDesc type_info;
    ListCell* le = NULL;
    ListCell* ln = NULL;
    int cur_resno = 1;

    Assert(list_length(expr_list) == list_length(names_list));

    type_info = CreateTemplateTupleDesc(list_length(expr_list), false, tam);

    forboth(le, expr_list, ln, names_list)
    {
        Node* e = (Node*)lfirst(le);
        char* n = strVal(lfirst(ln));

        TupleDescInitEntry(type_info, cur_resno, n, exprType(e), exprTypmod(e), 0);
        TupleDescInitEntryCollation(type_info, cur_resno, exprCollation(e));
        cur_resno++;
    }

    return type_info;
}

/*
 * BlessTupleDesc - make a completed tuple descriptor useful for SRFs
 *
 * Rowtype Datums returned by a function must contain valid type information.
 * This happens "for free" if the tupdesc came from a relcache entry, but
 * not if we have manufactured a tupdesc for a transient RECORD datatype.
 * In that case we have to notify typcache.c of the existence of the type.
 */
TupleDesc BlessTupleDesc(TupleDesc tup_desc)
{
    if (tup_desc->tdtypeid == RECORDOID && tup_desc->tdtypmod < 0) {
        assign_record_type_typmod(tup_desc);
    }
    return tup_desc; /* just for notational convenience */
}

/*
 * TupleDescGetSlot - Initialize a slot based on the supplied tupledesc
 *
 * Note: this is obsolete; it is sufficient to call BlessTupleDesc on
 * the tupdesc.  We keep it around just for backwards compatibility with
 * existing user-written SRFs.
 */
TupleTableSlot* TupleDescGetSlot(TupleDesc tup_desc)
{
    TupleTableSlot* slot = NULL;

    /* The useful work is here */
    BlessTupleDesc(tup_desc);

    /* Make a standalone slot */
    slot = MakeSingleTupleTableSlot(tup_desc);

    /* Return the slot */
    return slot;
}

/*
 * TupleDescGetAttInMetadata - Build an AttInMetadata structure based on the
 * supplied TupleDesc. AttInMetadata can be used in conjunction with C strings
 * to produce a properly formed tuple.
 */
AttInMetadata* TupleDescGetAttInMetadata(TupleDesc tup_desc)
{
    int natts = tup_desc->natts;
    int i;
    Oid att_type_id;
    Oid att_in_func_id;
    FmgrInfo* att_in_func_info = NULL;
    Oid* att_io_params = NULL;
    int32* att_typ_mods = NULL;
    AttInMetadata* att_in_meta = NULL;

    att_in_meta = (AttInMetadata*)palloc(sizeof(AttInMetadata));

    /* "Bless" the tupledesc so that we can make rowtype datums with it */
    att_in_meta->tupdesc = BlessTupleDesc(tup_desc);

    /*
     * Gather info needed later to call the "in" function for each attribute
     */
    att_in_func_info = (FmgrInfo*)palloc0(natts * sizeof(FmgrInfo));
    att_io_params = (Oid*)palloc0(natts * sizeof(Oid));
    att_typ_mods = (int32*)palloc0(natts * sizeof(int32));

    for (i = 0; i < natts; i++) {
        /* Ignore dropped attributes */
        if (!tup_desc->attrs[i]->attisdropped) {
            att_type_id = tup_desc->attrs[i]->atttypid;
            getTypeInputInfo(att_type_id, &att_in_func_id, &att_io_params[i]);
            fmgr_info(att_in_func_id, &att_in_func_info[i]);
            att_typ_mods[i] = tup_desc->attrs[i]->atttypmod;
        }
    }
    att_in_meta->attinfuncs = att_in_func_info;
    att_in_meta->attioparams = att_io_params;
    att_in_meta->atttypmods = att_typ_mods;

    return att_in_meta;
}

/*
 * BuildTupleFromCStrings - build a HeapTuple given user data in C string form.
 * values is an array of C strings, one for each attribute of the return tuple.
 * A NULL string pointer indicates we want to create a NULL field.
 */
HeapTuple BuildTupleFromCStrings(AttInMetadata* att_in_meta, char** values)
{
    TupleDesc tup_desc = att_in_meta->tupdesc;
    int natts = tup_desc->natts;
    Datum* d_values = NULL;
    bool* nulls = NULL;
    int i;
    HeapTuple tuple;

    d_values = (Datum*)palloc(natts * sizeof(Datum));
    nulls = (bool*)palloc(natts * sizeof(bool));

    /* Call the "in" function for each non-dropped attribute */
    for (i = 0; i < natts; i++) {
        if (!tup_desc->attrs[i]->attisdropped) {
            /* Non-dropped attributes */
            d_values[i] = InputFunctionCall(
                &att_in_meta->attinfuncs[i], values[i], att_in_meta->attioparams[i], att_in_meta->atttypmods[i]);

            nulls[i] = (values[i] == NULL);
        } else {
            /* Handle dropped attributes by setting to NULL */
            d_values[i] = (Datum)0;
            nulls[i] = true;
        }
    }

    /*
     * Form a tuple
     */
    tuple = (HeapTuple)tableam_tops_form_tuple(tup_desc, d_values, nulls, HEAP_TUPLE);

    /*
     * Release locally palloc'd space.  XXX would probably be good to pfree
     * values of pass-by-reference datums, as well.
     */
    pfree_ext(d_values);
    pfree_ext(nulls);

    return tuple;
}

/*
 * Functions for sending tuples to the frontend (or other specified destination)
 * as though it is a SELECT result. These are used by utility commands that
 * need to project directly to the destination and don't need or want full
 * table function capability. Currently used by EXPLAIN and SHOW ALL.
 */
TupOutputState* begin_tup_output_tupdesc(DestReceiver* dest, TupleDesc tup_desc)
{
    TupOutputState* tstate = NULL;

    tstate = (TupOutputState*)palloc(sizeof(TupOutputState));

    tstate->slot = MakeSingleTupleTableSlot(tup_desc);
    tstate->dest = dest;

    (*tstate->dest->rStartup)(tstate->dest, (int)CMD_SELECT, tup_desc);

    return tstate;
}

/*
 * write a single tuple
 */
void do_tup_output(TupOutputState* tstate, Datum* values, size_t values_len, const bool* is_null, size_t is_null_len)
{
    Assert(values != NULL);
    Assert(values_len != 0);
    Assert(is_null != NULL);
    Assert(is_null_len != 0);
    TupleTableSlot* slot = tstate->slot;
    int natts = slot->tts_tupleDescriptor->natts;
    errno_t rc = EOK;

    /* make sure the slot is clear */
    (void)ExecClearTuple(slot);

    /* insert data */
    rc = memcpy_s(slot->tts_values, natts * sizeof(Datum), values, natts * sizeof(Datum));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(slot->tts_isnull, natts * sizeof(bool), is_null, natts * sizeof(bool));
    securec_check(rc, "\0", "\0");

    /* mark slot as containing a virtual tuple */
    ExecStoreVirtualTuple(slot);

    /* send the tuple to the receiver */
    (*tstate->dest->receiveSlot)(slot, tstate->dest);

    /* clean up */
    (void)ExecClearTuple(slot);
}

/*
 * write a chunk of text, breaking at newline characters
 *
 * Should only be used with a single-TEXT-attribute tupdesc.
 */
int do_text_output_multiline(TupOutputState* tstate, char* text)
{
    Datum values[1];
    bool is_null[1] = {false};
    int tuple_count = 0;

    while (*text) {
        char* eol = NULL;
        int len;

        eol = strchr(text, '\n');
        if (eol != NULL) {
            len = eol - text;
            eol++;
        } else {
            len = strlen(text);
            eol = text;
            eol += len;
        }

        values[0] = PointerGetDatum(cstring_to_text_with_len(text, len));
        do_tup_output(tstate, values, 1, is_null, 1);
        tuple_count++;
        pfree(DatumGetPointer(values[0]));
        text = eol;
    }
    return tuple_count;
}

void end_tup_output(TupOutputState* tstate)
{
    (*tstate->dest->rShutdown)(tstate->dest);
    /* note that destroying the dest is not ours to do */
    ExecDropSingleTupleTableSlot(tstate->slot);
    pfree_ext(tstate);
}

#ifdef PGXC
/* --------------------------------
 *		ExecStoreDataRowTuple
 *
 *		Store a buffer in DataRow message format into the slot.
 *
 * --------------------------------
 */
TupleTableSlot* ExecStoreDataRowTuple(char* msg, size_t len, Oid msgnode_oid, TupleTableSlot* slot, bool should_free)
{
    /*
     * sanity checks
     */
    Assert(msg != NULL);
    Assert(len > 0);
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);

    /*
     * Free any old physical tuple belonging to the slot.
     */
    if (slot->tts_shouldFree && (HeapTuple)slot->tts_tuple != NULL) {
        heap_freetuple((HeapTuple)slot->tts_tuple);
        slot->tts_tuple = NULL;
        slot->tts_shouldFree = false;
    }
    if (slot->tts_shouldFreeMin) {
        heap_free_minimal_tuple(slot->tts_mintuple);
    }
    /*
     * if msg == slot->tts_dataRow then we would
     * free the dataRow in the slot loosing the contents in msg. It is safe
     * to reset shouldFreeRow, since it will be overwritten just below.
     */
    if (msg == slot->tts_dataRow) {
        slot->tts_shouldFreeRow = false;
    }
    if (slot->tts_shouldFreeRow) {
        pfree_ext(slot->tts_dataRow);
    }
    ResetSlotPerTupleContext(slot);

    /*
     * Drop the pin on the referenced buffer, if there is one.
     */
    if (BufferIsValid(slot->tts_buffer)) {
        ReleaseBuffer(slot->tts_buffer);
    }
    slot->tts_buffer = InvalidBuffer;

    /*
     * Store the new tuple into the specified slot.
     */
    slot->tts_isempty = false;
    slot->tts_shouldFree = false;
    slot->tts_shouldFreeMin = false;
    slot->tts_shouldFreeRow = should_free;
    slot->tts_tuple = NULL;
    slot->tts_mintuple = NULL;
    slot->tts_dataRow = msg;
    slot->tts_dataLen = len;
    slot->tts_xcnodeoid = msgnode_oid;
    /* Mark extracted state invalid */
    slot->tts_nvalid = 0;

    return slot;
}
#endif

#ifdef GS_GRAPH
/*
 * TupleTableSlotOps implementation for VirtualTupleTableSlot.
 */
static void
tts_virtual_init(TupleTableSlot *slot)
{
}

static void
tts_virtual_release(TupleTableSlot *slot)
{
}

static void
tts_virtual_clear(TupleTableSlot *slot)
{
	if (unlikely(TTS_SHOULDFREE(slot)))
	{
		VirtualTupleTableSlot *vslot = (VirtualTupleTableSlot *) slot;

		pfree(vslot->data);
		vslot->data = NULL;

		slot->tts_isempty &= ~TTS_FLAG_SHOULDFREE;
	}

	slot->tts_nvalid = 0;
	slot->tts_isempty |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
}

/*
 * Attribute values are readily available in tts_values and tts_isnull array
 * in a VirtualTupleTableSlot. So there should be no need to call either of the
 * following two functions.
 */
static void
tts_virtual_getsomeattrs(TupleTableSlot *slot, int natts)
{
	elog(ERROR, "getsomeattrs is not required to be called on a virtual tuple table slot");
}

static Datum
tts_virtual_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	elog(ERROR, "virtual tuple table slot does not have system attributes");

	return 0;					/* silence compiler warnings */
}

/*
 * To materialize a virtual slot all the datums that aren't passed by value
 * have to be copied into the slot's memory context.  To do so, compute the
 * required size, and allocate enough memory to store all attributes.  That's
 * good for cache hit ratio, but more importantly requires only memory
 * allocation/deallocation.
 */
static void
tts_virtual_materialize(TupleTableSlot *slot)
{
	VirtualTupleTableSlot *vslot = (VirtualTupleTableSlot *) slot;
	TupleDesc	desc = slot->tts_tupleDescriptor;
	Size		sz = 0;
	char	   *data;

	/* already materialized */
	if (TTS_SHOULDFREE(slot))
		return;

	/* compute size of memory required */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			sz = att_align_nominal(sz, att->attalign);
			sz += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			sz = att_align_nominal(sz, att->attalign);
			sz = att_addlength_datum(sz, att->attlen, val);
		}
	}

	/* all data is byval */
	if (sz == 0)
		return;

	/* allocate memory */
	vslot->data = data = (char*)MemoryContextAlloc(slot->tts_mcxt, sz);
	slot->tts_isempty |= TTS_FLAG_SHOULDFREE;

	/* and copy all attributes into the pre-allocated space */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			Size		data_length;

			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			ExpandedObjectHeader *eoh = DatumGetEOHP(val);

			data = (char *) att_align_nominal(data,
											  att->attalign);
			data_length = EOH_get_flat_size(eoh);
			EOH_flatten_into(eoh, data, data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
		else
		{
			Size		data_length = 0;

			data = (char *) att_align_nominal(data, att->attalign);
			data_length = att_addlength_datum(data_length, att->attlen, val);

			memcpy(data, DatumGetPointer(val), data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
	}
}

static void
tts_virtual_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	TupleDesc	srcdesc = srcslot->tts_tupleDescriptor;

	Assert(srcdesc->natts <= dstslot->tts_tupleDescriptor->natts);

	tts_virtual_clear(dstslot);

	slot_getallattrs(srcslot);

	for (int natt = 0; natt < srcdesc->natts; natt++)
	{
		dstslot->tts_values[natt] = srcslot->tts_values[natt];
		dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt];
	}

	dstslot->tts_nvalid = srcdesc->natts;
	dstslot->tts_isempty &= ~TTS_FLAG_EMPTY;

	/* make sure storage doesn't depend on external memory */
	tts_virtual_materialize(dstslot);
}

static HeapTuple
tts_virtual_copy_heap_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	return heap_form_tuple(slot->tts_tupleDescriptor,
						   slot->tts_values,
						   slot->tts_isnull);
}

static MinimalTuple
tts_virtual_copy_minimal_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
								   slot->tts_values,
								   slot->tts_isnull);
}


/*
 * TupleTableSlotOps implementation for HeapTupleTableSlot.
 */

static void
tts_heap_init(TupleTableSlot *slot)
{
}

static void
tts_heap_release(TupleTableSlot *slot)
{
}

static void
tts_heap_clear(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	/* Free the memory for the heap tuple if it's allowed. */
	if (TTS_SHOULDFREE(slot))
	{
		heap_freetuple(hslot->tuple);
		slot->tts_isempty &= ~TTS_FLAG_SHOULDFREE;
	}

	slot->tts_nvalid = 0;
	slot->tts_isempty |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
	hslot->off = 0;
	hslot->tuple = NULL;
}

static void
tts_heap_getsomeattrs(TupleTableSlot *slot, int natts)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	slot_deform_heap_tuple(slot, hslot->tuple, &hslot->off, natts);
}

static Datum
tts_heap_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	return heap_getsysattr(hslot->tuple, attnum,
						   slot->tts_tupleDescriptor, isnull);
}

static void
tts_heap_materialize(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;
	MemoryContext oldContext;

	Assert(!TTS_EMPTY(slot));

	/* If slot has its tuple already materialized, nothing to do. */
	if (TTS_SHOULDFREE(slot))
		return;

	oldContext = MemoryContextSwitchTo(slot->tts_mcxt);

	/*
	 * Have to deform from scratch, otherwise tts_values[] entries could point
	 * into the non-materialized tuple (which might be gone when accessed).
	 */
	slot->tts_nvalid = 0;
	hslot->off = 0;

	if (!hslot->tuple)
		hslot->tuple = heap_form_tuple(slot->tts_tupleDescriptor,
									   slot->tts_values,
									   slot->tts_isnull);
	else
	{
		/*
		 * The tuple contained in this slot is not allocated in the memory
		 * context of the given slot (else it would have TTS_SHOULDFREE set).
		 * Copy the tuple into the given slot's memory context.
		 */
		hslot->tuple = heap_copytuple(hslot->tuple);
	}

	slot->tts_isempty |= TTS_FLAG_SHOULDFREE;

	MemoryContextSwitchTo(oldContext);
}

static void
tts_heap_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	HeapTuple	tuple;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(dstslot->tts_mcxt);
	tuple = ExecCopySlotHeapTuple(srcslot);
	MemoryContextSwitchTo(oldcontext);

	ExecStoreHeapTuple(tuple, dstslot, true);
}

static HeapTuple
tts_heap_get_heap_tuple(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));
	if (!hslot->tuple)
		tts_heap_materialize(slot);

	return hslot->tuple;
}

static HeapTuple
tts_heap_copy_heap_tuple(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));
	if (!hslot->tuple)
		tts_heap_materialize(slot);

	return heap_copytuple(hslot->tuple);
}

static MinimalTuple
tts_heap_copy_minimal_tuple(TupleTableSlot *slot)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	if (!hslot->tuple)
		tts_heap_materialize(slot);

	return minimal_tuple_from_heap_tuple(hslot->tuple);
}

static void
tts_heap_store_tuple(TupleTableSlot *slot, HeapTuple tuple, bool shouldFree)
{
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

	tts_heap_clear(slot);

	slot->tts_nvalid = 0;
	hslot->tuple = tuple;
	hslot->off = 0;
	slot->tts_isempty &= ~(TTS_FLAG_EMPTY | TTS_FLAG_SHOULDFREE);
	slot->tts_tid = tuple->t_self;

	if (shouldFree)
		slot->tts_isempty |= TTS_FLAG_SHOULDFREE;
}


/*
 * TupleTableSlotOps implementation for MinimalTupleTableSlot.
 */

static void
tts_minimal_init(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	/*
	 * Initialize the heap tuple pointer to access attributes of the minimal
	 * tuple contained in the slot as if its a heap tuple.
	 */
	mslot->tuple = &mslot->minhdr;
}

static void
tts_minimal_release(TupleTableSlot *slot)
{
}

static void
tts_minimal_clear(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	if (TTS_SHOULDFREE(slot))
	{
		heap_free_minimal_tuple(mslot->mintuple);
		slot->tts_isempty &= ~TTS_FLAG_SHOULDFREE;
	}

	slot->tts_nvalid = 0;
	slot->tts_isempty |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
	mslot->off = 0;
	mslot->mintuple = NULL;
}

static void
tts_minimal_getsomeattrs(TupleTableSlot *slot, int natts)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	slot_deform_heap_tuple(slot, mslot->tuple, &mslot->off, natts);
}

static Datum
tts_minimal_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	elog(ERROR, "minimal tuple table slot does not have system attributes");

	return 0;					/* silence compiler warnings */
}

static void
tts_minimal_materialize(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;
	MemoryContext oldContext;

	Assert(!TTS_EMPTY(slot));

	/* If slot has its tuple already materialized, nothing to do. */
	if (TTS_SHOULDFREE(slot))
		return;

	oldContext = MemoryContextSwitchTo(slot->tts_mcxt);

	/*
	 * Have to deform from scratch, otherwise tts_values[] entries could point
	 * into the non-materialized tuple (which might be gone when accessed).
	 */
	slot->tts_nvalid = 0;
	mslot->off = 0;

	if (!mslot->mintuple)
	{
		mslot->mintuple = heap_form_minimal_tuple(slot->tts_tupleDescriptor,
												  slot->tts_values,
												  slot->tts_isnull);
	}
	else
	{
		/*
		 * The minimal tuple contained in this slot is not allocated in the
		 * memory context of the given slot (else it would have TTS_SHOULDFREE
		 * set).  Copy the minimal tuple into the given slot's memory context.
		 */
		mslot->mintuple = heap_copy_minimal_tuple(mslot->mintuple);
	}

	slot->tts_isempty |= TTS_FLAG_SHOULDFREE;

	Assert(mslot->tuple == &mslot->minhdr);

	mslot->minhdr.t_len = mslot->mintuple->t_len + MINIMAL_TUPLE_OFFSET;
	mslot->minhdr.t_data = (HeapTupleHeader) ((char *) mslot->mintuple - MINIMAL_TUPLE_OFFSET);

	MemoryContextSwitchTo(oldContext);
}

static void
tts_minimal_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	MemoryContext oldcontext;
	MinimalTuple mintuple;

	oldcontext = MemoryContextSwitchTo(dstslot->tts_mcxt);
	mintuple = ExecCopySlotMinimalTuple(srcslot);
	MemoryContextSwitchTo(oldcontext);

	ExecStoreMinimalTuple(mintuple, dstslot, true);
}

static MinimalTuple
tts_minimal_get_minimal_tuple(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	if (!mslot->mintuple)
		tts_minimal_materialize(slot);

	return mslot->mintuple;
}

static HeapTuple
tts_minimal_copy_heap_tuple(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	if (!mslot->mintuple)
		tts_minimal_materialize(slot);

	return heap_tuple_from_minimal_tuple(mslot->mintuple);
}

static MinimalTuple
tts_minimal_copy_minimal_tuple(TupleTableSlot *slot)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	if (!mslot->mintuple)
		tts_minimal_materialize(slot);

	return heap_copy_minimal_tuple(mslot->mintuple);
}

static void
tts_minimal_store_tuple(TupleTableSlot *slot, MinimalTuple mtup, bool shouldFree)
{
	MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

	tts_minimal_clear(slot);

	Assert(!TTS_SHOULDFREE(slot));
	Assert(TTS_EMPTY(slot));

	slot->tts_isempty &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;
	mslot->off = 0;

	mslot->mintuple = mtup;
	Assert(mslot->tuple == &mslot->minhdr);
	mslot->minhdr.t_len = mtup->t_len + MINIMAL_TUPLE_OFFSET;
	mslot->minhdr.t_data = (HeapTupleHeader) ((char *) mtup - MINIMAL_TUPLE_OFFSET);
	/* no need to set t_self or t_tableOid since we won't allow access */

	if (shouldFree)
		slot->tts_isempty |= TTS_FLAG_SHOULDFREE;
}


/*
 * TupleTableSlotOps implementation for BufferHeapTupleTableSlot.
 */

static void
tts_buffer_heap_init(TupleTableSlot *slot)
{
}

static void
tts_buffer_heap_release(TupleTableSlot *slot)
{
}

static void
tts_buffer_heap_clear(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	/*
	 * Free the memory for heap tuple if allowed. A tuple coming from buffer
	 * can never be freed. But we may have materialized a tuple from buffer.
	 * Such a tuple can be freed.
	 */
	if (TTS_SHOULDFREE(slot))
	{
		/* We should have unpinned the buffer while materializing the tuple. */
		Assert(!BufferIsValid(bslot->buffer));

		heap_freetuple(bslot->base.tuple);
		slot->tts_isempty &= ~TTS_FLAG_SHOULDFREE;
	}

	if (BufferIsValid(bslot->buffer))
		ReleaseBuffer(bslot->buffer);

	slot->tts_nvalid = 0;
	slot->tts_isempty |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
	bslot->base.tuple = NULL;
	bslot->base.off = 0;
	bslot->buffer = InvalidBuffer;
}

static void
tts_buffer_heap_getsomeattrs(TupleTableSlot *slot, int natts)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	slot_deform_heap_tuple(slot, bslot->base.tuple, &bslot->base.off, natts);
}

static Datum
tts_buffer_heap_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	return heap_getsysattr(bslot->base.tuple, attnum,
						   slot->tts_tupleDescriptor, isnull);
}

static void
tts_buffer_heap_materialize(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	MemoryContext oldContext;

	Assert(!TTS_EMPTY(slot));

	/* If slot has its tuple already materialized, nothing to do. */
	if (TTS_SHOULDFREE(slot))
		return;

	oldContext = MemoryContextSwitchTo(slot->tts_mcxt);

	/*
	 * Have to deform from scratch, otherwise tts_values[] entries could point
	 * into the non-materialized tuple (which might be gone when accessed).
	 */
	bslot->base.off = 0;
	slot->tts_nvalid = 0;

	if (!bslot->base.tuple)
	{
		/*
		 * Normally BufferHeapTupleTableSlot should have a tuple + buffer
		 * associated with it, unless it's materialized (which would've
		 * returned above). But when it's useful to allow storing virtual
		 * tuples in a buffer slot, which then also needs to be
		 * materializable.
		 */
		bslot->base.tuple = heap_form_tuple(slot->tts_tupleDescriptor,
											slot->tts_values,
											slot->tts_isnull);
	}
	else
	{
		bslot->base.tuple = heap_copytuple(bslot->base.tuple);

		/*
		 * A heap tuple stored in a BufferHeapTupleTableSlot should have a
		 * buffer associated with it, unless it's materialized or virtual.
		 */
		if (likely(BufferIsValid(bslot->buffer)))
			ReleaseBuffer(bslot->buffer);
		bslot->buffer = InvalidBuffer;
	}

	/*
	 * We don't set TTS_FLAG_SHOULDFREE until after releasing the buffer, if
	 * any.  This avoids having a transient state that would fall foul of our
	 * assertions that a slot with TTS_FLAG_SHOULDFREE doesn't own a buffer.
	 * In the unlikely event that ReleaseBuffer() above errors out, we'd
	 * effectively leak the copied tuple, but that seems fairly harmless.
	 */
	slot->tts_isempty |= TTS_FLAG_SHOULDFREE;

	MemoryContextSwitchTo(oldContext);
}

static void
tts_buffer_heap_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	BufferHeapTupleTableSlot *bsrcslot = (BufferHeapTupleTableSlot *) srcslot;
	BufferHeapTupleTableSlot *bdstslot = (BufferHeapTupleTableSlot *) dstslot;

	/*
	 * If the source slot is of a different kind, or is a buffer slot that has
	 * been materialized / is virtual, make a new copy of the tuple. Otherwise
	 * make a new reference to the in-buffer tuple.
	 */
	if (dstslot->tts_ops != srcslot->tts_ops ||
		TTS_SHOULDFREE(srcslot) ||
		!bsrcslot->base.tuple)
	{
		MemoryContext oldContext;

		ExecClearTuple(dstslot);
		dstslot->tts_isempty &= ~TTS_FLAG_EMPTY;
		oldContext = MemoryContextSwitchTo(dstslot->tts_mcxt);
		bdstslot->base.tuple = ExecCopySlotHeapTuple(srcslot);
		dstslot->tts_isempty |= TTS_FLAG_SHOULDFREE;
		MemoryContextSwitchTo(oldContext);
	}
	else
	{
		Assert(BufferIsValid(bsrcslot->buffer));

		tts_buffer_heap_store_tuple(dstslot, bsrcslot->base.tuple,
									bsrcslot->buffer, false);

		/*
		 * The HeapTupleData portion of the source tuple might be shorter
		 * lived than the destination slot. Therefore copy the HeapTuple into
		 * our slot's tupdata, which is guaranteed to live long enough (but
		 * will still point into the buffer).
		 */
		memcpy(&bdstslot->base.tupdata, bdstslot->base.tuple, sizeof(HeapTupleData));
		bdstslot->base.tuple = &bdstslot->base.tupdata;
	}
}

static HeapTuple
tts_buffer_heap_get_heap_tuple(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	if (!bslot->base.tuple)
		tts_buffer_heap_materialize(slot);

	return bslot->base.tuple;
}

static HeapTuple
tts_buffer_heap_copy_heap_tuple(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	if (!bslot->base.tuple)
		tts_buffer_heap_materialize(slot);

	return heap_copytuple(bslot->base.tuple);
}

static MinimalTuple
tts_buffer_heap_copy_minimal_tuple(TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	if (!bslot->base.tuple)
		tts_buffer_heap_materialize(slot);

	return minimal_tuple_from_heap_tuple(bslot->base.tuple);
}

static inline void
tts_buffer_heap_store_tuple(TupleTableSlot *slot, HeapTuple tuple,
							Buffer buffer, bool transfer_pin)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	if (TTS_SHOULDFREE(slot))
	{
		/* materialized slot shouldn't have a buffer to release */
		Assert(!BufferIsValid(bslot->buffer));

		heap_freetuple(bslot->base.tuple);
		slot->tts_isempty &= ~TTS_FLAG_SHOULDFREE;
	}

	slot->tts_isempty &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;
	bslot->base.tuple = tuple;
	bslot->base.off = 0;
	slot->tts_tid = tuple->t_self;

	/*
	 * If tuple is on a disk page, keep the page pinned as long as we hold a
	 * pointer into it.  We assume the caller already has such a pin.  If
	 * transfer_pin is true, we'll transfer that pin to this slot, if not
	 * we'll pin it again ourselves.
	 *
	 * This is coded to optimize the case where the slot previously held a
	 * tuple on the same disk page: in that case releasing and re-acquiring
	 * the pin is a waste of cycles.  This is a common situation during
	 * seqscans, so it's worth troubling over.
	 */
	if (bslot->buffer != buffer)
	{
		if (BufferIsValid(bslot->buffer))
			ReleaseBuffer(bslot->buffer);

		bslot->buffer = buffer;

		if (!transfer_pin && BufferIsValid(buffer))
			IncrBufferRefCount(buffer);
	}
	else if (transfer_pin && BufferIsValid(buffer))
	{
		/*
		 * In transfer_pin mode the caller won't know about the same-page
		 * optimization, so we gotta release its pin.
		 */
		ReleaseBuffer(buffer);
	}
}

/*
 * slot_deform_heap_tuple
 *		Given a TupleTableSlot, extract data from the slot's physical tuple
 *		into its Datum/isnull arrays.  Data is extracted up through the
 *		natts'th column (caller must ensure this is a legal column number).
 *
 *		This is essentially an incremental version of heap_deform_tuple:
 *		on each call we extract attributes up to the one needed, without
 *		re-computing information about previously extracted attributes.
 *		slot->tts_nvalid is the number of attributes already extracted.
 *
 * This is marked as always inline, so the different offp for different types
 * of slots gets optimized away.
 */
static inline void
slot_deform_heap_tuple(TupleTableSlot *slot, HeapTuple tuple, uint32 *offp,
					   int natts)
{
	TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
	Datum	   *values = slot->tts_values;
	bool	   *isnull = slot->tts_isnull;
	HeapTupleHeader tup = tuple->t_data;
	bool		hasnulls = HeapTupleHasNulls(tuple);
	int			attnum;
	char	   *tp;				/* ptr to tuple data */
	uint32		off;			/* offset in tuple data */
	bits8	   *bp = tup->t_bits;	/* ptr to null bitmap in tuple */
	bool		slow;			/* can we use/set attcacheoff? */

	/* We can only fetch as many attributes as the tuple has. */
    
	natts = Min((tuple->t_data)->t_infomask2 & HEAP_NATTS_MASK, natts);

	/*
	 * Check whether the first call for this tuple, and initialize or restore
	 * loop state.
	 */
	attnum = slot->tts_nvalid;
	if (attnum == 0)
	{
		/* Start from the first attribute */
		off = 0;
		slow = false;
	}
	else
	{
		/* Restore state from previous execution */
		off = *offp;
		slow = TTS_SLOW(slot);
	}

	tp = (char *) tup + tup->t_hoff;

	for (; attnum < natts; attnum++)
	{
		Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, attnum);

		if (hasnulls && att_isnull(attnum, bp))
		{
			values[attnum] = (Datum) 0;
			isnull[attnum] = true;
			slow = true;		/* can't use attcacheoff anymore */
			continue;
		}

		isnull[attnum] = false;

		if (!slow && thisatt->attcacheoff >= 0)
			off = thisatt->attcacheoff;
		else if (thisatt->attlen == -1)
		{
			/*
			 * We can only cache the offset for a varlena attribute if the
			 * offset is already suitably aligned, so that there would be no
			 * pad bytes in any case: then the offset will be valid for either
			 * an aligned or unaligned value.
			 */
			if (!slow &&
				off == att_align_nominal(off, thisatt->attalign))
				thisatt->attcacheoff = off;
			else
			{
				off = att_align_pointer(off, thisatt->attalign, -1,
										tp + off);
				slow = true;
			}
		}
		else
		{
			/* not varlena, so safe to use att_align_nominal */
			off = att_align_nominal(off, thisatt->attalign);

			if (!slow)
				thisatt->attcacheoff = off;
		}

		values[attnum] = fetchatt(thisatt, tp + off);

		off = att_addlength_pointer(off, thisatt->attlen, tp + off);

		if (thisatt->attlen <= 0)
			slow = true;		/* can't use attcacheoff anymore */
	}

	/*
	 * Save state for next execution
	 */
	slot->tts_nvalid = attnum;
	*offp = off;
	if (slow)
		slot->tts_isempty |= TTS_FLAG_SLOW;
	else
		slot->tts_isempty &= ~TTS_FLAG_SLOW;
}

/* --------------------------------
 *		ExecStoreHeapTuple
 *
 *		This function is used to store an on-the-fly physical tuple into a specified
 *		slot in the tuple table.
 *
 *		tuple:	tuple to store
 *		slot:	TTSOpsHeapTuple type slot to store it in
 *		shouldFree: true if ExecClearTuple should pfree() the tuple
 *					when done with it
 *
 * shouldFree is normally set 'true' for tuples constructed on-the-fly.  But it
 * can be 'false' when the referenced tuple is held in a tuple table slot
 * belonging to a lower-level executor Proc node.  In this case the lower-level
 * slot retains ownership and responsibility for eventually releasing the
 * tuple.  When this method is used, we must be certain that the upper-level
 * Proc node will lose interest in the tuple sooner than the lower-level one
 * does!  If you're not certain, copy the lower-level tuple with heap_copytuple
 * and let the upper-level table slot assume ownership of the copy!
 *
 * Return value is just the passed-in slot pointer.
 *
 * If the target slot is not guaranteed to be TTSOpsHeapTuple type slot, use
 * the, more expensive, ExecForceStoreHeapTuple().
 * --------------------------------
 */
TupleTableSlot *
ExecStoreHeapTuple(HeapTuple tuple,
				   TupleTableSlot *slot,
				   bool shouldFree)
{
	/*
	 * sanity checks
	 */
	Assert(tuple != NULL);
	Assert(slot != NULL);
	Assert(slot->tts_tupleDescriptor != NULL);

	if (unlikely(!TTS_IS_HEAPTUPLE(slot)))
		elog(ERROR, "trying to store a heap tuple into wrong type of slot");
	tts_heap_store_tuple(slot, tuple, shouldFree);

	slot->tts_tableOid = tuple->t_tableOid;

	return slot;
}
/*
 * Store a HeapTuple into any kind of slot, performing conversion if
 * necessary.
 */
void
ExecForceStoreHeapTuple(HeapTuple tuple,
						TupleTableSlot *slot,
						bool shouldFree)
{
	if (TTS_IS_HEAPTUPLE(slot))
	{
		ExecStoreHeapTuple(tuple, slot, shouldFree);
	}
	else if (TTS_IS_BUFFERTUPLE(slot))
	{
		MemoryContext oldContext;
		BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

		ExecClearTuple(slot);
		slot->tts_isempty &= ~TTS_FLAG_EMPTY;
		oldContext = MemoryContextSwitchTo(slot->tts_mcxt);
		bslot->base.tuple = heap_copytuple(tuple);
		slot->tts_isempty |= TTS_FLAG_SHOULDFREE;
		MemoryContextSwitchTo(oldContext);

		if (shouldFree)
			pfree(tuple);
	}
	else
	{
		ExecClearTuple(slot);
		heap_deform_tuple(tuple, slot->tts_tupleDescriptor,
						  slot->tts_values, slot->tts_isnull);
		ExecStoreVirtualTuple(slot);

		if (shouldFree)
		{
			ExecMaterializeSlot(slot);
			pfree(tuple);
		}
	}
}
/* ----------------------------------------------------------------
 *				convenience initialization routines
 * ----------------------------------------------------------------
 */

/* ----------------
 *		ExecInitResultTypeTL
 *
 *		Initialize result type, using the plan node's targetlist.
 * ----------------
 */
// void
// ExecInitResultTypeTL(PlanState *planstate)
// {
// 	TupleDesc	tupDesc = ExecTypeFromTL(planstate->plan->targetlist, false);

// 	planstate->ps_ResultTupleDesc = tupDesc;
// }

/* --------------------------------
 *		ExecInit{Result,Scan,Extra}TupleSlot[TL]
 *
 *		These are convenience routines to initialize the specified slot
 *		in nodes inheriting the appropriate state.  ExecInitExtraTupleSlot
 *		is used for initializing special-purpose slots.
 * --------------------------------
 */

/* ----------------
 *		ExecInitResultTupleSlotTL
 *
 *		Initialize result tuple slot, using the tuple descriptor previously
 *		computed with ExecInitResultTypeTL().
 * ----------------
 */
void
ExecInitResultSlot(PlanState *planstate)
{
	TupleTableSlot *slot;

	slot = ExecAllocTableSlot(&planstate->state->es_tupleTable, TAM_HEAP);
	planstate->ps_ResultTupleSlot = slot;
}
/* ----------------
 *		ExecInitResultTupleSlotTL
 *
 *		Initialize result tuple slot, using the plan node's targetlist.
 * ----------------
 */
void ExecInitResultTupleSlotTL(PlanState *planstate)
{
	// ExecInitResultTypeTL(planstate);
	ExecInitResultSlot(planstate);
}
const TupleTableSlotOps TTSOpsVirtual = {
	.base_slot_size = sizeof(VirtualTupleTableSlot),
	.init = tts_virtual_init,
	.release = tts_virtual_release,
	.clear = tts_virtual_clear,
	.getsomeattrs = tts_virtual_getsomeattrs,
	.getsysattr = tts_virtual_getsysattr,
	.materialize = tts_virtual_materialize,
	.copyslot = tts_virtual_copyslot,

	/*
	 * A virtual tuple table slot can not "own" a heap tuple or a minimal
	 * tuple.
	 */
	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_virtual_copy_heap_tuple,
	.copy_minimal_tuple = tts_virtual_copy_minimal_tuple
};

const TupleTableSlotOps TTSOpsHeapTuple = {
	.base_slot_size = sizeof(HeapTupleTableSlot),
	.init = tts_heap_init,
	.release = tts_heap_release,
	.clear = tts_heap_clear,
	.getsomeattrs = tts_heap_getsomeattrs,
	.getsysattr = tts_heap_getsysattr,
	.materialize = tts_heap_materialize,
	.copyslot = tts_heap_copyslot,
	.get_heap_tuple = tts_heap_get_heap_tuple,

	/* A heap tuple table slot can not "own" a minimal tuple. */
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_heap_copy_heap_tuple,
	.copy_minimal_tuple = tts_heap_copy_minimal_tuple
};

const TupleTableSlotOps TTSOpsMinimalTuple = {
	.base_slot_size = sizeof(MinimalTupleTableSlot),
	.init = tts_minimal_init,
	.release = tts_minimal_release,
	.clear = tts_minimal_clear,
	.getsomeattrs = tts_minimal_getsomeattrs,
	.getsysattr = tts_minimal_getsysattr,
	.materialize = tts_minimal_materialize,
	.copyslot = tts_minimal_copyslot,

	/* A minimal tuple table slot can not "own" a heap tuple. */
	.get_heap_tuple = NULL,
	.get_minimal_tuple = tts_minimal_get_minimal_tuple,
	.copy_heap_tuple = tts_minimal_copy_heap_tuple,
	.copy_minimal_tuple = tts_minimal_copy_minimal_tuple
};

const TupleTableSlotOps TTSOpsBufferHeapTuple = {
	.base_slot_size = sizeof(BufferHeapTupleTableSlot),
	.init = tts_buffer_heap_init,
	.release = tts_buffer_heap_release,
	.clear = tts_buffer_heap_clear,
	.getsomeattrs = tts_buffer_heap_getsomeattrs,
	.getsysattr = tts_buffer_heap_getsysattr,
	.materialize = tts_buffer_heap_materialize,
	.copyslot = tts_buffer_heap_copyslot,
	.get_heap_tuple = tts_buffer_heap_get_heap_tuple,

	/* A buffer heap tuple table slot can not "own" a minimal tuple. */
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_buffer_heap_copy_heap_tuple,
	.copy_minimal_tuple = tts_buffer_heap_copy_minimal_tuple
};
#endif