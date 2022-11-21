/* -------------------------------------------------------------------------
 *
 * tuptable.h
 *	  tuple table support stuff
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/tuptable.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef TUPTABLE_H
#define TUPTABLE_H

#include "access/htup.h"
#include "storage/buf/buf.h"

#ifndef FRONTEND_PARSER
/* ----------
 * The executor stores tuples in a "tuple table" which is a List of
 * independent TupleTableSlots.  There are several cases we need to handle:
 *		1. physical tuple in a disk buffer page
 *		2. physical tuple constructed in palloc'ed memory
 *		3. "minimal" physical tuple constructed in palloc'ed memory
 *		4. "virtual" tuple consisting of Datum/isnull arrays
 *
 * The first two cases are similar in that they both deal with "materialized"
 * tuples, but resource management is different.  For a tuple in a disk page
 * we need to hold a pin on the buffer until the TupleTableSlot's reference
 * to the tuple is dropped; while for a palloc'd tuple we usually want the
 * tuple pfree'd when the TupleTableSlot's reference is dropped.
 *
 * A "minimal" tuple is handled similarly to a palloc'd regular tuple.
 * At present, minimal tuples never are stored in buffers, so there is no
 * parallel to case 1.	Note that a minimal tuple has no "system columns".
 * (Actually, it could have an OID, but we have no need to access the OID.)
 *
 * A "virtual" tuple is an optimization used to minimize physical data
 * copying in a nest of plan nodes.  Any pass-by-reference Datums in the
 * tuple point to storage that is not directly associated with the
 * TupleTableSlot; generally they will point to part of a tuple stored in
 * a lower plan node's output TupleTableSlot, or to a function result
 * constructed in a plan node's per-tuple econtext.  It is the responsibility
 * of the generating plan node to be sure these resources are not released
 * for as long as the virtual tuple needs to be valid.	We only use virtual
 * tuples in the result slots of plan nodes --- tuples to be copied anywhere
 * else need to be "materialized" into physical tuples.  Note also that a
 * virtual tuple does not have any "system columns".
 *
 * It is also possible for a TupleTableSlot to hold both physical and minimal
 * copies of a tuple.  This is done when the slot is requested to provide
 * the format other than the one it currently holds.  (Originally we attempted
 * to handle such requests by replacing one format with the other, but that
 * had the fatal defect of invalidating any pass-by-reference Datums pointing
 * into the existing slot contents.)  Both copies must contain identical data
 * payloads when this is the case.
 *
 * The Datum/isnull arrays of a TupleTableSlot serve double duty.  When the
 * slot contains a virtual tuple, they are the authoritative data.	When the
 * slot contains a physical tuple, the arrays contain data extracted from
 * the tuple.  (In this state, any pass-by-reference Datums point into
 * the physical tuple.)  The extracted information is built "lazily",
 * ie, only as needed.	This serves to avoid repeated extraction of data
 * from the physical tuple.
 *
 * A TupleTableSlot can also be "empty", holding no valid data.  This is
 * the only valid state for a freshly-created slot that has not yet had a
 * tuple descriptor assigned to it.  In this state, tts_isempty must be
 * TRUE, tts_shouldFree FALSE, tts_tuple NULL, tts_buffer InvalidBuffer,
 * and tts_nvalid zero.
 *
 * The tupleDescriptor is simply referenced, not copied, by the TupleTableSlot
 * code.  The caller of ExecSetSlotDescriptor() is responsible for providing
 * a descriptor that will live as long as the slot does.  (Typically, both
 * slots and descriptors are in per-query memory and are freed by memory
 * context deallocation at query end; so it's not worth providing any extra
 * mechanism to do more.  However, the slot will increment the tupdesc
 * reference count if a reference-counted tupdesc is supplied.)
 *
 * When tts_shouldFree is true, the physical tuple is "owned" by the slot
 * and should be freed when the slot's reference to the tuple is dropped.
 *
 * If tts_buffer is not InvalidBuffer, then the slot is holding a pin
 * on the indicated buffer page; drop the pin when we release the
 * slot's reference to that buffer.  (tts_shouldFree should always be
 * false in such a case, since presumably tts_tuple is pointing at the
 * buffer page.)
 *
 * tts_nvalid indicates the number of valid columns in the tts_values/isnull
 * arrays.	When the slot is holding a "virtual" tuple this must be equal
 * to the descriptor's natts.  When the slot is holding a physical tuple
 * this is equal to the number of columns we have extracted (we always
 * extract columns from left to right, so there are no holes).
 *
 * tts_values/tts_isnull are allocated when a descriptor is assigned to the
 * slot; they are of length equal to the descriptor's natts.
 *
 * tts_mintuple must always be NULL if the slot does not hold a "minimal"
 * tuple.  When it does, tts_mintuple points to the actual MinimalTupleData
 * object (the thing to be pfree'd if tts_shouldFreeMin is true).  If the slot
 * has only a minimal and not also a regular physical tuple, then tts_tuple
 * points at tts_minhdr and the fields of that struct are set correctly
 * for access to the minimal tuple; in particular, tts_minhdr.t_data points
 * MINIMAL_TUPLE_OFFSET bytes before tts_mintuple.	This allows column
 * extraction to treat the case identically to regular physical tuples.
 *
 * tts_slow/tts_off are saved state for slot_deform_tuple, and should not
 * be touched by any other code.
 * ----------
 */
#ifdef GS_GRAPH
#define			TTS_FLAG_EMPTY			(1 << 1)
#define TTS_EMPTY(slot)	(((slot)->tts_isempty & TTS_FLAG_EMPTY) != 0)

/* should pfree tuple "owned" by the slot? */
#define			TTS_FLAG_SHOULDFREE		(1 << 2)
#define TTS_SHOULDFREE(slot) (((slot)->tts_isempty & TTS_FLAG_SHOULDFREE) != 0)

/* saved state for slot_deform_heap_tuple */
#define			TTS_FLAG_SLOW		(1 << 3)
#define TTS_SLOW(slot) (((slot)->tts_isempty & TTS_FLAG_SLOW) != 0)

/* fixed tuple descriptor */
#define			TTS_FLAG_FIXED		(1 << 4)
#define TTS_FIXED(slot) (((slot)->tts_isempty & TTS_FLAG_FIXED) != 0)
struct TupleTableSlotOps;
typedef struct TupleTableSlotOps TupleTableSlotOps;
/*
 * Predefined TupleTableSlotOps for various types of TupleTableSlotOps. The
 * same are used to identify the type of a given slot.
 */
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsVirtual;
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsHeapTuple;
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsMinimalTuple;
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsBufferHeapTuple;

#define TTS_IS_VIRTUAL(slot) ((slot)->tts_ops == &TTSOpsVirtual)
#define TTS_IS_HEAPTUPLE(slot) ((slot)->tts_ops == &TTSOpsHeapTuple)
#define TTS_IS_MINIMALTUPLE(slot) ((slot)->tts_ops == &TTSOpsMinimalTuple)
#define TTS_IS_BUFFERTUPLE(slot) ((slot)->tts_ops == &TTSOpsBufferHeapTuple)
#endif
typedef struct TupleTableSlot {
    NodeTag type;
    bool tts_isempty;       /* true = slot is empty */
    bool tts_shouldFree;    /* should pfree tts_tuple? */
    bool tts_shouldFreeMin; /* should pfree tts_mintuple? */
    bool tts_slow;          /* saved state for slot_deform_tuple */

    Tuple tts_tuple;    /* physical tuple, or NULL if virtual */
#ifdef PGXC
    /*
     * PGXC extension to support tuples sent from remote Datanode.
     */
    char* tts_dataRow;                   /* Tuple data in DataRow format */
    int tts_dataLen;                     /* Actual length of the data row */
    bool tts_shouldFreeRow;              /* should pfree tts_dataRow? */
    struct AttInMetadata* tts_attinmeta; /* store here info to extract values from the DataRow */
    Oid tts_xcnodeoid;                   /* Oid of node from where the datarow is fetched */
    MemoryContext tts_per_tuple_mcxt;
#endif
    TupleDesc tts_tupleDescriptor; /* slot's tuple descriptor */
    MemoryContext tts_mcxt;        /* slot itself is in this context */
    Buffer tts_buffer;             /* tuple's buffer, or InvalidBuffer */
    int tts_nvalid;                /* # of valid values in tts_values */
    Datum* tts_values;             /* current per-attribute values */
    bool* tts_isnull;              /* current per-attribute isnull flags */
    Datum* tts_lobPointers;
    MinimalTuple tts_mintuple;     /* minimal tuple, or NULL if none */
    HeapTupleData tts_minhdr;      /* workspace for minimal-tuple-only case */
    long tts_off;                  /* saved state for slot_deform_tuple */
    long tts_meta_off;             /* saved state for slot_deform_cmpr_tuple */
    TableAmType tts_tupslotTableAm;    /* slots's tuple table type */
#ifdef GS_GRAPH
    const TupleTableSlotOps *const tts_ops; /* implementation of slot */
    ItemPointerData tts_tid;	/* stored tuple's tid */
    Oid			tts_tableOid;	/* table oid of tuple */
#endif
} TupleTableSlot;

#ifdef GS_GRAPH
/* routines for a TupleTableSlot implementation */
struct TupleTableSlotOps
{
	/* Minimum size of the slot */
	size_t		base_slot_size;

	/* Initialization. */
	void		(*init) (TupleTableSlot *slot);

	/* Destruction. */
	void		(*release) (TupleTableSlot *slot);

	/*
	 * Clear the contents of the slot. Only the contents are expected to be
	 * cleared and not the tuple descriptor. Typically an implementation of
	 * this callback should free the memory allocated for the tuple contained
	 * in the slot.
	 */
	void		(*clear) (TupleTableSlot *slot);

	/*
	 * Fill up first natts entries of tts_values and tts_isnull arrays with
	 * values from the tuple contained in the slot. The function may be called
	 * with natts more than the number of attributes available in the tuple,
	 * in which case it should set tts_nvalid to the number of returned
	 * columns.
	 */
	void		(*getsomeattrs) (TupleTableSlot *slot, int natts);

	/*
	 * Returns value of the given system attribute as a datum and sets isnull
	 * to false, if it's not NULL. Throws an error if the slot type does not
	 * support system attributes.
	 */
	Datum		(*getsysattr) (TupleTableSlot *slot, int attnum, bool *isnull);

	/*
	 * Make the contents of the slot solely depend on the slot, and not on
	 * underlying resources (like another memory context, buffers, etc).
	 */
	void		(*materialize) (TupleTableSlot *slot);

	/*
	 * Copy the contents of the source slot into the destination slot's own
	 * context. Invoked using callback of the destination slot.
	 */
	void		(*copyslot) (TupleTableSlot *dstslot, TupleTableSlot *srcslot);

	/*
	 * Return a heap tuple "owned" by the slot. It is slot's responsibility to
	 * free the memory consumed by the heap tuple. If the slot can not "own" a
	 * heap tuple, it should not implement this callback and should set it as
	 * NULL.
	 */
	HeapTuple	(*get_heap_tuple) (TupleTableSlot *slot);

	/*
	 * Return a minimal tuple "owned" by the slot. It is slot's responsibility
	 * to free the memory consumed by the minimal tuple. If the slot can not
	 * "own" a minimal tuple, it should not implement this callback and should
	 * set it as NULL.
	 */
	MinimalTuple (*get_minimal_tuple) (TupleTableSlot *slot);

	/*
	 * Return a copy of heap tuple representing the contents of the slot. The
	 * copy needs to be palloc'd in the current memory context. The slot
	 * itself is expected to remain unaffected. It is *not* expected to have
	 * meaningful "system columns" in the copy. The copy is not be "owned" by
	 * the slot i.e. the caller has to take responsibility to free memory
	 * consumed by the slot.
	 */
	HeapTuple	(*copy_heap_tuple) (TupleTableSlot *slot);

	/*
	 * Return a copy of minimal tuple representing the contents of the slot.
	 * The copy needs to be palloc'd in the current memory context. The slot
	 * itself is expected to remain unaffected. It is *not* expected to have
	 * meaningful "system columns" in the copy. The copy is not be "owned" by
	 * the slot i.e. the caller has to take responsibility to free memory
	 * consumed by the slot.
	 */
	MinimalTuple (*copy_minimal_tuple) (TupleTableSlot *slot);
};

/*
 * Tuple table slot implementations.
 */

typedef struct VirtualTupleTableSlot
{
	TupleTableSlot base;

	char	   *data;			/* data for materialized slots */
} VirtualTupleTableSlot;

typedef struct HeapTupleTableSlot
{
	TupleTableSlot base;

#define FIELDNO_HEAPTUPLETABLESLOT_TUPLE 1
	HeapTuple	tuple;			/* physical tuple */
#define FIELDNO_HEAPTUPLETABLESLOT_OFF 2
	uint32		off;			/* saved state for slot_deform_heap_tuple */
	HeapTupleData tupdata;		/* optional workspace for storing tuple */
} HeapTupleTableSlot;

/* heap tuple residing in a buffer */
typedef struct BufferHeapTupleTableSlot
{
	HeapTupleTableSlot base;

	/*
	 * If buffer is not InvalidBuffer, then the slot is holding a pin on the
	 * indicated buffer page; drop the pin when we release the slot's
	 * reference to that buffer.  (TTS_FLAG_SHOULDFREE should not be set in
	 * such a case, since presumably tts_tuple is pointing into the buffer.)
	 */
	Buffer		buffer;			/* tuple's buffer, or InvalidBuffer */
} BufferHeapTupleTableSlot;

typedef struct MinimalTupleTableSlot
{
	TupleTableSlot base;

	/*
	 * In a minimal slot tuple points at minhdr and the fields of that struct
	 * are set correctly for access to the minimal tuple; in particular,
	 * minhdr.t_data points MINIMAL_TUPLE_OFFSET bytes before mintuple.  This
	 * allows column extraction to treat the case identically to regular
	 * physical tuples.
	 */
#define FIELDNO_MINIMALTUPLETABLESLOT_TUPLE 1
	HeapTuple	tuple;			/* tuple wrapper */
	MinimalTuple mintuple;		/* minimal tuple, or NULL if none */
	HeapTupleData minhdr;		/* workspace for minimal-tuple-only case */
#define FIELDNO_MINIMALTUPLETABLESLOT_OFF 4
	uint32		off;			/* saved state for slot_deform_heap_tuple */
} MinimalTupleTableSlot;
#endif

#define TTS_HAS_PHYSICAL_TUPLE(slot) ((slot)->tts_tuple != NULL && (slot)->tts_tuple != &((slot)->tts_minhdr))

/*
 * TupIsNull -- is a TupleTableSlot empty?
 */
#define TupIsNull(slot) ((slot) == NULL || (slot)->tts_isempty)

/* in executor/execTuples.c */
extern TupleTableSlot* MakeTupleTableSlot(bool has_tuple_mcxt = false, TableAmType tupslotTableAm = TAM_HEAP);
extern TupleTableSlot* ExecAllocTableSlot(List** tupleTable, TableAmType tupslotTableAm = TAM_HEAP);
extern void ExecResetTupleTable(List* tupleTable, bool shouldFree);
extern TupleTableSlot* MakeSingleTupleTableSlot(TupleDesc tupdesc, bool allocSlotCxt = false, TableAmType tupslotTableAm = TAM_HEAP);
extern void ExecDropSingleTupleTableSlot(TupleTableSlot* slot);
extern void ExecSetSlotDescriptor(TupleTableSlot* slot, TupleDesc tupdesc);
extern TupleTableSlot* ExecStoreTuple(Tuple tuple, TupleTableSlot* slot, Buffer buffer, bool shouldFree);
extern TupleTableSlot *ExecStoreTupleBatch(HeapTuple tuple, TupleTableSlot *slot,
    Buffer buffer, bool shouldFree, int rownum);
extern TupleTableSlot* ExecStoreMinimalTuple(MinimalTuple mtup, TupleTableSlot* slot, bool shouldFree);

#ifdef PGXC
extern TupleTableSlot* ExecStoreDataRowTuple(
    char* msg, size_t len, Oid msgnode_oid, TupleTableSlot* slot, bool shouldFree);
#endif
extern TupleTableSlot* ExecClearTuple(TupleTableSlot* slot);
extern TupleTableSlot* ExecStoreVirtualTuple(TupleTableSlot* slot);
extern TupleTableSlot* ExecStoreAllNullTuple(TupleTableSlot* slot);
extern HeapTuple ExecCopySlotTuple(TupleTableSlot* slot);
extern MinimalTuple ExecCopySlotMinimalTuple(TupleTableSlot* slot, bool need_transform_anyarray = false);
extern HeapTuple ExecFetchSlotTuple(TupleTableSlot* slot);
extern MinimalTuple ExecFetchSlotMinimalTuple(TupleTableSlot* slot);
extern Datum ExecFetchSlotTupleDatum(TupleTableSlot* slot);
extern HeapTuple ExecMaterializeSlot(TupleTableSlot* slot);
extern TupleTableSlot* ExecCopySlot(TupleTableSlot* dstslot, TupleTableSlot* srcslot);

/* heap table specific slot/tuple operations*/
/* definitions are found in access/common/heaptuple.c */
extern void heap_slot_clear(TupleTableSlot* slot);
extern void heap_slot_materialize(TupleTableSlot* slot);
extern MinimalTuple heap_slot_get_minimal_tuple(TupleTableSlot *slot);
extern MinimalTuple heap_slot_copy_minimal_tuple(TupleTableSlot *slot);
extern void heap_slot_store_minimal_tuple(MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree);
extern HeapTuple heap_slot_get_heap_tuple (TupleTableSlot* slot);
extern HeapTuple heap_slot_copy_heap_tuple (TupleTableSlot *slot);
extern void heap_slot_store_heap_tuple(HeapTuple tuple, TupleTableSlot* slot, Buffer buffer, bool shouldFree, bool batchMode);
extern Datum heap_slot_getattr(TupleTableSlot* slot, int attnum, bool* isnull, bool need_transform_anyarray = false);
extern void heap_slot_getallattrs(TupleTableSlot* slot, bool need_transform_anyarray = false);
extern void slot_getallattrs(TupleTableSlot *slot);
extern void slot_getallattrsfast(TupleTableSlot *slot, int maxIdx);
extern void heap_slot_getsomeattrs(TupleTableSlot* slot, int attnum);
extern bool heap_slot_attisnull(TupleTableSlot* slot, int attnum);
extern void heap_slot_formbatch(TupleTableSlot* slot, struct VectorBatch* batch, int cur_rows, int attnum);
extern Datum slot_getattr(TupleTableSlot *slot, int attnum, bool *isnull);

#ifdef GS_GRAPH
/*
 * ExecCopySlotHeapTuple - return HeapTuple allocated in caller's context
 */
extern void ExecForceStoreHeapTuple(HeapTuple tuple, TupleTableSlot *slot, bool shouldFree);
extern TupleTableSlot *ExecStoreHeapTuple(HeapTuple tuple, TupleTableSlot *slot, bool shouldFree);
static inline HeapTuple
ExecCopySlotHeapTuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	return slot->tts_ops->copy_heap_tuple(slot);
}
#endif

#endif /* !FRONTEND_PARSER */
#endif /* TUPTABLE_H */
