/* -------------------------------------------------------------------------
 *
 * array_userfuncs.c
 *	  Misc user-visible array support functions
 *
 * Copyright (c) 2003-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/array_userfuncs.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#ifdef GS_GRAPH
static Datum array_position_common(FunctionCallInfo fcinfo);
#endif

/* -----------------------------------------------------------------------------
 * array_push :
 *		push an element onto either end of a one-dimensional array
 * ----------------------------------------------------------------------------
 */
Datum array_push(PG_FUNCTION_ARGS)
{
    ArrayType* v = NULL;
    Datum newelem;
    bool isNull = false;
    int *dimv = NULL, *lb = NULL;
    ArrayType* result = NULL;
    int indx;
    Oid element_type;
    int16 typlen;
    bool typbyval = false;
    char typalign;
    Oid arg0_typeid = get_fn_expr_argtype(fcinfo->flinfo, 0);
    Oid arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
    Oid arg0_elemid;
    Oid arg1_elemid;
    ArrayMetaState* my_extra = NULL;

    if (arg0_typeid == InvalidOid || arg1_typeid == InvalidOid)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("could not determine input data types")));

    arg0_elemid = get_element_type(arg0_typeid);
    arg1_elemid = get_element_type(arg1_typeid);

    if (arg0_elemid != InvalidOid) {
        if (PG_ARGISNULL(0))
            v = construct_empty_array(arg0_elemid);
        else
            v = PG_GETARG_ARRAYTYPE_P(0);
        isNull = PG_ARGISNULL(1);
        if (isNull)
            newelem = (Datum)0;
        else
            newelem = PG_GETARG_DATUM(1);
    } else if (arg1_elemid != InvalidOid) {
        if (PG_ARGISNULL(1))
            v = construct_empty_array(arg1_elemid);
        else
            v = PG_GETARG_ARRAYTYPE_P(1);
        isNull = PG_ARGISNULL(0);
        if (isNull)
            newelem = (Datum)0;
        else
            newelem = PG_GETARG_DATUM(0);
    } else {
        /* Shouldn't get here given proper type checking in parser */
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("neither input type is an array")));
        PG_RETURN_NULL(); /* keep compiler quiet */
    }

    element_type = ARR_ELEMTYPE(v);

    if (ARR_NDIM(v) == 1) {
        lb = ARR_LBOUND(v);
        dimv = ARR_DIMS(v);

        if (arg0_elemid != InvalidOid) {
            /* append newelem */
            if (dimv[0] > 0 && lb[0] > 0 && (INT_MAX - dimv[0] < lb[0])) {
                ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
            }
            if (dimv[0] < 0 && lb[0] < 0 && (INT_MAX + dimv[0] < 0 - lb[0])) {
                ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
            }
            int ub = dimv[0] + lb[0] - 1;
            indx = ub + 1;
        } else {
            /* prepend newelem */
            indx = lb[0] - 1;
            /* overflow? */
            if (indx > lb[0])
                ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
        }
    } else if (ARR_NDIM(v) == 0)
        indx = 1;
    else
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("argument must be empty or one-dimensional array")));

    /*
     * We arrange to look up info about element type only once per series of
     * calls, assuming the element type doesn't change underneath us.
     */
    my_extra = (ArrayMetaState*)fcinfo->flinfo->fn_extra;
    if (my_extra == NULL) {
        fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, sizeof(ArrayMetaState));
        my_extra = (ArrayMetaState*)fcinfo->flinfo->fn_extra;
        my_extra->element_type = ~element_type;
    }

    if (my_extra->element_type != element_type) {
        /* Get info about element type */
        get_typlenbyvalalign(element_type, &my_extra->typlen, &my_extra->typbyval, &my_extra->typalign);
        my_extra->element_type = element_type;
    }
    typlen = my_extra->typlen;
    typbyval = my_extra->typbyval;
    typalign = my_extra->typalign;

    result = array_set(v, 1, &indx, newelem, isNull, -1, typlen, typbyval, typalign);

    /*
     * Readjust result's LB to match the input's.  This does nothing in the
     * append case, but it's the simplest way to implement the prepend case.
     */
    if (ARR_NDIM(v) == 1)
        ARR_LBOUND(result)[0] = ARR_LBOUND(v)[0];

    PG_RETURN_ARRAYTYPE_P(result);
}

/* -----------------------------------------------------------------------------
 * array_cat :
 *		concatenate two nD arrays to form an nD array, or
 *		push an (n-1)D array onto the end of an nD array
 * ----------------------------------------------------------------------------
 */
Datum array_cat(PG_FUNCTION_ARGS)
{
    ArrayType *v1 = NULL, *v2 = NULL;
    ArrayType* result = NULL;
    int *dims = NULL, *lbs = NULL, ndims, nitems, ndatabytes, nbytes;
    int *dims1 = NULL, *lbs1 = NULL, ndims1, nitems1, ndatabytes1;
    int *dims2 = NULL, *lbs2 = NULL, ndims2, nitems2, ndatabytes2;
    int i;
    char *dat1 = NULL, *dat2 = NULL;
    bits8 *bitmap1 = NULL, *bitmap2 = NULL;
    Oid element_type;
    Oid element_type1;
    Oid element_type2;
    int32 dataoffset;
    errno_t rc = EOK;

    /* Concatenating a null array is a no-op, just return the other input */
    if (PG_ARGISNULL(0)) {
        if (PG_ARGISNULL(1))
            PG_RETURN_NULL();
        result = PG_GETARG_ARRAYTYPE_P(1);
        PG_RETURN_ARRAYTYPE_P(result);
    }
    if (PG_ARGISNULL(1)) {
        result = PG_GETARG_ARRAYTYPE_P(0);
        PG_RETURN_ARRAYTYPE_P(result);
    }

    v1 = PG_GETARG_ARRAYTYPE_P(0);
    v2 = PG_GETARG_ARRAYTYPE_P(1);

    element_type1 = ARR_ELEMTYPE(v1);
    element_type2 = ARR_ELEMTYPE(v2);

    /* Check we have matching element types */
    if (element_type1 != element_type2)
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("cannot concatenate incompatible arrays"),
                errdetail("Arrays with element types %s and %s are not "
                          "compatible for concatenation.",
                    format_type_be(element_type1),
                    format_type_be(element_type2))));

    /* OK, use it */
    element_type = element_type1;

    /* ----------
     * We must have one of the following combinations of inputs:
     * 1) one empty array, and one non-empty array
     * 2) both arrays empty
     * 3) two arrays with ndims1 == ndims2
     * 4) ndims1 == ndims2 - 1
     * 5) ndims1 == ndims2 + 1
     * ----------
     */
    ndims1 = ARR_NDIM(v1);
    ndims2 = ARR_NDIM(v2);

    /*
     * short circuit - if one input array is empty, and the other is not, we
     * return the non-empty one as the result
     *
     * if both are empty, return the first one
     */
    if (ndims1 == 0 && ndims2 > 0)
        PG_RETURN_ARRAYTYPE_P(v2);

    if (ndims2 == 0)
        PG_RETURN_ARRAYTYPE_P(v1);

    /* the rest fall under rule 3, 4, or 5 */
    if (ndims1 != ndims2 && ndims1 != ndims2 - 1 && ndims1 != ndims2 + 1)
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                errmsg("cannot concatenate incompatible arrays"),
                errdetail("Arrays of %d and %d dimensions are not "
                          "compatible for concatenation.",
                    ndims1,
                    ndims2)));

    /* get argument array details */
    lbs1 = ARR_LBOUND(v1);
    lbs2 = ARR_LBOUND(v2);
    dims1 = ARR_DIMS(v1);
    dims2 = ARR_DIMS(v2);
    dat1 = ARR_DATA_PTR(v1);
    dat2 = ARR_DATA_PTR(v2);
    bitmap1 = ARR_NULLBITMAP(v1);
    bitmap2 = ARR_NULLBITMAP(v2);
    nitems1 = ArrayGetNItems(ndims1, dims1);
    nitems2 = ArrayGetNItems(ndims2, dims2);
    ndatabytes1 = ARR_SIZE(v1) - ARR_DATA_OFFSET(v1);
    ndatabytes2 = ARR_SIZE(v2) - ARR_DATA_OFFSET(v2);

    if (ndims1 == ndims2) {
        /*
         * resulting array is made up of the elements (possibly arrays
         * themselves) of the input argument arrays
         */
        ndims = ndims1;
        dims = (int*)palloc(ndims * sizeof(int));
        lbs = (int*)palloc(ndims * sizeof(int));
        /* dims1[0] is elements nums of dimension 1 in the left array */
        if (unlikely(INT_MAX - dims1[0] < dims2[0])) {
            ereport(ERROR,
                (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                    errmsg("cannot accpect arrays with dimensions out of range")));
        }
        dims[0] = dims1[0] + dims2[0];
        lbs[0] = lbs1[0];

        for (i = 1; i < ndims; i++) {
            if (dims1[i] != dims2[i] || lbs1[i] != lbs2[i])
                ereport(ERROR,
                    (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                        errmsg("cannot concatenate incompatible arrays"),
                        errdetail("Arrays with differing element dimensions are "
                                  "not compatible for concatenation.")));

            dims[i] = dims1[i];
            lbs[i] = lbs1[i];
        }
    } else if (ndims1 == ndims2 - 1) {
        /*
         * resulting array has the second argument as the outer array, with
         * the first argument inserted at the front of the outer dimension
         */
        ndims = ndims2;
        dims = (int*)palloc(ndims * sizeof(int));
        lbs = (int*)palloc(ndims * sizeof(int));
        rc = memcpy_s(dims, ndims * sizeof(int), dims2, ndims * sizeof(int));
        securec_check(rc, "", "");
        rc = memcpy_s(lbs, ndims * sizeof(int), lbs2, ndims * sizeof(int));
        securec_check(rc, "", "");

        /* increment number of elements in outer array */
        dims[0] += 1;

        /* make sure the added element matches our existing elements */
        for (i = 0; i < ndims1; i++) {
            if (dims1[i] != dims[i + 1] || lbs1[i] != lbs[i + 1])
                ereport(ERROR,
                    (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                        errmsg("cannot concatenate incompatible arrays"),
                        errdetail("Arrays with differing dimensions are not "
                                  "compatible for concatenation.")));
        }
    } else {
        /*
         * (ndims1 == ndims2 + 1)
         *
         * resulting array has the first argument as the outer array, with the
         * second argument appended to the end of the outer dimension
         */
        ndims = ndims1;
        dims = (int*)palloc(ndims * sizeof(int));
        lbs = (int*)palloc(ndims * sizeof(int));
        rc = memcpy_s(dims, ndims * sizeof(int), dims1, ndims * sizeof(int));
        securec_check(rc, "", "");
        rc = memcpy_s(lbs, ndims * sizeof(int), lbs1, ndims * sizeof(int));
        securec_check(rc, "", "");

        /* increment number of elements in outer array */
        dims[0] += 1;

        /* make sure the added element matches our existing elements */
        for (i = 0; i < ndims2; i++) {
            if (dims2[i] != dims[i + 1] || lbs2[i] != lbs[i + 1])
                ereport(ERROR,
                    (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                        errmsg("cannot concatenate incompatible arrays"),
                        errdetail("Arrays with differing dimensions are not "
                                  "compatible for concatenation.")));
        }
    }

    /* Do this mainly for overflow checking */
    nitems = ArrayGetNItems(ndims, dims);

    /* build the result array */
    ndatabytes = ndatabytes1 + ndatabytes2;
    if (ARR_HASNULL(v1) || ARR_HASNULL(v2)) {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nitems);
        nbytes = ndatabytes + dataoffset;
    } else {
        dataoffset = 0; /* marker for no null bitmap */
        nbytes = ndatabytes + ARR_OVERHEAD_NONULLS(ndims);
    }
    result = (ArrayType*)palloc0(nbytes);
    SET_VARSIZE(result, nbytes);
    result->ndim = ndims;
    result->dataoffset = dataoffset;
    result->elemtype = element_type;
    rc = memcpy_s(ARR_DIMS(result), ndims * sizeof(int), dims, ndims * sizeof(int));
    securec_check(rc, "", "");
    rc = memcpy_s(ARR_LBOUND(result), ndims * sizeof(int), lbs, ndims * sizeof(int));
    securec_check(rc, "", "");

    /* data area is arg1 then arg2. And make sure the destMax of memcpy_s should never be zero. */
    if (ndatabytes1 > 0) {
        rc = memcpy_s(ARR_DATA_PTR(result), ndatabytes1, dat1, ndatabytes1);
        securec_check(rc, "", "");
    }
    if (ndatabytes2 > 0) {
        rc = memcpy_s(ARR_DATA_PTR(result) + ndatabytes1, ndatabytes2, dat2, ndatabytes2);
        securec_check(rc, "", "");
    }

    /* handle the null bitmap if needed */
    if (ARR_HASNULL(result)) {
        array_bitmap_copy(ARR_NULLBITMAP(result), 0, bitmap1, 0, nitems1);
        array_bitmap_copy(ARR_NULLBITMAP(result), nitems1, bitmap2, 0, nitems2);
    }

    PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * used by text_to_array() in varlena.c
 */
ArrayType* create_singleton_array(FunctionCallInfo fcinfo, Oid element_type, Datum element, bool isNull, int ndims)
{
    Datum dvalues[1];
    bool nulls[1];
    int16 typlen;
    bool typbyval = false;
    char typalign;
    int dims[MAXDIM];
    int lbs[MAXDIM];
    int i;
    ArrayMetaState* my_extra = NULL;

    if (ndims < 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid number of dimensions: %d", ndims)));
    if (ndims > MAXDIM)
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)", ndims, MAXDIM)));

    dvalues[0] = element;
    nulls[0] = isNull;

    for (i = 0; i < ndims; i++) {
        dims[i] = 1;
        lbs[i] = 1;
    }

    /*
     * We arrange to look up info about element type only once per series of
     * calls, assuming the element type doesn't change underneath us.
     */
    my_extra = (ArrayMetaState*)fcinfo->flinfo->fn_extra;
    if (my_extra == NULL) {
        fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, sizeof(ArrayMetaState));
        my_extra = (ArrayMetaState*)fcinfo->flinfo->fn_extra;
        my_extra->element_type = ~element_type;
    }

    if (my_extra->element_type != element_type) {
        /* Get info about element type */
        get_typlenbyvalalign(element_type, &my_extra->typlen, &my_extra->typbyval, &my_extra->typalign);
        my_extra->element_type = element_type;
    }
    typlen = my_extra->typlen;
    typbyval = my_extra->typbyval;
    typalign = my_extra->typalign;

    return construct_md_array(dvalues, nulls, ndims, dims, lbs, element_type, typlen, typbyval, typalign);
}

/*
 * ARRAY_AGG aggregate function
 */
Datum array_agg_transfn(PG_FUNCTION_ARGS)
{
    Oid arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
    MemoryContext aggcontext;
    ArrayBuildState* state = NULL;
    Datum elem;

    if (arg1_typeid == InvalidOid)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("could not determine input data type")));

    if (!AggCheckCallContext(fcinfo, &aggcontext)) {
        /* cannot be called directly because of internal-type argument */
        ereport(ERROR,
            (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("array_agg_transfn called in non-aggregate context")));
    }

    state = PG_ARGISNULL(0) ? NULL : (ArrayBuildState*)PG_GETARG_POINTER(0);
    elem = PG_ARGISNULL(1) ? (Datum)0 : PG_GETARG_DATUM(1);
    state = accumArrayResult(state, elem, PG_ARGISNULL(1), arg1_typeid, aggcontext);

    /*
     * The transition type for array_agg() is declared to be "internal", which
     * is a pass-by-value type the same size as a pointer.	So we can safely
     * pass the ArrayBuildState pointer through nodeAgg.c's machinations.
     */
    PG_RETURN_POINTER(state);
}

Datum array_agg_finalfn(PG_FUNCTION_ARGS)
{
    Datum result;
    ArrayBuildState* state = NULL;
    int dims[1];
    int lbs[1];

    /*
     * Test for null before Asserting we are in right context.	This is to
     * avoid possible Assert failure in 8.4beta installations, where it is
     * possible for users to create NULL constants of type internal.
     */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL(); /* returns null iff no input values */

    /* cannot be called directly because of internal-type argument */
    Assert(AggCheckCallContext(fcinfo, NULL));

    state = (ArrayBuildState*)PG_GETARG_POINTER(0);

    dims[0] = state->nelems;
    lbs[0] = 1;

    /*
     * Make the result.  We cannot release the ArrayBuildState because
     * sometimes aggregate final functions are re-executed.  Rather, it is
     * nodeAgg.c's responsibility to reset the aggcontext when it's safe to do
     * so.
     */
    result = makeMdArrayResult(state, 1, dims, lbs, CurrentMemoryContext, false);

    PG_RETURN_DATUM(result);
}

#ifdef GS_GRAPH
/*-----------------------------------------------------------------------------
 * array_position, array_position_start :
 *			return the offset of a value in an array.
 *
 * IS NOT DISTINCT FROM semantics are used for comparisons.  Return NULL when
 * the value is not found.
 *-----------------------------------------------------------------------------
 */
Datum
array_position(PG_FUNCTION_ARGS)
{
	return array_position_common(fcinfo);
}

/*
 * array_position_common
 *		Common code for array_position and array_position_start
 *
 * These are separate wrappers for the sake of opr_sanity regression test.
 * They are not strict so we have to test for null inputs explicitly.
 */
static Datum
array_position_common(FunctionCallInfo fcinfo)
{
	ArrayType  *array;
	Oid			collation = PG_GET_COLLATION();
	Oid			element_type;
	Datum		searched_element,
				value;
	bool		isnull;
	int			position,
				position_min;
	bool		found = false;
	TypeCacheEntry *typentry;
	ArrayMetaState *my_extra;
	bool		null_search;
	ArrayIterator array_iterator;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	array = PG_GETARG_ARRAYTYPE_P(0);
	element_type = ARR_ELEMTYPE(array);

	/*
	 * We refuse to search for elements in multi-dimensional arrays, since we
	 * have no good way to report the element's location in the array.
	 */
	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("searching for elements in multidimensional arrays is not supported")));

	if (PG_ARGISNULL(1))
	{
		/* fast return when the array doesn't have nulls */
		if (!array_contains_nulls(array))
			PG_RETURN_NULL();
		searched_element = (Datum) 0;
		null_search = true;
	}
	else
	{
		searched_element = PG_GETARG_DATUM(1);
		null_search = false;
	}

	position = (ARR_LBOUND(array))[0] - 1;

	/* figure out where to start */
	if (PG_NARGS() == 3)
	{
		if (PG_ARGISNULL(2))
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("initial position must not be null")));

		position_min = PG_GETARG_INT32(2);
	}
	else
		position_min = (ARR_LBOUND(array))[0];

	/*
	 * We arrange to look up type info for array_create_iterator only once per
	 * series of calls, assuming the element type doesn't change underneath
	 * us.
	 */
	my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(ArrayMetaState));
		my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
		my_extra->element_type = ~element_type;
	}

	if (my_extra->element_type != element_type)
	{
		get_typlenbyvalalign(element_type,
							 &my_extra->typlen,
							 &my_extra->typbyval,
							 &my_extra->typalign);

		typentry = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);

		if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("could not identify an equality operator for type %s",
							format_type_be(element_type))));

		my_extra->element_type = element_type;
		fmgr_info_cxt(typentry->eq_opr_finfo.fn_oid, &my_extra->proc,
					  fcinfo->flinfo->fn_mcxt);
	}

	/* Examine each array element until we find a match. */
	array_iterator = array_create_iterator(array, 0);
	while (array_iterate(array_iterator, &value, &isnull))
	{
		position++;

		/* skip initial elements if caller requested so */
		if (position < position_min)
			continue;

		/*
		 * Can't look at the array element's value if it's null; but if we
		 * search for null, we have a hit and are done.
		 */
		if (isnull || null_search)
		{
			if (isnull && null_search)
			{
				found = true;
				break;
			}
			else
				continue;
		}

		/* not nulls, so run the operator */
		if (DatumGetBool(FunctionCall2Coll(&my_extra->proc, collation,
										   searched_element, value)))
		{
			found = true;
			break;
		}
	}

	array_free_iterator(array_iterator);

	/* Avoid leaking memory when handed toasted input */
	PG_FREE_IF_COPY(array, 0);

	if (!found)
		PG_RETURN_NULL();

	PG_RETURN_INT32(position);
}
#endif