/* -------------------------------------------------------------------------
 *
 * jsonb.cpp
 *      I/O routines for jsonb type
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/common/backend/utils/adt/jsonb.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/jsonb.h"
#ifdef GS_GRAPH
#include "utils/lsyscache.h"
#include "catalog/pg_type.h"
#include "access/transam.h"
#include "parser/parse_coerce.h"
#include "utils/date.h"
#include "utils/typcache.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/json.h"
#endif /* GS_GRAPH */

typedef struct JsonbInState {
    JsonbParseState *parseState;
    JsonbValue *res;
}   JsonbInState;


#ifdef GS_GRAPH
/* unlike with json categories, we need to treat json and jsonb differently */
typedef enum					/* type categories for datum_to_jsonb */
{
	JSONBTYPE_NULL,				/* null, so we didn't bother to identify */
	JSONBTYPE_BOOL,				/* boolean (built-in types only) */
	JSONBTYPE_NUMERIC,			/* numeric (ditto) */
	JSONBTYPE_DATE,				/* we use special formatting for datetimes */
	JSONBTYPE_TIMESTAMP,		/* we use special formatting for timestamp */
	JSONBTYPE_TIMESTAMPTZ,		/* ... and timestamptz */
	JSONBTYPE_JSON,				/* JSON */
	JSONBTYPE_JSONB,			/* JSONB */
	JSONBTYPE_ARRAY,			/* array */
	JSONBTYPE_COMPOSITE,		/* composite */
	JSONBTYPE_JSONCAST,			/* something with an explicit cast to JSON */
	JSONBTYPE_OTHER				/* all else */
} JsonbTypeCategory;
#endif /* GS_GRAPH */

static inline Datum jsonb_from_cstring(char *json, int len);
static size_t checkStringLen(size_t len);
static void jsonb_in_object_start(void *pstate);
static void jsonb_in_object_end(void *pstate);
static void jsonb_in_array_start(void *pstate);
static void jsonb_in_array_end(void *pstate);
static void jsonb_in_object_field_start(void *pstate, char *fname, bool isnull);
static void jsonb_put_escaped_value(StringInfo out, JsonbValue *scalarVal);
static void jsonb_in_scalar(void *pstate, char *token, JsonTokenType tokentype);
char *JsonbToCString(StringInfo out, char *in, int estimated_len);
#ifdef GS_GRAPH
static void array_to_jsonb_internal(Datum array, JsonbInState *result);
static void composite_to_jsonb(Datum composite, JsonbInState *result);
static void jsonb_categorize_type(Oid typoid,
					  JsonbTypeCategory *tcategory,
					  Oid *outfuncoid);
static void datum_to_jsonb(Datum val, bool is_null, JsonbInState *result,
			   JsonbTypeCategory tcategory, Oid outfuncoid,
			   bool key_scalar);
static void array_dim_to_jsonb(JsonbInState *result, int dim, int ndims, int *dims,
				   Datum *vals, bool *nulls, int *valcount,
				   JsonbTypeCategory tcategory, Oid outfuncoid);
static JsonbParseState *clone_parse_state(JsonbParseState *state);
static JsonbValue *setPath(JsonbIterator **it, Datum *path_elems,
						   bool *path_nulls, int path_len,
						   JsonbParseState **st, int level, Jsonb *newval,
						   int op_type);
static void setPathObject(JsonbIterator **it, Datum *path_elems,
						  bool *path_nulls, int path_len, JsonbParseState **st,
						  int level,
						  Jsonb *newval, uint32 npairs, int op_type);
static void setPathArray(JsonbIterator **it, Datum *path_elems,
						 bool *path_nulls, int path_len, JsonbParseState **st,
						 int level, Jsonb *newval, uint32 nelems, int op_type);
static void addJsonbToParseState(JsonbParseState **jbps, Jsonb *jb);
#endif /* GS_GRAPH */

typedef struct JsonbAggState
{
	JsonbInState *res;
	JsonbTypeCategory key_category;
	Oid			key_output_func;
	JsonbTypeCategory val_category;
	Oid			val_output_func;
} JsonbAggState;

/*
 * jsonb type input function
 */
Datum jsonb_in(PG_FUNCTION_ARGS)
{
    char *json = PG_GETARG_CSTRING(0);
    json = json == NULL ? pstrdup("") : json;

    return jsonb_from_cstring(json, strlen(json));
}

/*
 * jsonb type recv function
 *
 * The type is sent as text in binary mode, so this is almost the same
 * as the input function, but it's prefixed with a version number so we
 * can change the binary format sent in future if necessary. For now,
 * only version 1 is supported.
 */
Datum jsonb_recv(PG_FUNCTION_ARGS)
{
    StringInfo  buf = (StringInfo) PG_GETARG_POINTER(0);
    int         version = pq_getmsgint(buf, 1);
    char       *str = NULL;
    int         nbytes;

    if (version == 1)
        str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    else
        elog(ERROR, "Unsupported jsonb version number %d", version);

    return jsonb_from_cstring(str, nbytes);
}

/*
 * jsonb type output function
 */
Datum jsonb_out(PG_FUNCTION_ARGS)
{
    Jsonb      *jb = PG_GETARG_JSONB(0);
    char       *out = NULL;

    out = JsonbToCString(NULL, VARDATA(jb), VARSIZE(jb));

    PG_RETURN_CSTRING(out);
}

/*
 * jsonb type send function
 *
 * Just send jsonb as a version number, then a string of text
 */
Datum jsonb_send(PG_FUNCTION_ARGS)
{
    Jsonb      *jb = PG_GETARG_JSONB(0);
    StringInfoData buf;
    StringInfo  jtext = makeStringInfo();
    int         version = 1;

    (void) JsonbToCString(jtext, VARDATA(jb), VARSIZE(jb));

    pq_begintypsend(&buf);
    pq_sendint(&buf, version, 1);
    pq_sendtext(&buf, jtext->data, jtext->len);
    pfree(jtext->data);
    pfree(jtext);

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * SQL function jsonb_typeof(jsonb) -> text
 *
 * This function is here because the analog json function is in json.c, since
 * it uses the json parser internals not exposed elsewhere.
 */
Datum jsonb_typeof(PG_FUNCTION_ARGS)
{
    Jsonb      *in = PG_GETARG_JSONB(0);
    JsonbIterator *it = NULL;
    JsonbValue  v;
    char       *result = NULL;

    if (JB_ROOT_IS_OBJECT(in)) {
        result = "object";
    } else if (JB_ROOT_IS_ARRAY(in) && !JB_ROOT_IS_SCALAR(in)) {
        result = "array";
    } else {
        Assert(JB_ROOT_IS_SCALAR(in));
        it = JsonbIteratorInit(VARDATA_ANY(in));
        /*
         * A root scalar is stored as an array of one element, so we get the
         * array and then its first (and only) member.
         */
        (void) JsonbIteratorNext(&it, &v, true);
        Assert(v.type == jbvArray);
        (void) JsonbIteratorNext(&it, &v, true);
        switch (v.type) {
            case jbvNull:
                result = "null";
                break;
            case jbvString:
                result = "string";
                break;
            case jbvNumeric:
                result = "number";
                break;
            case jbvBool:
                result = "boolean";
                break;
            default:
                elog(ERROR, "unknown jsonb scalar type");
        }
    }

    PG_RETURN_TEXT_P(cstring_to_text(result));
}

#ifdef GS_GRAPH
/*
 * Turn a composite / record into JSON.
 */
static void
composite_to_jsonb(Datum composite, JsonbInState *result)
{
	HeapTupleHeader td;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tmptup,
			   *tuple;
	int			i;

	td = DatumGetHeapTupleHeader(composite);

	/* Extract rowtype info and find a tupdesc */
	tupType = HeapTupleHeaderGetTypeId(td);
	tupTypmod = HeapTupleHeaderGetTypMod(td);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	/* Build a temporary HeapTuple control structure */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
	tmptup.t_data = td;
	tuple = &tmptup;

	result->res = pushJsonbValue(&result->parseState, WJB_BEGIN_OBJECT, NULL);

	for (i = 0; i < tupdesc->natts; i++)
	{
		Datum		val;
		bool		isnull;
		char	   *attname;
		JsonbTypeCategory tcategory;
		Oid			outfuncoid;
		JsonbValue	v;

		if (tupdesc->attrs[i]->attisdropped)
			continue;

		attname = NameStr(tupdesc->attrs[i]->attname);

		v.type = jbvString;
		/* don't need checkStringLen here - can't exceed maximum name length */
		v.string.len = strlen(attname);
		v.estSize = sizeof(JEntry) + v.string.len;
		v.string.val = attname;

		result->res = pushJsonbValue(&result->parseState, WJB_KEY, &v);

		val = heap_getattr(tuple, i + 1, tupdesc, &isnull);

		if (isnull)
		{
			tcategory = JSONBTYPE_NULL;
			outfuncoid = InvalidOid;
		}
		else
			jsonb_categorize_type(tupdesc->attrs[i]->atttypid,
								  &tcategory, &outfuncoid);

		datum_to_jsonb(val, isnull, result, tcategory, outfuncoid, false);
	}

	result->res = pushJsonbValue(&result->parseState, WJB_END_OBJECT, NULL);
	ReleaseTupleDesc(tupdesc);
}

/*
 * Turn an array into JSON.
 */
static void
array_to_jsonb_internal(Datum array, JsonbInState *result)
{
	ArrayType  *v = DatumGetArrayTypeP(array);
	Oid			element_type = ARR_ELEMTYPE(v);
	int		   *dim;
	int			ndim;
	int			nitems;
	int			count = 0;
	Datum	   *elements;
	bool	   *nulls;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	JsonbTypeCategory tcategory;
	Oid			outfuncoid;

	ndim = ARR_NDIM(v);
	dim = ARR_DIMS(v);
	nitems = ArrayGetNItems(ndim, dim);

	if (nitems <= 0)
	{
		result->res = pushJsonbValue(&result->parseState, WJB_BEGIN_ARRAY, NULL);
		result->res = pushJsonbValue(&result->parseState, WJB_END_ARRAY, NULL);
		return;
	}

	get_typlenbyvalalign(element_type,
						 &typlen, &typbyval, &typalign);

	jsonb_categorize_type(element_type,
						  &tcategory, &outfuncoid);

	deconstruct_array(v, element_type, typlen, typbyval,
					  typalign, &elements, &nulls,
					  &nitems);

	array_dim_to_jsonb(result, 0, ndim, dim, elements, nulls, &count, tcategory,
					   outfuncoid);

	pfree(elements);
	pfree(nulls);
}

/*
 * Process a single dimension of an array.
 * If it's the innermost dimension, output the values, otherwise call
 * ourselves recursively to process the next dimension.
 */
static void
array_dim_to_jsonb(JsonbInState *result, int dim, int ndims, int *dims, Datum *vals,
				   bool *nulls, int *valcount, JsonbTypeCategory tcategory,
				   Oid outfuncoid)
{
	int			i;

	Assert(dim < ndims);

	result->res = pushJsonbValue(&result->parseState, WJB_BEGIN_ARRAY, NULL);

	for (i = 1; i <= dims[dim]; i++)
	{
		if (dim + 1 == ndims)
		{
			datum_to_jsonb(vals[*valcount], nulls[*valcount], result, tcategory,
						   outfuncoid, false);
			(*valcount)++;
		}
		else
		{
			array_dim_to_jsonb(result, dim + 1, ndims, dims, vals, nulls,
							   valcount, tcategory, outfuncoid);
		}
	}

	result->res = pushJsonbValue(&result->parseState, WJB_END_ARRAY, NULL);
}

/*
 * Determine how we want to render values of a given type in datum_to_jsonb.
 *
 * Given the datatype OID, return its JsonbTypeCategory, as well as the type's
 * output function OID.  If the returned category is JSONBTYPE_JSONCAST,
 * we return the OID of the relevant cast function instead.
 */
static void
jsonb_categorize_type(Oid typoid,
					  JsonbTypeCategory *tcategory,
					  Oid *outfuncoid)
{
	bool		typisvarlena;

	/* Look through any domain */
	typoid = getBaseType(typoid);

	*outfuncoid = InvalidOid;

	/*
	 * We need to get the output function for everything except date and
	 * timestamp types, booleans, array and composite types, json and jsonb,
	 * and non-builtin types where there's a cast to json. In this last case
	 * we return the oid of the cast function instead.
	 */

	switch (typoid)
	{
		case BOOLOID:
			*tcategory = JSONBTYPE_BOOL;
			break;

		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
			*tcategory = JSONBTYPE_NUMERIC;
			break;

		case DATEOID:
			*tcategory = JSONBTYPE_DATE;
			break;

		case TIMESTAMPOID:
			*tcategory = JSONBTYPE_TIMESTAMP;
			break;

		case TIMESTAMPTZOID:
			*tcategory = JSONBTYPE_TIMESTAMPTZ;
			break;

		case JSONBOID:
			*tcategory = JSONBTYPE_JSONB;
			break;

		case JSONOID:
			*tcategory = JSONBTYPE_JSON;
			break;

		default:
			/* Check for arrays and composites */
			if (OidIsValid(get_element_type(typoid)) || typoid == ANYARRAYOID
				|| typoid == RECORDARRAYOID)
				*tcategory = JSONBTYPE_ARRAY;
			else if (type_is_rowtype(typoid))	/* includes RECORDOID */
				*tcategory = JSONBTYPE_COMPOSITE;
			else
			{
				/* It's probably the general case ... */
				*tcategory = JSONBTYPE_OTHER;

				/*
				 * but first let's look for a cast to json (note: not to
				 * jsonb) if it's not built-in.
				 */
				if (typoid >= FirstNormalObjectId)
				{
					Oid			castfunc;
					CoercionPathType ctype;

					ctype = find_coercion_pathway(JSONOID, typoid,
												  COERCION_EXPLICIT, &castfunc);
					if (ctype == COERCION_PATH_FUNC && OidIsValid(castfunc))
					{
						*tcategory = JSONBTYPE_JSONCAST;
						*outfuncoid = castfunc;
					}
					else
					{
						/* not a cast type, so just get the usual output func */
						getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
					}
				}
				else
				{
					/* any other builtin type */
					getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
				}
				break;
			}
	}
}

/*
 * Turn a Datum into jsonb, adding it to the result JsonbInState.
 *
 * tcategory and outfuncoid are from a previous call to json_categorize_type,
 * except that if is_null is true then they can be invalid.
 *
 * If key_scalar is true, the value is stored as a key, so insist
 * it's of an acceptable type, and force it to be a jbvString.
 */
static void
datum_to_jsonb(Datum val, bool is_null, JsonbInState *result,
			   JsonbTypeCategory tcategory, Oid outfuncoid,
			   bool key_scalar)
{
	char	   *outputstr;
	bool		numeric_error;
	JsonbValue	jb;
	bool		scalar_jsonb = false;

	check_stack_depth();

	/* Convert val to a JsonbValue in jb (in most cases) */
	if (is_null)
	{
		Assert(!key_scalar);
		jb.type = jbvNull;
		jb.estSize = sizeof(JEntry);
	}
	else if (key_scalar &&
			 (tcategory == JSONBTYPE_ARRAY ||
			  tcategory == JSONBTYPE_COMPOSITE ||
			  tcategory == JSONBTYPE_JSON ||
			  tcategory == JSONBTYPE_JSONB ||
			  tcategory == JSONBTYPE_JSONCAST))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("key value must be scalar, not array, composite, or json")));
	}
	else
	{
		if (tcategory == JSONBTYPE_JSONCAST)
			val = OidFunctionCall1(outfuncoid, val);

		switch (tcategory)
		{
			case JSONBTYPE_ARRAY:
				array_to_jsonb_internal(val, result);
				break;
			case JSONBTYPE_COMPOSITE:
				composite_to_jsonb(val, result);
				break;
			case JSONBTYPE_BOOL:
				if (key_scalar)
				{
					outputstr = (char* )(DatumGetBool(val) ? "true" : "false");
					jb.type = jbvString;
					jb.string.len = strlen(outputstr);
					jb.estSize = sizeof(JEntry) + jb.string.len;
					jb.string.val = outputstr;
				}
				else
				{
					jb.type = jbvBool;
					jb.estSize = sizeof(JEntry);
					jb.boolean = DatumGetBool(val);
				}
				break;
			case JSONBTYPE_NUMERIC:
				outputstr = OidOutputFunctionCall(outfuncoid, val);
				if (key_scalar)
				{
					/* always quote keys */
					jb.type = jbvString;
					jb.string.len = strlen(outputstr);
					jb.estSize = sizeof(JEntry) + jb.string.len;
					jb.string.val = outputstr;
				}
				else
				{
					/*
					 * Make it numeric if it's a valid JSON number, otherwise
					 * a string. Invalid numeric output will always have an
					 * 'N' or 'n' in it (I think).
					 */
					numeric_error = (strchr(outputstr, 'N') != NULL ||
									 strchr(outputstr, 'n') != NULL);
					if (!numeric_error)
					{
						jb.type = jbvNumeric;
						jb.numeric = DatumGetNumeric(DirectFunctionCall3(numeric_in, CStringGetDatum(outputstr), 0, -1));
						jb.estSize = 2 * sizeof(JEntry) + VARSIZE_ANY(jb.numeric);
						pfree(outputstr);
					}
					else
					{
						jb.type = jbvString;
						jb.string.len = strlen(outputstr);
						jb.estSize = sizeof(JEntry) + jb.string.len;
						jb.string.val = outputstr;
					}
				}
				break;
			case JSONBTYPE_DATE:
				{
					DateADT		date;
					struct pg_tm tm;
					char		buf[MAXDATELEN + 1];

					date = DatumGetDateADT(val);
					/* Same as date_out(), but forcing DateStyle */
					if (DATE_NOT_FINITE(date))
						call_EncodeSpecialDate(date, buf, strlen(buf));
					else
					{
						j2date(date + POSTGRES_EPOCH_JDATE,
							   &(tm.tm_year), &(tm.tm_mon), &(tm.tm_mday));
						EncodeDateOnly(&tm, USE_XSD_DATES, buf);
					}
					jb.type = jbvString;
					jb.string.len = strlen(buf);
					jb.estSize = sizeof(JEntry) + jb.string.len;
					jb.string.val = pstrdup(buf);
				}
				break;
			case JSONBTYPE_TIMESTAMP:
				{
					Timestamp	timestamp;
					struct pg_tm tm;
					fsec_t		fsec;
					char		buf[MAXDATELEN + 1];

					timestamp = DatumGetTimestamp(val);
					/* Same as timestamp_out(), but forcing DateStyle */
					if (TIMESTAMP_NOT_FINITE(timestamp))
						call_EncodeSpecialTimestamp(timestamp, buf);
					else if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL) == 0)
						EncodeDateTime(&tm, fsec, false, 0, NULL, USE_XSD_DATES, buf);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("timestamp out of range")));
					jb.type = jbvString;
					jb.string.len = strlen(buf);
					jb.estSize = sizeof(JEntry) + jb.string.len;
					jb.string.val = pstrdup(buf);
				}
				break;
			case JSONBTYPE_TIMESTAMPTZ:
				{
					TimestampTz timestamp;
					struct pg_tm tm;
					int			tz;
					fsec_t		fsec;
					const char *tzn = NULL;
					char		buf[MAXDATELEN + 1];

					timestamp = DatumGetTimestampTz(val);
					/* Same as timestamptz_out(), but forcing DateStyle */
					if (TIMESTAMP_NOT_FINITE(timestamp))
						call_EncodeSpecialTimestamp(timestamp, buf);
					else if (timestamp2tm(timestamp, &tz, &tm, &fsec, &tzn, NULL) == 0)
						EncodeDateTime(&tm, fsec, true, tz, tzn, USE_XSD_DATES, buf);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("timestamp out of range")));
					jb.type = jbvString;
					jb.string.len = strlen(buf);
					jb.estSize = sizeof(JEntry) + jb.string.len;
					jb.string.val = pstrdup(buf);
				}
				break;
			case JSONBTYPE_JSONCAST:
			case JSONBTYPE_JSON:
				{
					/* parse the json right into the existing result object */
					JsonLexContext *lex;
					JsonSemAction sem;
					text	   *json = DatumGetTextPP(val);

					lex = makeJsonLexContext(json, true);

					memset(&sem, 0, sizeof(sem));

					sem.semstate = (void *) result;

					sem.object_start = jsonb_in_object_start;
					sem.array_start = jsonb_in_array_start;
					sem.object_end = jsonb_in_object_end;
					sem.array_end = jsonb_in_array_end;
					sem.scalar = jsonb_in_scalar;
					sem.object_field_start = jsonb_in_object_field_start;

					pg_parse_json(lex, &sem);

				}
				break;
			case JSONBTYPE_JSONB:
				{
					Jsonb	   *jsonb = DatumGetJsonb(val);
					JsonbIterator *it;

					it = JsonbIteratorInit(VARDATA(jsonb));

					if (JB_ROOT_IS_SCALAR(jsonb))
					{
						(void) JsonbIteratorNext(&it, &jb, true);
						Assert(jb.type == jbvArray);
						(void) JsonbIteratorNext(&it, &jb, true);
						scalar_jsonb = true;
					}
					else
					{
						int type;

						while ((type = JsonbIteratorNext(&it, &jb, false))
							   != WJB_DONE)
						{
							if (type == WJB_END_ARRAY || type == WJB_END_OBJECT ||
								type == WJB_BEGIN_ARRAY || type == WJB_BEGIN_OBJECT)
								result->res = pushJsonbValue(&result->parseState,
															 type, NULL);
							else
								result->res = pushJsonbValue(&result->parseState,
															 type, &jb);
						}
					}
				}
				break;
			default:
				outputstr = OidOutputFunctionCall(outfuncoid, val);
				jb.type = jbvString;
				jb.string.len = checkStringLen(strlen(outputstr));
				jb.estSize = sizeof(JEntry) + jb.string.len;
				jb.string.val = outputstr;
				break;
		}
	}

	/* Now insert jb into result, unless we did it recursively */
	if (!is_null && !scalar_jsonb &&
		tcategory >= JSONBTYPE_JSON && tcategory <= JSONBTYPE_JSONCAST)
	{
		/* work has been done recursively */
		return;
	}
	else if (result->parseState == NULL)
	{
		/* single root scalar */
		JsonbValue	va;

		va.type = jbvArray;
		va.array.rawScalar = true;
		va.array.nElems = 1;

		result->res = pushJsonbValue(&result->parseState, WJB_BEGIN_ARRAY, &va);
		result->res = pushJsonbValue(&result->parseState, WJB_ELEM, &jb);
		result->res = pushJsonbValue(&result->parseState, WJB_END_ARRAY, NULL);
	}
	else
	{
		JsonbValue *o = &result->parseState->contVal;

		switch (o->type)
		{
			case jbvArray:
				result->res = pushJsonbValue(&result->parseState, WJB_ELEM, &jb);
				break;
			case jbvObject:
				result->res = pushJsonbValue(&result->parseState,
											 key_scalar ? WJB_KEY : WJB_VALUE,
											 &jb);
				break;
			default:
				elog(ERROR, "unexpected parent of nested structure");
		}
	}
}


/*
 * SQL function to_jsonb(anyvalue)
 */
Datum
to_jsonb(PG_FUNCTION_ARGS)
{
	Datum		val = PG_GETARG_DATUM(0);
	Oid			val_type = get_fn_expr_argtype(fcinfo->flinfo, 0);
	JsonbInState result;
	JsonbTypeCategory tcategory;
	Oid			outfuncoid;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	jsonb_categorize_type(val_type,
						  &tcategory, &outfuncoid);

	memset(&result, 0, sizeof(JsonbInState));

	datum_to_jsonb(val, false, &result, tcategory, outfuncoid, false);

	PG_RETURN_POINTER(JsonbValueToJsonb(result.res));
}
/*
 * SQL function jsonb_set(jsonb, text[], jsonb, boolean)
 */
Datum
jsonb_set(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
	Jsonb	   *newval = PG_GETARG_JSONB_P(2);
	bool		create = PG_GETARG_BOOL(3);
	JsonbValue *res = NULL;
	Datum	   *path_elems;
	bool	   *path_nulls;
	int			path_len;
	JsonbIterator *it;
	JsonbParseState *st = NULL;

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot set path in scalar")));

	if (JB_ROOT_COUNT(in) == 0 && !create)
		PG_RETURN_JSONB_P(in);

	deconstruct_array(path, TEXTOID, -1, false, TYPALIGN_INT,
					  &path_elems, &path_nulls, &path_len);

	if (path_len == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(VARDATA(in));

	res = setPath(&it, path_elems, path_nulls, path_len, &st,
				  0, newval, create ? JB_PATH_CREATE : JB_PATH_REPLACE);

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}

/*
 * SQL function jsonb_delete_path(jsonb, text[])
 */
Datum
jsonb_delete_path(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
	JsonbValue *res = NULL;
	Datum	   *path_elems;
	bool	   *path_nulls;
	int			path_len;
	JsonbIterator *it;
	JsonbParseState *st = NULL;

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot delete path in scalar")));

	if (JB_ROOT_COUNT(in) == 0)
		PG_RETURN_JSONB_P(in);

	deconstruct_array(path, TEXTOID, -1, false, TYPALIGN_INT,
					  &path_elems, &path_nulls, &path_len);

	if (path_len == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(VARDATA(in));

	res = setPath(&it, path_elems, path_nulls, path_len, &st,
				  0, NULL, JB_PATH_DELETE);

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}
#endif /* GS_GRAPH */

/*
 * jsonb_from_cstring
 *
 * Turns json string into a jsonb Datum.
 *
 * Uses the json parser (with hooks) to construct a jsonb.
 */
static inline Datum jsonb_from_cstring(char *json, int len)
{
    JsonLexContext *lex = NULL;
    JsonbInState state;
    JsonSemAction sem;

    errno_t rc = memset_s(&state, sizeof(state), 0, sizeof(state));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&sem, sizeof(sem), 0, sizeof(sem));
    securec_check(rc, "\0", "\0");
    lex = makeJsonLexContextCstringLen(json, len, true);

    sem.semstate = (void *) &state;
    sem.object_start = jsonb_in_object_start;
    sem.array_start = jsonb_in_array_start;
    sem.object_end = jsonb_in_object_end;
    sem.array_end = jsonb_in_array_end;
    sem.scalar = jsonb_in_scalar;
    sem.object_field_start = jsonb_in_object_field_start;

    pg_parse_json(lex, &sem);

    /* after parsing, the item member has the composed jsonb structure */
    PG_RETURN_POINTER(JsonbValueToJsonb(state.res));
}

static size_t checkStringLen(size_t len)
{
    if (len > JENTRY_POSMASK) {
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("string too long to represent as jsonb string"),
                 errdetail("Due to an implementation restriction, jsonb strings cannot exceed %d bytes.",
                           JENTRY_POSMASK)));
    }

    return len;
}

static void jsonb_in_object_start(void *pstate)
{
    JsonbInState *_state = (JsonbInState *) pstate;
    _state->res = pushJsonbValue(&_state->parseState, WJB_BEGIN_OBJECT, NULL);
}

static void jsonb_in_object_end(void *pstate)
{
    JsonbInState *_state = (JsonbInState *) pstate;
    _state->res = pushJsonbValue(&_state->parseState, WJB_END_OBJECT, NULL);
}

static void jsonb_in_array_start(void *pstate)
{
    JsonbInState *_state = (JsonbInState *) pstate;
    _state->res = pushJsonbValue(&_state->parseState, WJB_BEGIN_ARRAY, NULL);
}

static void jsonb_in_array_end(void *pstate)
{
    JsonbInState *_state = (JsonbInState *) pstate;
    _state->res = pushJsonbValue(&_state->parseState, WJB_END_ARRAY, NULL);
}

static void jsonb_in_object_field_start(void *pstate, char *fname, bool isnull)
{
    JsonbInState *_state = (JsonbInState *) pstate;
    JsonbValue  v;

    Assert (fname != NULL);
    v.type = jbvString;
    v.string.len = checkStringLen(strlen(fname));
    v.string.val = pnstrdup(fname, v.string.len);
    v.estSize = sizeof(JEntry) + v.string.len;

    _state->res = pushJsonbValue(&_state->parseState, WJB_KEY, &v);
}

static void jsonb_put_escaped_value(StringInfo out, JsonbValue * scalarVal)
{
    switch (scalarVal->type) {
        case jbvNull:
            appendBinaryStringInfo(out, "null", 4);
            break;
        case jbvString:
            escape_json(out, pnstrdup(scalarVal->string.val, scalarVal->string.len));
            break;
        case jbvNumeric:
            appendStringInfoString(out,
                                   DatumGetCString(DirectFunctionCall1(numeric_out,
                                                                       PointerGetDatum(scalarVal->numeric))));
            break;
        case jbvBool:
            if (scalarVal->boolean)
                appendBinaryStringInfo(out, "true", 4);
            else
                appendBinaryStringInfo(out, "false", 5);
            break;
        default:
            elog(ERROR, "unknown jsonb scalar type");
    }
}

/*
 * For jsonb we always want the de-escaped value - that's what's in token
 */
static void jsonb_in_scalar(void *pstate, char *token, JsonTokenType tokentype)
{
    JsonbInState *_state = (JsonbInState *) pstate;
    JsonbValue  v;

    v.estSize = sizeof(JEntry);
    switch (tokentype) {
        case JSON_TOKEN_STRING:
            Assert (token != NULL);
            v.type = jbvString;
            v.string.len = checkStringLen(strlen(token));
            v.string.val = pnstrdup(token, v.string.len);
            v.estSize += v.string.len;
            break;
        case JSON_TOKEN_NUMBER:
            /*
             * No need to check size of numeric values, because maximum numeric
             * size is well below the JsonbValue restriction
             */
            Assert (token != NULL);
            v.type = jbvNumeric;
            v.numeric = DatumGetNumeric(DirectFunctionCall3(numeric_in, CStringGetDatum(token), 0, -1));
            v.estSize += VARSIZE_ANY(v.numeric) + sizeof(JEntry);  /* alignment */
            break;
        case JSON_TOKEN_TRUE:
            v.type = jbvBool;
            v.boolean = true;
            break;
        case JSON_TOKEN_FALSE:
            v.type = jbvBool;
            v.boolean = false;
            break;
        case JSON_TOKEN_NULL:
            v.type = jbvNull;
            break;
        default:
            /* should not be possible */
            elog(ERROR, "invalid json token type");
            break;
    }

    if (_state->parseState == NULL) {
        /* single scalar */
        JsonbValue  va;

        va.type = jbvArray;
        va.array.rawScalar = true;
        va.array.nElems = 1;

        _state->res = pushJsonbValue(&_state->parseState, WJB_BEGIN_ARRAY, &va);
        _state->res = pushJsonbValue(&_state->parseState, WJB_ELEM, &v);
        _state->res = pushJsonbValue(&_state->parseState, WJB_END_ARRAY, NULL);
    } else {
        JsonbValue *o = &_state->parseState->contVal;
        switch (o->type) {
            case jbvArray:
                _state->res = pushJsonbValue(&_state->parseState, WJB_ELEM, &v);
                break;
            case jbvObject:
                _state->res = pushJsonbValue(&_state->parseState, WJB_VALUE, &v);
                break;
            default:
                elog(ERROR, "unexpected parent of nested structure");
        }
    }
}

/*
 * JsonbToCString
 *     Converts jsonb value to a C-string.
 *
 * If 'out' argument is non-null, the resulting C-string is stored inside the
 * StringBuffer.  The resulting string is always returned.
 *
 * A typical case for passing the StringInfo in rather than NULL is where the
 * caller wants access to the len attribute without having to call strlen, e.g.
 * if they are converting it to a text* object.
 */
char *JsonbToCString(StringInfo out, JsonbSuperHeader in, int estimated_len)
{
    bool        first = true;
    JsonbIterator *it = NULL;
    int         type = 0;
    JsonbValue  v;
    int         level = 0;
    bool        redo_switch = false;

    if (out == NULL)
        out = makeStringInfo();

    enlargeStringInfo(out, (estimated_len >= 0) ? estimated_len : 64);
    it = JsonbIteratorInit(in);

    while (redo_switch || ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)) {
        redo_switch = false;
        switch (type) {
            case WJB_BEGIN_ARRAY:
                if (!first)
                    appendBinaryStringInfo(out, ", ", 2);
                first = true;

                if (!v.array.rawScalar)
                    appendStringInfoChar(out, '[');
                level++;
                break;
            case WJB_BEGIN_OBJECT:
                if (!first)
                    appendBinaryStringInfo(out, ", ", 2);
                first = true;
                appendStringInfoCharMacro(out, '{');

                level++;
                break;
            case WJB_KEY:
                if (!first)
                    appendBinaryStringInfo(out, ", ", 2);
                first = true;

                /* json rules guarantee this is a string */
                jsonb_put_escaped_value(out, &v);
                appendBinaryStringInfo(out, ": ", 2);

                type = JsonbIteratorNext(&it, &v, false);
                if (type == WJB_VALUE) {
                    first = false;
                    jsonb_put_escaped_value(out, &v);
                } else {
                    Assert(type == WJB_BEGIN_OBJECT || type == WJB_BEGIN_ARRAY);
                    /*
                     * We need to rerun the current switch() since we need to
                     * output the object which we just got from the iterator
                     * before calling the iterator again.
                     */
                    redo_switch = true;
                }
                break;
            case WJB_ELEM:
                if (!first)
                    appendBinaryStringInfo(out, ", ", 2);
                else
                    first = false;
                jsonb_put_escaped_value(out, &v);
                break;
            case WJB_END_ARRAY:
                level--;
                if (!v.array.rawScalar)
                    appendStringInfoChar(out, ']');
                first = false;
                break;
            case WJB_END_OBJECT:
                level--;
                appendStringInfoCharMacro(out, '}');
                first = false;
                break;
            default:
                elog(ERROR, "unknown flag of jsonb iterator");
        }
    }
    Assert(level == 0);
    return out->data;
}

#ifdef GS_GRAPH
/*
 * degenerate case of jsonb_build_object where it gets 0 arguments.
 */
Datum
jsonb_build_object_noargs(PG_FUNCTION_ARGS)
{
	JsonbInState result;

	memset(&result, 0, sizeof(JsonbInState));

	(void) pushJsonbValue(&result.parseState, WJB_BEGIN_OBJECT, NULL);
	result.res = pushJsonbValue(&result.parseState, WJB_END_OBJECT, NULL);

	PG_RETURN_POINTER(JsonbValueToJsonb(result.res));
}

/*
 * jsonb_agg aggregate function
 */
Datum
jsonb_agg_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext oldcontext,
				aggcontext;
	JsonbAggState *state;
	JsonbInState elem;
	Datum		val;
	JsonbInState *result;
	bool		single_scalar = false;
	JsonbIterator *it;
	Jsonb	   *jbelem;
	JsonbValue	v;
	int type;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "jsonb_agg_transfn called in non-aggregate context");
	}

	/* set up the accumulator on the first go round */

	if (PG_ARGISNULL(0))
	{
		Oid			arg_type = get_fn_expr_argtype(fcinfo->flinfo, 1);

		if (arg_type == InvalidOid)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not determine input data type")));

		oldcontext = MemoryContextSwitchTo(aggcontext);
		state = (JsonbAggState*)palloc(sizeof(JsonbAggState));
		result = (JsonbInState*)palloc0(sizeof(JsonbInState));
		state->res = result;
		result->res = pushJsonbValue(&result->parseState,
									 WJB_BEGIN_ARRAY, NULL);
		MemoryContextSwitchTo(oldcontext);

		jsonb_categorize_type(arg_type, &state->val_category,
							  &state->val_output_func);
	}
	else
	{
		state = (JsonbAggState *) PG_GETARG_POINTER(0);
		result = state->res;
	}

	/* turn the argument into jsonb in the normal function context */

	val = PG_ARGISNULL(1) ? (Datum) 0 : PG_GETARG_DATUM(1);

	memset(&elem, 0, sizeof(JsonbInState));

	datum_to_jsonb(val, PG_ARGISNULL(1), &elem, state->val_category,
				   state->val_output_func, false);

	jbelem = JsonbValueToJsonb(elem.res);

	/* switch to the aggregate context for accumulation operations */

	oldcontext = MemoryContextSwitchTo(aggcontext);

	it = JsonbIteratorInit(VARDATA(jbelem));

	while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		switch (type)
		{
			case WJB_BEGIN_ARRAY:
				if (v.array.rawScalar)
					single_scalar = true;
				else
					result->res = pushJsonbValue(&result->parseState,
												 type, NULL);
				break;
			case WJB_END_ARRAY:
				if (!single_scalar)
					result->res = pushJsonbValue(&result->parseState,
												 type, NULL);
				break;
			case WJB_BEGIN_OBJECT:
			case WJB_END_OBJECT:
				result->res = pushJsonbValue(&result->parseState,
											 type, NULL);
				break;
			case WJB_ELEM:
			case WJB_KEY:
			case WJB_VALUE:
				if (v.type == jbvString)
				{
					/* copy string values in the aggregate context */
					char	   *buf = (char*)palloc(v.string.len + 1);

					snprintf(buf, v.string.len + 1, "%s", v.string.val);
					v.string.val = buf;
				}
				else if (v.type == jbvNumeric)
				{
					/* same for numeric */
					v.numeric =
						DatumGetNumeric(DirectFunctionCall1(numeric_uplus,
															NumericGetDatum(v.numeric)));
				}
				result->res = pushJsonbValue(&result->parseState,
											 type, &v);
				break;
			default:
				elog(ERROR, "unknown jsonb iterator token type");
		}
	}

	MemoryContextSwitchTo(oldcontext);

	PG_RETURN_POINTER(state);
}

Datum
jsonb_agg_finalfn(PG_FUNCTION_ARGS)
{
	JsonbAggState *arg;
	JsonbInState result;
	Jsonb	   *out;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();		/* returns null iff no input values */

	arg = (JsonbAggState *) PG_GETARG_POINTER(0);

	/*
	 * We need to do a shallow clone of the argument in case the final
	 * function is called more than once, so we avoid changing the argument. A
	 * shallow clone is sufficient as we aren't going to change any of the
	 * values, just add the final array end marker.
	 */

	result.parseState = clone_parse_state(arg->res->parseState);

	result.res = pushJsonbValue(&result.parseState,
								WJB_END_ARRAY, NULL);

	out = JsonbValueToJsonb(result.res);

	PG_RETURN_POINTER(out);
}

/*
 * shallow clone of a parse state, suitable for use in aggregate
 * final functions that will only append to the values rather than
 * change them.
 */
static JsonbParseState *
clone_parse_state(JsonbParseState *state)
{
	JsonbParseState *result,
			   *icursor,
			   *ocursor;

	if (state == NULL)
		return NULL;

	result = (JsonbParseState*)palloc(sizeof(JsonbParseState));
	icursor = state;
	ocursor = result;
	for (;;)
	{
		ocursor->contVal = icursor->contVal;
		ocursor->size = icursor->size;
		icursor = icursor->next;
		if (icursor == NULL)
			break;
		ocursor->next = (JsonbParseState*)palloc(sizeof(JsonbParseState));
		ocursor = ocursor->next;
	}
	ocursor->next = NULL;

	return result;
}

/*
 * Do most of the heavy work for jsonb_set/jsonb_insert
 *
 * If JB_PATH_DELETE bit is set in op_type, the element is to be removed.
 *
 * If any bit mentioned in JB_PATH_CREATE_OR_INSERT is set in op_type,
 * we create the new value if the key or array index does not exist.
 *
 * Bits JB_PATH_INSERT_BEFORE and JB_PATH_INSERT_AFTER in op_type
 * behave as JB_PATH_CREATE if new value is inserted in JsonbObject.
 *
 * All path elements before the last must already exist
 * whatever bits in op_type are set, or nothing is done.
 */
static JsonbValue *
setPath(JsonbIterator **it, Datum *path_elems,
		bool *path_nulls, int path_len,
		JsonbParseState **st, int level, Jsonb *newval, int op_type)
{
	JsonbValue	v;
	int r;
	JsonbValue *res;

	check_stack_depth();

	if (path_nulls[level])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("path element at position %d is null",
						level + 1)));

	r = JsonbIteratorNext(it, &v, false);

	switch (r)
	{
		case WJB_BEGIN_ARRAY:
			(void) pushJsonbValue(st, r, NULL);
			setPathArray(it, path_elems, path_nulls, path_len, st, level,
						 newval, v.array.nElems, op_type);
			r = JsonbIteratorNext(it, &v, false);
			Assert(r == WJB_END_ARRAY);
			res = pushJsonbValue(st, r, NULL);
			break;
		case WJB_BEGIN_OBJECT:
			(void) pushJsonbValue(st, r, NULL);
			setPathObject(it, path_elems, path_nulls, path_len, st, level,
						  newval, v.object.nPairs, op_type);
			r = JsonbIteratorNext(it, &v, true);
			Assert(r == WJB_END_OBJECT);
			res = pushJsonbValue(st, r, NULL);
			break;
		case WJB_ELEM:
		case WJB_VALUE:
			res = pushJsonbValue(st, r, &v);
			break;
		default:
			elog(ERROR, "unrecognized iterator result: %d", (int) r);
			res = NULL;			/* keep compiler quiet */
			break;
	}

	return res;
}

/*
 * Array walker for setPath
 */
static void
setPathArray(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
			 int path_len, JsonbParseState **st, int level,
			 Jsonb *newval, uint32 nelems, int op_type)
{
	JsonbValue	v;
	int			idx,
				i;
	bool		done = false;

	/* pick correct index */
	if (level < path_len && !path_nulls[level])
	{
		char	   *c = TextDatumGetCString(path_elems[level]);
		long		lindex;
		char	   *badp;

		errno = 0;
		lindex = strtol(c, &badp, 10);
		if (errno != 0 || badp == c || *badp != '\0' || lindex > INT_MAX ||
			lindex < INT_MIN)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("path element at position %d is not an integer: \"%s\"",
							level + 1, c)));
		idx = lindex;
	}
	else
		idx = nelems;

	if (idx < 0)
	{
		if (-idx > nelems)
			idx = INT_MIN;
		else
			idx = nelems + idx;
	}

	if (idx > 0 && idx > nelems)
		idx = nelems;

	/*
	 * if we're creating, and idx == INT_MIN, we prepend the new value to the
	 * array also if the array is empty - in which case we don't really care
	 * what the idx value is
	 */

	if ((idx == INT_MIN || nelems == 0) && (level == path_len - 1) &&
		(op_type & JB_PATH_CREATE_OR_INSERT))
	{
		Assert(newval != NULL);
		addJsonbToParseState(st, newval);
		done = true;
	}

	/* iterate over the array elements */
	for (i = 0; i < nelems; i++)
	{
		int r;

		if (i == idx && level < path_len)
		{
			if (level == path_len - 1)
			{
				r = JsonbIteratorNext(it, &v, true);	/* skip */

				if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_CREATE))
					addJsonbToParseState(st, newval);

				/*
				 * We should keep current value only in case of
				 * JB_PATH_INSERT_BEFORE or JB_PATH_INSERT_AFTER because
				 * otherwise it should be deleted or replaced
				 */
				if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_INSERT_BEFORE))
					(void) pushJsonbValue(st, r, &v);

				if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_REPLACE))
					addJsonbToParseState(st, newval);

				done = true;
			}
			else
				(void) setPath(it, path_elems, path_nulls, path_len,
							   st, level + 1, newval, op_type);
		}
		else
		{
			r = JsonbIteratorNext(it, &v, false);

			(void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);

			if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
			{
				int			walking_level = 1;

				while (walking_level != 0)
				{
					r = JsonbIteratorNext(it, &v, false);

					if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
						++walking_level;
					if (r == WJB_END_ARRAY || r == WJB_END_OBJECT)
						--walking_level;

					(void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
				}
			}

			if ((op_type & JB_PATH_CREATE_OR_INSERT) && !done &&
				level == path_len - 1 && i == nelems - 1)
			{
				addJsonbToParseState(st, newval);
			}
		}
	}
}

/*
 * Object walker for setPath
 */
static void
setPathObject(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
			  int path_len, JsonbParseState **st, int level,
			  Jsonb *newval, uint32 npairs, int op_type)
{
	JsonbValue	v;
	int			i;
	JsonbValue	k;
	bool		done = false;

	if (level >= path_len || path_nulls[level])
		done = true;

	/* empty object is a special case for create */
	if ((npairs == 0) && (op_type & JB_PATH_CREATE_OR_INSERT) &&
		(level == path_len - 1))
	{
		JsonbValue	newkey;

		newkey.type = jbvString;
		newkey.string.len = VARSIZE_ANY_EXHDR(path_elems[level]);
		newkey.string.val = VARDATA_ANY(path_elems[level]);
		newkey.estSize = sizeof(JEntry) + newkey.string.len;
		(void) pushJsonbValue(st, WJB_KEY, &newkey);
		addJsonbToParseState(st, newval);
	}

	for (i = 0; i < npairs; i++)
	{
		int r = JsonbIteratorNext(it, &k, true);

		Assert(r == WJB_KEY);

		if (!done &&
			k.string.len == VARSIZE_ANY_EXHDR(path_elems[level]) &&
			memcmp(k.string.val, VARDATA_ANY(path_elems[level]),
				   k.string.len) == 0)
		{
			if (level == path_len - 1)
			{
				/*
				 * called from jsonb_insert(), it forbids redefining an
				 * existing value
				 */
				if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("cannot replace existing key"),
							 errhint("Try using the function jsonb_set "
									 "to replace key value.")));

				r = JsonbIteratorNext(it, &v, true);	/* skip value */
				if (!(op_type & JB_PATH_DELETE))
				{
					(void) pushJsonbValue(st, WJB_KEY, &k);
					addJsonbToParseState(st, newval);
				}
				done = true;
			}
			else
			{
				(void) pushJsonbValue(st, r, &k);
				setPath(it, path_elems, path_nulls, path_len,
						st, level + 1, newval, op_type);
			}
		}
		else
		{
			if ((op_type & JB_PATH_CREATE_OR_INSERT) && !done &&
				level == path_len - 1 && i == npairs - 1)
			{
				JsonbValue	newkey;

				newkey.type = jbvString;
				newkey.string.len = VARSIZE_ANY_EXHDR(path_elems[level]);
				newkey.string.val = VARDATA_ANY(path_elems[level]);
				newkey.estSize = sizeof(JEntry) + newkey.string.len;
				(void) pushJsonbValue(st, WJB_KEY, &newkey);
				addJsonbToParseState(st, newval);
			}

			(void) pushJsonbValue(st, r, &k);
			r = JsonbIteratorNext(it, &v, false);
			(void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
			if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
			{
				int			walking_level = 1;

				while (walking_level != 0)
				{
					r = JsonbIteratorNext(it, &v, false);

					if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
						++walking_level;
					if (r == WJB_END_ARRAY || r == WJB_END_OBJECT)
						--walking_level;

					(void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
				}
			}
		}
	}
}

/*
 * Add values from the jsonb to the parse state.
 *
 * If the parse state container is an object, the jsonb is pushed as
 * a value, not a key.
 *
 * This needs to be done using an iterator because pushJsonbValue doesn't
 * like getting jbvBinary values, so we can't just push jb as a whole.
 */
static void
addJsonbToParseState(JsonbParseState **jbps, Jsonb *jb)
{
	JsonbIterator *it;
	JsonbValue *o = &(*jbps)->contVal;
	JsonbValue	v;
	int type;

	it = JsonbIteratorInit(VARDATA(jb));

	Assert(o->type == jbvArray || o->type == jbvObject);

	if (JB_ROOT_IS_SCALAR(jb))
	{
		(void) JsonbIteratorNext(&it, &v, false);	/* skip array header */
		Assert(v.type == jbvArray);
		(void) JsonbIteratorNext(&it, &v, false);	/* fetch scalar value */

		switch (o->type)
		{
			case jbvArray:
				(void) pushJsonbValue(jbps, WJB_ELEM, &v);
				break;
			case jbvObject:
				(void) pushJsonbValue(jbps, WJB_VALUE, &v);
				break;
			default:
				elog(ERROR, "unexpected parent of nested structure");
		}
	}
	else
	{
		while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
		{
			if (type == WJB_KEY || type == WJB_VALUE || type == WJB_ELEM)
				(void) pushJsonbValue(jbps, type, &v);
			else
				(void) pushJsonbValue(jbps, type, NULL);
		}
	}

}

#endif