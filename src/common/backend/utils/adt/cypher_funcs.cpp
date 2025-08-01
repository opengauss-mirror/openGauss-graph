/*
 * cypher_funcs.c
 *	  Functions in Cypher expressions.
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/cypher_funcs.c
 */

#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/cypher_funcs.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include <string.h>

static Datum get_numeric_10_datum(void);

#define FUNC_JSONB_MAX_ARGS 3

typedef struct FunctionCallJsonbInfo
{
	PGFunction	fn;
	const char *funcname;
	int			nargs;
	Jsonb	   *args[FUNC_JSONB_MAX_ARGS];
	Oid			argtypes[FUNC_JSONB_MAX_ARGS];
	Oid			rettype;
} FunctionCallJsonbInfo;

static Jsonb *FunctionCallJsonb(FunctionCallJsonbInfo *fcjinfo);
static Datum jsonb_to_datum(Jsonb *j, Oid type);
static bool is_numeric_integer(Numeric n);
static void ereport_invalid_jsonb_param(FunctionCallJsonbInfo *fcjinfo);
static char *type_to_jsonb_type_str(Oid type);
static Jsonb *datum_to_jsonb(Datum d, Oid type);

Datum
jsonb_head(PG_FUNCTION_ARGS)
{
	Jsonb	   *j = PG_GETARG_JSONB(0);
	JsonbValue *jv;

	if (!JB_ROOT_IS_ARRAY(j) || JB_ROOT_IS_SCALAR(j))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("head(): list is expected but %s",
						JsonbToCString(NULL, (JsonbSuperHeader)&j->root, VARSIZE(j)))));

	jv = getIthJsonbValueFromContainer(&j->root, 0);
	if (jv == NULL)
		PG_RETURN_NULL();

	PG_RETURN_JSONB(JsonbValueToJsonb(jv));
}

Datum
jsonb_last(PG_FUNCTION_ARGS)
{
	Jsonb	   *j = PG_GETARG_JSONB(0);
	uint32		cnt;
	JsonbValue *jv;

	if (!JB_ROOT_IS_ARRAY(j) || JB_ROOT_IS_SCALAR(j))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("last(): list is expected but %s",
						JsonbToCString(NULL, (JsonbSuperHeader)&j->root, VARSIZE(j)))));

	cnt = JB_ROOT_COUNT(j);
	if (cnt == 0)
		PG_RETURN_NULL();

	jv = getIthJsonbValueFromContainer(&j->root, cnt - 1);

	PG_RETURN_JSONB(JsonbValueToJsonb(jv));
}

Datum
jsonb_length(PG_FUNCTION_ARGS)
{
	Jsonb	   *j = PG_GETARG_JSONB(0);
	int			cnt = -1;
	Datum		n;
	JsonbValue	jv;

	if (JB_ROOT_IS_SCALAR(j))
	{
		JsonbValue *sjv;

		sjv = getIthJsonbValueFromContainer(&j->root, 0);
		if (sjv->type == jbvString)
			cnt = sjv->string.len;
	}
	else if (JB_ROOT_IS_ARRAY(j))
	{
		cnt = (int) JB_ROOT_COUNT(j);
	}

	if (cnt < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("length(): list or string is expected but %s",
						JsonbToCString(NULL, (JsonbSuperHeader)&j->root, VARSIZE(j)))));

	n = DirectFunctionCall1(int4_numeric, Int32GetDatum(cnt));
	jv.type = jbvNumeric;
	jv.numeric = DatumGetNumeric(n);
	jv.estSize = 2 * sizeof(JEntry) + VARSIZE_ANY(jv.numeric); 
	PG_RETURN_JSONB(JsonbValueToJsonb(&jv));
}

Datum
jsonb_toboolean(PG_FUNCTION_ARGS)
{
	Jsonb	   *j = PG_GETARG_JSONB(0);

	if (JB_ROOT_IS_SCALAR(j))
	{
		JsonbValue *jv;

		jv = getIthJsonbValueFromContainer(&j->root, 0);
		if (jv->type == jbvString)
		{
			if (jv->string.len == 4 &&
				pg_strncasecmp(jv->string.val, "true", 4) == 0)
				PG_RETURN_BOOL(true);
			else if (jv->string.len == 5 &&
					 pg_strncasecmp(jv->string.val, "false", 5) == 0)
				PG_RETURN_BOOL(false);
			else
				PG_RETURN_NULL();
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("toBoolean(): string is expected but %s",
					JsonbToCString(NULL, (JsonbSuperHeader)&j->root, VARSIZE(j)))));
	PG_RETURN_NULL();
}

Datum
jsonb_keys(PG_FUNCTION_ARGS)
{
	Jsonb	   *j = PG_GETARG_JSONB(0);
	JsonbParseState *jpstate = NULL;
	JsonbIterator *it;
	JsonbValue	jv;
	int tok;
	JsonbValue *ajv;

	if (!JB_ROOT_IS_OBJECT(j))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("keys(): map is expected but %s",
						JsonbToCString(NULL, (JsonbSuperHeader)&j->root, VARSIZE(j)))));

	pushJsonbValue(&jpstate, WJB_BEGIN_ARRAY, NULL);

	it = JsonbIteratorInit((JsonbSuperHeader)&j->root);
	tok = JsonbIteratorNext(&it, &jv, false);
	while (tok != WJB_DONE)
	{
		if (tok == WJB_KEY)
			pushJsonbValue(&jpstate, WJB_ELEM, &jv);

		tok = JsonbIteratorNext(&it, &jv, true);
	}

	ajv = pushJsonbValue(&jpstate, WJB_END_ARRAY, NULL);

	PG_RETURN_JSONB(JsonbValueToJsonb(ajv));
}

Datum
jsonb_tail(PG_FUNCTION_ARGS)
{
	Jsonb	   *j = PG_GETARG_JSONB(0);
	JsonbParseState *jpstate = NULL;
	JsonbValue *ajv;

	if (!JB_ROOT_IS_ARRAY(j) || JB_ROOT_IS_SCALAR(j))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("tail(): list is expected but %s",
						JsonbToCString(NULL, (JsonbSuperHeader)&j->root, VARSIZE(j)))));

	pushJsonbValue(&jpstate, WJB_BEGIN_ARRAY, NULL);

	if (JB_ROOT_COUNT(j) > 1)
	{
		JsonbIterator *it;
		JsonbValue	jv;
		int tok;
		bool		push = false;

		it = JsonbIteratorInit((JsonbSuperHeader)&j->root);
		tok = JsonbIteratorNext(&it, &jv, false);
		while (tok != WJB_DONE)
		{
			if (tok == WJB_ELEM)
			{
				if (push)
					pushJsonbValue(&jpstate, WJB_ELEM, &jv);
				else
					push = true;
			}

			tok = JsonbIteratorNext(&it, &jv, true);
		}
	}

	ajv = pushJsonbValue(&jpstate, WJB_END_ARRAY, NULL);

	PG_RETURN_JSONB(JsonbValueToJsonb(ajv));
}

Datum
jsonb_abs(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = numeric_abs;
	fcjinfo.funcname = "abs";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = NUMERICOID;
	fcjinfo.rettype = NUMERICOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_ceil(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = numeric_ceil;
	fcjinfo.funcname = "ceil";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = NUMERICOID;
	fcjinfo.rettype = NUMERICOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_floor(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = numeric_floor;
	fcjinfo.funcname = "floor";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = NUMERICOID;
	fcjinfo.rettype = NUMERICOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_rand(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = drandom;
	fcjinfo.funcname = "rand";
	fcjinfo.nargs = 0;
	fcjinfo.rettype = FLOAT8OID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_round(PG_FUNCTION_ARGS)
{
	Jsonb	   *j = PG_GETARG_JSONB(0);

	if (JB_ROOT_IS_SCALAR(j))
	{
		JsonbValue *jv;

		jv = getIthJsonbValueFromContainer(&j->root, 0);
		if (jv->type == jbvNumeric)
		{
			Datum		n;
			JsonbValue	njv;

			n = DirectFunctionCall2(numeric_round,
									NumericGetDatum(jv->numeric),
									Int32GetDatum(0));

			njv.type = jbvNumeric;
			njv.numeric = DatumGetNumeric(n);
			njv.estSize = 2 * sizeof(JEntry) + VARSIZE_ANY(njv.numeric); 

			PG_RETURN_JSONB(JsonbValueToJsonb(&njv));
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("round(): number is expected but %s",
					JsonbToCString(NULL, (JsonbSuperHeader)&j->root, VARSIZE(j)))));
	PG_RETURN_NULL();
}

Datum
jsonb_sign(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = numeric_sign;
	fcjinfo.funcname = "sign";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = NUMERICOID;
	fcjinfo.rettype = NUMERICOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_exp(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = numeric_exp;
	fcjinfo.funcname = "exp";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = NUMERICOID;
	fcjinfo.rettype = NUMERICOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_log(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = numeric_ln;
	fcjinfo.funcname = "log";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = NUMERICOID;
	fcjinfo.rettype = NUMERICOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_log10(PG_FUNCTION_ARGS)
{
	Jsonb	   *j = PG_GETARG_JSONB(0);

	if (JB_ROOT_IS_SCALAR(j))
	{
		JsonbValue *jv;

		jv = getIthJsonbValueFromSuperHeader(VARDATA(j), 0);
		if (jv->type == jbvNumeric)
		{
			Datum		n;
			JsonbValue	njv;

			n = DirectFunctionCall2(numeric_log, get_numeric_10_datum(),
									NumericGetDatum(jv->numeric));

			njv.type = jbvNumeric;
			njv.numeric = DatumGetNumeric(n);
			njv.estSize = 2 * sizeof(JEntry) + VARSIZE_ANY(njv.numeric); 

			PG_RETURN_JSONB(JsonbValueToJsonb(&njv));
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("log10(): number is expected but %s",
					JsonbToCString(NULL,VARDATA(j), VARSIZE(j)))));
	PG_RETURN_NULL();
}

static Datum
get_numeric_10_datum(void)
{
	static Datum n = 0;

	if (n == 0)
	{
		MemoryContext oldMemoryContext;

		oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);

		n = DirectFunctionCall1(int8_numeric, Int64GetDatum(10));

		MemoryContextSwitchTo(oldMemoryContext);
	}

	return n;
}

Datum
jsonb_sqrt(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = numeric_sqrt;
	fcjinfo.funcname = "sqrt";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = NUMERICOID;
	fcjinfo.rettype = NUMERICOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_acos(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = dacos;
	fcjinfo.funcname = "acos";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = FLOAT8OID;
	fcjinfo.rettype = FLOAT8OID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_asin(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = dasin;
	fcjinfo.funcname = "asin";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = FLOAT8OID;
	fcjinfo.rettype = FLOAT8OID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_atan(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = datan;
	fcjinfo.funcname = "atan";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = FLOAT8OID;
	fcjinfo.rettype = FLOAT8OID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_atan2(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = datan2;
	fcjinfo.funcname = "atan2";
	fcjinfo.nargs = 2;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = FLOAT8OID;
	fcjinfo.args[1] = PG_GETARG_JSONB(1);
	fcjinfo.argtypes[1] = FLOAT8OID;
	fcjinfo.rettype = FLOAT8OID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_cos(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = dcos;
	fcjinfo.funcname = "cos";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = FLOAT8OID;
	fcjinfo.rettype = FLOAT8OID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_cot(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = dcot;
	fcjinfo.funcname = "cot";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = FLOAT8OID;
	fcjinfo.rettype = FLOAT8OID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_degrees(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = degrees;
	fcjinfo.funcname = "degrees";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = FLOAT8OID;
	fcjinfo.rettype = FLOAT8OID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_radians(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = radians;
	fcjinfo.funcname = "radians";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = FLOAT8OID;
	fcjinfo.rettype = FLOAT8OID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_sin(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = dsin;
	fcjinfo.funcname = "sin";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = FLOAT8OID;
	fcjinfo.rettype = FLOAT8OID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_tan(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = dtan;
	fcjinfo.funcname = "tan";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = FLOAT8OID;
	fcjinfo.rettype = FLOAT8OID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_left(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = text_left;
	fcjinfo.funcname = "left";
	fcjinfo.nargs = 2;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = TEXTOID;
	fcjinfo.args[1] = PG_GETARG_JSONB(1);
	fcjinfo.argtypes[1] = INT4OID;
	fcjinfo.rettype = TEXTOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_ltrim(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = ltrim1;
	fcjinfo.funcname = "lTrim";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = TEXTOID;
	fcjinfo.rettype = TEXTOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_replace(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = replace_text;
	fcjinfo.funcname = "replace";
	fcjinfo.nargs = 3;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = TEXTOID;
	fcjinfo.args[1] = PG_GETARG_JSONB(1);
	fcjinfo.argtypes[1] = TEXTOID;
	fcjinfo.args[2] = PG_GETARG_JSONB(2);
	fcjinfo.argtypes[2] = TEXTOID;
	fcjinfo.rettype = TEXTOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_reverse(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = text_reverse;
	fcjinfo.funcname = "reverse";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = TEXTOID;
	fcjinfo.rettype = TEXTOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_right(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = text_right;
	fcjinfo.funcname = "right";
	fcjinfo.nargs = 2;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = TEXTOID;
	fcjinfo.args[1] = PG_GETARG_JSONB(1);
	fcjinfo.argtypes[1] = INT4OID;
	fcjinfo.rettype = TEXTOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_rtrim(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = rtrim1;
	fcjinfo.funcname = "rTrim";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = TEXTOID;
	fcjinfo.rettype = TEXTOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_substr_no_len(PG_FUNCTION_ARGS)
{
	Jsonb	   *sj = PG_GETARG_JSONB(0);
	Jsonb	   *ij = PG_GETARG_JSONB(1);

	if (JB_ROOT_IS_SCALAR(sj) && JB_ROOT_IS_SCALAR(ij))
	{
		JsonbValue *sjv;
		JsonbValue *ijv;

		sjv = getIthJsonbValueFromContainer(&sj->root, 0);
		ijv = getIthJsonbValueFromContainer(&ij->root, 0);

		if (sjv->type == jbvString &&
			ijv->type == jbvNumeric && is_numeric_integer(ijv->numeric))
		{
			text	   *s;
			Datum		i;
			Datum		r;
			JsonbValue	rjv;

			s = cstring_to_text_with_len(sjv->string.val,
										 sjv->string.len);
			i = DirectFunctionCall1(numeric_int4,
									NumericGetDatum(ijv->numeric));
			i = Int32GetDatum(DatumGetInt32(i) + 1);

			r = DirectFunctionCall2(text_substr_no_len, PointerGetDatum(s), i);

			rjv.type = jbvString;
			rjv.string.val = TextDatumGetCString(r);
			rjv.string.len = strlen(rjv.string.val);
			rjv.estSize = sizeof(JEntry) + rjv.string.len;

			PG_RETURN_JSONB(JsonbValueToJsonb(&rjv));
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("substring(): string, number is expected but %s, %s",
					JsonbToCString(NULL, (JsonbSuperHeader)&sj->root, VARSIZE(sj)),
					JsonbToCString(NULL, (JsonbSuperHeader)&ij->root, VARSIZE(ij)))));
	PG_RETURN_NULL();
}

Datum
jsonb_substr(PG_FUNCTION_ARGS)
{
	Jsonb	   *sj = PG_GETARG_JSONB(0);
	Jsonb	   *ij = PG_GETARG_JSONB(1);
	Jsonb	   *lj = PG_GETARG_JSONB(2);

	if (JB_ROOT_IS_SCALAR(sj) &&
		JB_ROOT_IS_SCALAR(ij) && JB_ROOT_IS_SCALAR(lj))
	{
		JsonbValue *sjv;
		JsonbValue *ijv;
		JsonbValue *ljv;

		sjv = getIthJsonbValueFromContainer(&sj->root, 0);
		ijv = getIthJsonbValueFromContainer(&ij->root, 0);
		ljv = getIthJsonbValueFromContainer(&lj->root, 0);

		if (sjv->type == jbvString &&
			ijv->type == jbvNumeric && is_numeric_integer(ijv->numeric) &&
			ljv->type == jbvNumeric && is_numeric_integer(ljv->numeric))
		{
			text	   *s;
			Datum		i;
			Datum		l;
			Datum		r;
			JsonbValue	rjv;

			s = cstring_to_text_with_len(sjv->string.val,
										 sjv->string.len);
			i = DirectFunctionCall1(numeric_int4,
									NumericGetDatum(ijv->numeric));
			i = Int32GetDatum(DatumGetInt32(i) + 1);
			l = DirectFunctionCall1(numeric_int4,
									NumericGetDatum(ljv->numeric));

			r = DirectFunctionCall3(text_substr, PointerGetDatum(s), i, l);

			rjv.type = jbvString;
			rjv.string.val = TextDatumGetCString(r);
			rjv.string.len = strlen(rjv.string.val);
			rjv.estSize = sizeof(JEntry) + rjv.string.len;

			PG_RETURN_JSONB(JsonbValueToJsonb(&rjv));
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("substring(): string, number, number is expected "
					"but %s, %s, %s",
					JsonbToCString(NULL, (JsonbSuperHeader)&sj->root, VARSIZE(sj)),
					JsonbToCString(NULL, (JsonbSuperHeader)&ij->root, VARSIZE(ij)),
					JsonbToCString(NULL, (JsonbSuperHeader)&lj->root, VARSIZE(lj)))));
	PG_RETURN_NULL();
}

Datum
jsonb_tolower(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = lower;
	fcjinfo.funcname = "toLower";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = TEXTOID;
	fcjinfo.rettype = TEXTOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_tostring(PG_FUNCTION_ARGS)
{
	Jsonb	   *j = PG_GETARG_JSONB(0);

	if (JB_ROOT_IS_SCALAR(j))
	{
		JsonbValue *jv;
		JsonbValue	sjv;

		jv = getIthJsonbValueFromContainer(&j->root, 0);
		if (jv->type == jbvString)
		{
			PG_RETURN_JSONB(j);
		}
		else if (jv->type == jbvNumeric)
		{
			Datum		s;

			s = DirectFunctionCall1(numeric_out,
									NumericGetDatum(jv->numeric));

			sjv.type = jbvString;
			sjv.string.val = DatumGetCString(s);
			sjv.string.len = strlen(sjv.string.val);
			sjv.estSize = sizeof(JEntry) + sjv.string.len;

			PG_RETURN_JSONB(JsonbValueToJsonb(&sjv));
		}
		else if (jv->type == jbvBool)
		{
			sjv.type = jbvString;

			if (jv->boolean)
			{
				sjv.string.len = 4;
				sjv.string.val = "true";
			}
			else
			{
				sjv.string.len = 5;
				sjv.string.val = "false";
			}
			sjv.estSize = sizeof(JEntry) + sjv.string.len;


			PG_RETURN_JSONB(JsonbValueToJsonb(&sjv));
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("toString(): string, number, or bool is expected but %s",
					JsonbToCString(NULL, (JsonbSuperHeader)&j->root, VARSIZE(j)))));
	PG_RETURN_NULL();
}

Datum
jsonb_toupper(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = upper;
	fcjinfo.funcname = "toUpper";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = TEXTOID;
	fcjinfo.rettype = TEXTOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

Datum
jsonb_trim(PG_FUNCTION_ARGS)
{
	FunctionCallJsonbInfo fcjinfo;

	fcjinfo.fn = btrim1;
	fcjinfo.funcname = "trim";
	fcjinfo.nargs = 1;
	fcjinfo.args[0] = PG_GETARG_JSONB(0);
	fcjinfo.argtypes[0] = TEXTOID;
	fcjinfo.rettype = TEXTOID;

	PG_RETURN_JSONB(FunctionCallJsonb(&fcjinfo));
}

static Jsonb *
FunctionCallJsonb(FunctionCallJsonbInfo *fcjinfo)
{
	FunctionCallInfoData fcinfo;
	int			i;
	Datum		result;

	if (fcjinfo->nargs > FUNC_JSONB_MAX_ARGS)
	{
		elog(ERROR, "unexpected number of arguments: %d", fcjinfo->nargs);
		return NULL;
	}

	InitFunctionCallInfoData(fcinfo, NULL, fcjinfo->nargs,
							 DEFAULT_COLLATION_OID, NULL, NULL);

	for (i = 0; i < fcjinfo->nargs; i++)
	{
		if (!JB_ROOT_IS_SCALAR(fcjinfo->args[i]))
			ereport_invalid_jsonb_param(fcjinfo);

		fcinfo.arg[i] = jsonb_to_datum(fcjinfo->args[i], fcjinfo->argtypes[i]);
		fcinfo.argnull[i] = false;
	}

	result = (*fcjinfo->fn) (&fcinfo);

	return datum_to_jsonb(result, fcjinfo->rettype);
}

static Datum
jsonb_to_datum(Jsonb *j, Oid type)
{
	JsonbValue *jv;
	Datum		retval = 0;

	jv = getIthJsonbValueFromContainer(&j->root, 0);

	switch (type)
	{
		case INT4OID:
			if (jv->type == jbvNumeric && is_numeric_integer(jv->numeric))
				retval = DirectFunctionCall1(numeric_int4,
										   NumericGetDatum(jv->numeric));
			break;
		case TEXTOID:
			if (jv->type == jbvString)
			{
				text	   *t;

				t = cstring_to_text_with_len(jv->string.val,
											 jv->string.len);
				retval = PointerGetDatum(t);
			}
			break;
		case FLOAT8OID:
			if (jv->type == jbvNumeric)
				retval = DirectFunctionCall1(numeric_float8,
										   NumericGetDatum(jv->numeric));
			break;
		case NUMERICOID:
			if (jv->type == jbvNumeric)
				retval = NumericGetDatum(jv->numeric);
			break;
		default:
			elog(ERROR, "unexpected type: %s", format_type_be(type));
	}

	return retval;
}

static bool
is_numeric_integer(Numeric n)
{
	int32		scale;

	scale = DatumGetInt32(DirectFunctionCall1(numeric_scale,
											  NumericGetDatum(n)));

	PG_RETURN_BOOL(scale <= 0);
}

static void
ereport_invalid_jsonb_param(FunctionCallJsonbInfo *fcjinfo)
{
	switch (fcjinfo->nargs)
	{
		case 1:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s(): %s is expected but %s",
							fcjinfo->funcname,
							type_to_jsonb_type_str(fcjinfo->argtypes[0]),
							JsonbToCString(NULL, (JsonbSuperHeader)&fcjinfo->args[0]->root,
										   VARSIZE(fcjinfo->args[0])))));
			break;
		case 2:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s(): %s, %s is expected but %s, %s",
							fcjinfo->funcname,
							type_to_jsonb_type_str(fcjinfo->argtypes[0]),
							type_to_jsonb_type_str(fcjinfo->argtypes[1]),
							JsonbToCString(NULL, (JsonbSuperHeader)&fcjinfo->args[0]->root,
										   VARSIZE(fcjinfo->args[0])),
							JsonbToCString(NULL, (JsonbSuperHeader)&fcjinfo->args[1]->root,
										   VARSIZE(fcjinfo->args[1])))));
			break;
		case 3:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s(): %s, %s, %s is expected but %s, %s, %s",
							fcjinfo->funcname,
							type_to_jsonb_type_str(fcjinfo->argtypes[0]),
							type_to_jsonb_type_str(fcjinfo->argtypes[1]),
							type_to_jsonb_type_str(fcjinfo->argtypes[2]),
							JsonbToCString(NULL, (JsonbSuperHeader)&fcjinfo->args[0]->root,
										   VARSIZE(fcjinfo->args[0])),
							JsonbToCString(NULL, (JsonbSuperHeader)&fcjinfo->args[1]->root,
										   VARSIZE(fcjinfo->args[1])),
							JsonbToCString(NULL, (JsonbSuperHeader)&fcjinfo->args[2]->root,
										   VARSIZE(fcjinfo->args[2])))));
			break;
		case 0:
		default:
			elog(ERROR, "unexpected number of arguments: %d", fcjinfo->nargs);
			break;
	}
}

static char *
type_to_jsonb_type_str(Oid type)
{
	switch (type)
	{
		case INT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			return "number";
		case TEXTOID:
			return "string";
		default:
			elog(ERROR, "unexpected type: %s", format_type_be(type));
			return NULL;
	}
}

static Jsonb *
datum_to_jsonb(Datum d, Oid type)
{
	JsonbValue	jv;

	switch (type)
	{
		case TEXTOID:
			jv.type = jbvString;
			jv.string.val = TextDatumGetCString(d);
			jv.string.len = strlen(jv.string.val);
			jv.estSize = sizeof(JEntry) + jv.string.len;
			break;
		case FLOAT8OID:
			{
				Datum		n;

				n = DirectFunctionCall1(float8_numeric, d);

				jv.type = jbvNumeric;
				jv.numeric = DatumGetNumeric(n);
				jv.estSize = 2 * sizeof(JEntry) + VARSIZE_ANY(jv.numeric); 

			}
			break;
		case NUMERICOID:
			jv.type = jbvNumeric;
			jv.numeric = DatumGetNumeric(d);
			jv.estSize = 2 * sizeof(JEntry) + VARSIZE_ANY(jv.numeric); 

			break;
		default:
			elog(ERROR, "unexpected type: %s", format_type_be(type));
			return NULL;
	}

	return JsonbValueToJsonb(&jv);
}

Datum
jsonb_string_starts_with(PG_FUNCTION_ARGS)
{
	Jsonb	   *lj = PG_GETARG_JSONB(0);
	Jsonb	   *rj = PG_GETARG_JSONB(1);

	if (JB_ROOT_IS_SCALAR(lj) && JB_ROOT_IS_SCALAR(rj))
	{
		JsonbValue *ljv;
		JsonbValue *rjv;

		ljv = getIthJsonbValueFromContainer(&lj->root, 0);
		rjv = getIthJsonbValueFromContainer(&rj->root, 0);

		if (ljv->type == jbvString && rjv->type == jbvString)
		{
			if (ljv->string.len < rjv->string.len)
				PG_RETURN_BOOL(false);

			if (strncmp(ljv->string.val,
						rjv->string.val,
						rjv->string.len) == 0)
				PG_RETURN_BOOL(true);
			else
				PG_RETURN_BOOL(false);
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("STARTS WITH: two string values expected but %s, %s",
					JsonbToCString(NULL, (JsonbSuperHeader)&lj->root, VARSIZE(lj)),
					JsonbToCString(NULL, (JsonbSuperHeader)&rj->root, VARSIZE(rj)))));
	PG_RETURN_NULL();
}

Datum
jsonb_string_ends_with(PG_FUNCTION_ARGS)
{
	Jsonb	   *lj = PG_GETARG_JSONB(0);
	Jsonb	   *rj = PG_GETARG_JSONB(1);

	if (JB_ROOT_IS_SCALAR(lj) && JB_ROOT_IS_SCALAR(rj))
	{
		JsonbValue *ljv;
		JsonbValue *rjv;

		ljv = getIthJsonbValueFromContainer(&lj->root, 0);
		rjv = getIthJsonbValueFromContainer(&rj->root, 0);

		if (ljv->type == jbvString && rjv->type == jbvString)
		{
			if (ljv->string.len < rjv->string.len)
				PG_RETURN_BOOL(false);

			if (strncmp(ljv->string.val + ljv->string.len - rjv->string.len,
						rjv->string.val,
						rjv->string.len) == 0)
				PG_RETURN_BOOL(true);
			else
				PG_RETURN_BOOL(false);
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("ENDS WITH: two string values expected but %s, %s",
					JsonbToCString(NULL, (JsonbSuperHeader)&lj->root, VARSIZE(lj)),
					JsonbToCString(NULL, (JsonbSuperHeader)&rj->root, VARSIZE(rj)))));
	PG_RETURN_NULL();
}

Datum
jsonb_string_contains(PG_FUNCTION_ARGS)
{
	Jsonb	   *lj = PG_GETARG_JSONB(0);
	Jsonb	   *rj = PG_GETARG_JSONB(1);

	if (JB_ROOT_IS_SCALAR(lj) && JB_ROOT_IS_SCALAR(rj))
	{
		JsonbValue *ljv;
		JsonbValue *rjv;

		ljv = getIthJsonbValueFromContainer(&lj->root, 0);
		rjv = getIthJsonbValueFromContainer(&rj->root, 0);

		if (ljv->type == jbvString && rjv->type == jbvString)
		{
			char	   *l;
			char	   *r;

			if (ljv->string.len < rjv->string.len)
				PG_RETURN_BOOL(false);

			l = pnstrdup(ljv->string.val, ljv->string.len);
			r = pnstrdup(rjv->string.val, rjv->string.len);

			if (strstr(l, r) == NULL)
				PG_RETURN_BOOL(false);
			else
				PG_RETURN_BOOL(true);
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("CONTAINS: two string values expected but %s, %s",
					JsonbToCString(NULL, (JsonbSuperHeader)&lj->root, VARSIZE(lj)),
					JsonbToCString(NULL, (JsonbSuperHeader)&rj->root, VARSIZE(rj)))));
	PG_RETURN_NULL();
}

Datum
jsonb_string_regex(PG_FUNCTION_ARGS)
{
	Jsonb	   *lj = PG_GETARG_JSONB(0);
	Jsonb	   *rj = PG_GETARG_JSONB(1);

	if (JB_ROOT_IS_SCALAR(lj) && JB_ROOT_IS_SCALAR(rj))
	{
		JsonbValue *ljv;
		JsonbValue *rjv;

		ljv = getIthJsonbValueFromContainer(&lj->root, 0);
		rjv = getIthJsonbValueFromContainer(&rj->root, 0);

		if (ljv->type == jbvString && rjv->type == jbvString)
		{
			text	   *lt;
			text	   *rt;
			Datum		result;

			lt = cstring_to_text_with_len(ljv->string.val,
										  ljv->string.len);
			rt = cstring_to_text_with_len(rjv->string.val,
										  rjv->string.len);

			result = DirectFunctionCall2Coll(textregexeq,
											 DEFAULT_COLLATION_OID,
											 PointerGetDatum(lt),
											 PointerGetDatum(rt));
			PG_RETURN_DATUM(result);
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("Regular Expression Pattern: two string values expected but %s, %s",
					JsonbToCString(NULL, (JsonbSuperHeader)&lj->root, VARSIZE(lj)),
					JsonbToCString(NULL, (JsonbSuperHeader)&rj->root, VARSIZE(rj)))));
	PG_RETURN_NULL();
}
