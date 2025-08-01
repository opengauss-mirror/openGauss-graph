/*
 * cypher_ops.c
 *	  Functions for operators in Cypher expressions.
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/cypher_ops.c
 */
#ifdef GS_GRAPH

#include "postgres.h"

#include "utils/builtins.h"
#include "utils/cypher_ops.h"
#include "utils/datum.h"
#include "utils/graph.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/numeric.h"

static Jsonb *jnumber_op(PGFunction f, Jsonb *l, Jsonb *r);
static Jsonb *numeric_to_jnumber(Numeric n);
static void ereport_op(PGFunction f, Jsonb *l, Jsonb *r);
static void ereport_op_str(const char *op, Jsonb *l, Jsonb *r);
static Datum get_numeric_0_datum(void);
static Datum jsonb_num(Jsonb *j, PGFunction f);

Datum
jsonb_add(PG_FUNCTION_ARGS)
{
	Jsonb	   *l = PG_GETARG_JSONB(0);
	Jsonb	   *r = PG_GETARG_JSONB(1);
	JsonbValue *ljv;
	JsonbValue *rjv;
	JsonbValue	jv;
	Size		len;
	char	   *buf;
	Datum		n;
	char	   *nstr;
	Size		nlen;

	if (!(JB_ROOT_IS_SCALAR(l) && JB_ROOT_IS_SCALAR(r)))
	{
		Datum		j;

		if ((JB_ROOT_IS_SCALAR(l) && JB_ROOT_IS_OBJECT(r)) ||
			(JB_ROOT_IS_OBJECT(l) && JB_ROOT_IS_SCALAR(r)) ||
			(JB_ROOT_IS_OBJECT(l) && JB_ROOT_IS_OBJECT(r)))
			ereport_op_str("+", l, r);

		j = DirectFunctionCall2(jsonb_concat,
								JsonbGetDatum(l), JsonbGetDatum(r));

		PG_RETURN_DATUM(j);
	}

	ljv = getIthJsonbValueFromContainer(&l->root, 0);
	rjv = getIthJsonbValueFromContainer(&r->root, 0);

	if (ljv->type == jbvString && rjv->type == jbvString)
	{
		len = ljv->string.len + rjv->string.len;
		buf = (char*)palloc(len);

		strncpy(buf, ljv->string.val, ljv->string.len);
		strncpy(buf + ljv->string.len,
				rjv->string.val, rjv->string.len);

		jv.type = jbvString;
		jv.string.len = len;
		jv.string.val = buf;
		jv.estSize = sizeof(JEntry) + jv.string.len;

		PG_RETURN_JSONB(JsonbValueToJsonb(&jv));
	}
	else if (ljv->type == jbvString && rjv->type == jbvNumeric)
	{
		n = DirectFunctionCall1(numeric_out,
								NumericGetDatum(rjv->numeric));
		nstr = DatumGetCString(n);
		nlen = strlen(nstr);

		len = ljv->string.len + nlen;
		buf = (char*)palloc(len);

		strncpy(buf, ljv->string.val, ljv->string.len);
		strncpy(buf + ljv->string.len, nstr, nlen);

		jv.type = jbvString;
		jv.string.len = len;
		jv.string.val = buf;
		jv.estSize = sizeof(JEntry) + jv.string.len;

		PG_RETURN_JSONB(JsonbValueToJsonb(&jv));
	}
	else if (ljv->type == jbvNumeric && rjv->type == jbvString)
	{
		n = DirectFunctionCall1(numeric_out,
								NumericGetDatum(ljv->numeric));
		nstr = DatumGetCString(n);
		nlen = strlen(nstr);

		len = nlen + rjv->string.len;
		buf = (char*)palloc(len);

		strncpy(buf, nstr, nlen);
		strncpy(buf + nlen, rjv->string.val, rjv->string.len);

		jv.type = jbvString;
		jv.string.len = len;
		jv.string.val = buf;
		jv.estSize = sizeof(JEntry) + jv.string.len;
		
		PG_RETURN_JSONB(JsonbValueToJsonb(&jv));
	}
	else if (ljv->type == jbvNumeric && rjv->type == jbvNumeric)
	{
		n = DirectFunctionCall2(numeric_add,
								NumericGetDatum(ljv->numeric),
								NumericGetDatum(rjv->numeric));

		PG_RETURN_JSONB(numeric_to_jnumber(DatumGetNumeric(n)));
	}
	else
	{
		ereport_op_str("+", l, r);
	}

	PG_RETURN_NULL();
}

Datum
jsonb_sub(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(jnumber_op(numeric_sub,
							   PG_GETARG_JSONB(0), PG_GETARG_JSONB(1)));
}

Datum
jsonb_mul(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(jnumber_op(numeric_mul,
							   PG_GETARG_JSONB(0), PG_GETARG_JSONB(1)));
}

Datum
jsonb_div(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(jnumber_op(numeric_div,
							   PG_GETARG_JSONB(0), PG_GETARG_JSONB(1)));
}

Datum
jsonb_mod(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(jnumber_op(numeric_mod,
							   PG_GETARG_JSONB(0), PG_GETARG_JSONB(1)));
}

Datum
jsonb_pow(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(jnumber_op(numeric_power,
							   PG_GETARG_JSONB(0), PG_GETARG_JSONB(1)));
}

Datum
jsonb_uplus(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(jnumber_op(numeric_uplus, NULL, PG_GETARG_JSONB(0)));
}

Datum
jsonb_uminus(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(jnumber_op(numeric_uminus, NULL, PG_GETARG_JSONB(0)));
}

static Jsonb *
jnumber_op(PGFunction f, Jsonb *l, Jsonb *r)
{
	FunctionCallInfoData fcinfo;
	JsonbValue *jv;
	Datum		n;

	AssertArg(r != NULL);

	if (!((l == NULL || JB_ROOT_IS_SCALAR(l)) && JB_ROOT_IS_SCALAR(r)))
		ereport_op(f, l, r);

	InitFunctionCallInfoData(fcinfo, NULL, 0, InvalidOid, NULL, NULL);

	if (l != NULL)
	{
		// jv = getIthJsonbValueFromContainer(&l->root, 0);
		jv = getIthJsonbValueFromSuperHeader(VARDATA(l), 0); 
		if (jv->type != jbvNumeric)
			ereport_op(f, l, r);

		fcinfo.arg[fcinfo.nargs] = NumericGetDatum(jv->numeric);
		fcinfo.argnull[fcinfo.nargs] = false;
		fcinfo.nargs++;
	}

	// jv = getIthJsonbValueFromContainer(&r->root, 0);
	jv = getIthJsonbValueFromSuperHeader(VARDATA(r), 0); 
	if (jv->type != jbvNumeric)
		ereport_op(f, l, r);

	fcinfo.arg[fcinfo.nargs] = NumericGetDatum(jv->numeric);
	fcinfo.argnull[fcinfo.nargs] = false;
	fcinfo.nargs++;

	n = (*f) (&fcinfo);
	if (fcinfo.isnull)
		elog(ERROR, "function %p returned NULL", (void *) f);

	if (f == numeric_power || f == numeric_div)
	{
		int			s;

		s = DatumGetInt32(DirectFunctionCall1(numeric_scale, fcinfo.arg[0])) +
			DatumGetInt32(DirectFunctionCall1(numeric_scale, fcinfo.arg[1]));
		if (s == 0)
			n = DirectFunctionCall2(numeric_trunc, n, 0);
	}

	return numeric_to_jnumber(DatumGetNumeric(n));
}

static Jsonb *
numeric_to_jnumber(Numeric n)
{
	JsonbValue	jv;

	jv.type = jbvNumeric;
	jv.numeric = n;
	jv.estSize = 2 * sizeof(JEntry) + VARSIZE_ANY(jv.numeric); 

	return JsonbValueToJsonb(&jv);
}

static void
ereport_op(PGFunction f, Jsonb *l, Jsonb *r)
{
	const char *op;

	if (f == numeric_add)
		op = "+";
	else if (f == numeric_sub)
		op = "-";
	else if (f == numeric_mul)
		op = "*";
	else if (f == numeric_div)
		op = "/";
	else if (f == numeric_mod)
		op = "%";
	else if (f == numeric_power)
		op = "^";
	else if (f == numeric_uplus)
		op = "+";
	else if (f == numeric_uminus)
		op = "-";
	else
		elog(ERROR, "invalid number operator");

	ereport_op_str(op, l, r);
}

static void
ereport_op_str(const char *op, Jsonb *l, Jsonb *r)
{
	const char *msgfmt;
	const char *lstr;
	const char *rstr;

	AssertArg(r != NULL);

	if (l == NULL)
	{
		msgfmt = "invalid expression: %s%s%s";
		lstr = "";
	}
	else
	{
		msgfmt = "invalid expression: %s %s %s";
		lstr = JsonbToCString(NULL, (JsonbSuperHeader)&l->root, VARSIZE(l));
	}
	rstr = JsonbToCString(NULL, (JsonbSuperHeader)&r->root, VARSIZE(r));

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg(msgfmt, lstr, op, rstr)));
}

Datum
jsonb_bool(PG_FUNCTION_ARGS)
{
	Jsonb	   *j = PG_GETARG_JSONB(0);

	if (JB_ROOT_IS_SCALAR(j))
	{
		JsonbValue *jv;

		jv = getIthJsonbValueFromContainer(&j->root, 0);
		switch (jv->type)
		{
			case jbvNull:
				PG_RETURN_NULL();
			case jbvString:
				PG_RETURN_BOOL(jv->string.len > 0);
			case jbvNumeric:
				{
					Datum		b;

					if (numeric_is_nan(jv->numeric))
						PG_RETURN_BOOL(false);

					b = DirectFunctionCall2(numeric_ne,
											NumericGetDatum(jv->numeric),
											get_numeric_0_datum());
					PG_RETURN_DATUM(b);
				}
			case jbvBool:
				PG_RETURN_BOOL(jv->boolean);
			default:
				elog(ERROR, "unknown jsonb scalar type");
		}
	}

	Assert(JB_ROOT_IS_OBJECT(j) || JB_ROOT_IS_ARRAY(j));
	PG_RETURN_BOOL(JB_ROOT_COUNT(j) > 0);
}

static Datum
get_numeric_0_datum(void)
{
	static Datum n = 0;

	if (n == 0)
	{
		MemoryContext oldMemoryContext;

		oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);

		n = DirectFunctionCall1(int8_numeric, Int64GetDatum(0));

		MemoryContextSwitchTo(oldMemoryContext);
	}

	return n;
}

Datum
bool_jsonb(PG_FUNCTION_ARGS)
{
	bool		b = PG_GETARG_BOOL(0);
	JsonbValue	jv;

	jv.type = jbvBool;
	jv.boolean = b;
	jv.estSize = sizeof(JEntry);
	PG_RETURN_JSONB(JsonbValueToJsonb(&jv));
}

Datum
jsonb_int8(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(jsonb_num(PG_GETARG_JSONB(0), numeric_int8));
}

Datum
jsonb_int4(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(jsonb_num(PG_GETARG_JSONB(0), numeric_int4));
}

Datum
jsonb_numeric(PG_FUNCTION_ARGS)
{
	Jsonb	   *j = PG_GETARG_JSONB(0);

	if (JB_ROOT_IS_SCALAR(j))
	{
		JsonbValue *jv;

		jv = getIthJsonbValueFromContainer(&j->root, 0);
		if (jv->type == jbvNumeric)
			PG_RETURN_DATUM(datumCopy(NumericGetDatum(jv->numeric), false,
													  -1));
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("%s cannot be converted to numeric",
					JsonbToCString(NULL, (JsonbSuperHeader)&j->root, VARSIZE(j)))));
	PG_RETURN_NULL();
}

Datum
jsonb_float8(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(jsonb_num(PG_GETARG_JSONB(0), numeric_float8));
}

static Datum
jsonb_num(Jsonb *j, PGFunction f)
{
	const char *type;

	if (f == numeric_int8)
		type = "int8";
	else if (f == numeric_int4)
		type = "int4";
	else if (f == numeric_float8)
		type = "float8";
	else
		elog(ERROR, "unexpected type");

	if (JB_ROOT_IS_SCALAR(j))
	{
		JsonbValue *jv;

		jv = getIthJsonbValueFromContainer(&j->root, 0);
		if (jv->type == jbvNumeric)
		{
			Datum		n;

			n = DirectFunctionCall1(f, NumericGetDatum(jv->numeric));

			return n;
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("%s cannot be converted to %s",
					JsonbToCString(NULL, (JsonbSuperHeader)&j->root, VARSIZE(j)), type)));
	return 0;
}

Datum
numeric_graphid(PG_FUNCTION_ARGS)
{
	Datum		n = PG_GETARG_DATUM(0);
	Datum		d;

	d = DirectFunctionCall1(numeric_out, n);

	PG_RETURN_DATUM(DirectFunctionCall1(graphid_in, d));
}
#endif /* GS_GRAPH */