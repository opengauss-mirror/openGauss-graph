/* -------------------------------------------------------------------------
 *
 * pg_attribute.h
 *	  definition of the system "attribute" relation (pg_attribute)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_attribute.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_ATTRIBUTE_H
#define PG_ATTRIBUTE_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_attribute definition.  cpp turns this into
 *		typedef struct FormData_pg_attribute
 *
 *		If you change the following, make sure you change the structs for
 *		system attributes in catalog/heap.c also.
 *		You may need to change catalog/genbki.pl as well.
 * ----------------
 */
#define AttributeRelationId  1249
#define AttributeRelation_Rowtype_Id  75

#define IsAttributeRelation(rel) (RelationGetRelid(rel) == AttributeRelationId)
#define IsAttributeCache(reloid) ((reloid) == AttributeRelationId)

CATALOG(pg_attribute,1249) BKI_BOOTSTRAP BKI_WITHOUT_OIDS BKI_ROWTYPE_OID(75) BKI_SCHEMA_MACRO
{
	Oid			attrelid;		/* OID of relation containing this attribute */
	NameData	attname;		/* name of attribute */

	/*
	 * atttypid is the OID of the instance in Catalog Class pg_type that
	 * defines the data type of this attribute (e.g. int4).  Information in
	 * that instance is redundant with the attlen, attbyval, and attalign
	 * attributes of this instance, so they had better match or openGauss will
	 * fail.
	 */
	Oid			atttypid;

	/*
	 * attstattarget is the target number of statistics datapoints to collect
	 * during VACUUM ANALYZE of this column.  A zero here indicates that we do
	 * not wish to collect any stats about this column. A "-1" here indicates
	 * that no value has been explicitly set for this column, so ANALYZE
	 * should use the default setting.
	 */
	int4		attstattarget;

	/*
	 * attlen is a copy of the typlen field from pg_type for this attribute.
	 * See atttypid comments above.
	 */
	int2		attlen;

	/*
	 * attnum is the "attribute number" for the attribute:	A value that
	 * uniquely identifies this attribute within its class. For user
	 * attributes, Attribute numbers are greater than 0 and not greater than
	 * the number of attributes in the class. I.e. if the Class pg_class says
	 * that Class XYZ has 10 attributes, then the user attribute numbers in
	 * Class pg_attribute must be 1-10.
	 *
	 * System attributes have attribute numbers less than 0 that are unique
	 * within the class, but not constrained to any particular range.
	 *
	 * Note that (attnum - 1) is often used as the index to an array.
	 */
	int2		attnum;

	/*
	 * attndims is the declared number of dimensions, if an array type,
	 * otherwise zero.
	 */
	int4		attndims;

	/*
	 * fastgetattr() uses attcacheoff to cache byte offsets of attributes in
	 * heap tuples.  The value actually stored in pg_attribute (-1) indicates
	 * no cached value.  But when we copy these tuples into a tuple
	 * descriptor, we may then update attcacheoff in the copies. This speeds
	 * up the attribute walking process.
	 *
	 * Important: this is only for uncompressed tuples, both cached and updated.
	 * And it cann't be applied to compressed tuples. Each attribute within 
	 * compressed tuple should be accessed one by one, step by step.
	 */
	int4		attcacheoff;

	/*
	 * atttypmod records type-specific data supplied at table creation time
	 * (for example, the max length of a varchar field).  It is passed to
	 * type-specific input and output functions as the third argument. The
	 * value will generally be -1 for types that do not need typmod.
	 */
	int4		atttypmod;

	/*
	 * attbyval is a copy of the typbyval field from pg_type for this
	 * attribute.  See atttypid comments above.
	 */
	bool		attbyval;

	/* ----------
	 * attstorage tells for VARLENA attributes, what the heap access
	 * methods can do to it if a given tuple doesn't fit into a page.
	 * Possible values are
	 *		'p': Value must be stored plain always
	 *		'e': Value can be stored in "secondary" relation (if relation
	 *			 has one, see pg_class.reltoastrelid)
	 *		'm': Value can be stored compressed inline
	 *		'x': Value can be stored compressed inline or in "secondary"
	 * Note that 'm' fields can also be moved out to secondary storage,
	 * but only as a last resort ('e' and 'x' fields are moved first).
	 * ----------
	 */
	char		attstorage;

	/*
	 * attalign is a copy of the typalign field from pg_type for this
	 * attribute.  See atttypid comments above.
	 */
	char		attalign;

	/* This flag represents the "NOT NULL" constraint */
	bool		attnotnull;

	/* Has DEFAULT value or not */
	bool		atthasdef;

	/* Is dropped (ie, logically invisible) or not */
	bool		attisdropped;

	/* Has a local definition (hence, do not drop when attinhcount is 0) */
	bool		attislocal;
	
	/* Compression Mode for this attribute
	 * its size is 1Byte, and the 7 fields before are also 1Btye wide, so place it here;
	 * its valid value is:  CMPR_NONE ~ CMPR_NUMSTR. see also pagecompress.h
	 */
	int1		attcmprmode;

	/* Number of times inherited from direct parent relation(s) */
	int4		attinhcount;

	/* attribute's collation */
	Oid			attcollation;

#ifdef CATALOG_VARLEN                    /* variable-length fields start here */
	/* NOTE: The following fields are not present in tuple descriptors. */

	/* Column-level access permissions */
	aclitem		attacl[1];

	/* Column-level options */
	text		attoptions[1];

	/* Column-level FDW options */
	text		attfdwoptions[1];

	/* the value is not null only when ALTER TABLE ... ADD COLUMN call */
	bytea		attinitdefval;

#endif
	/* the attribute type for kv storage: tag(1), field(2), time(3), hide(4) or default(0) */
	int1		attkvtype;
} FormData_pg_attribute;

/*
 * ATTRIBUTE_FIXED_PART_SIZE is the size of the fixed-layout,
 * guaranteed-not-null part of a pg_attribute row.	This is in fact as much
 * of the row as gets copied into tuple descriptors, so don't expect you
 * can access fields beyond attcollation except in a real tuple!
 */
#define ATTRIBUTE_FIXED_PART_SIZE \
	(offsetof(FormData_pg_attribute, attkvtype) + sizeof(Oid))

/* ----------------
 *		Form_pg_attribute corresponds to a pointer to a tuple with
 *		the format of pg_attribute relation.
 * ----------------
 */
typedef FormData_pg_attribute *Form_pg_attribute;

/* ----------------
 *		compiler constants for pg_attribute
 * ----------------
 */
#define Natts_pg_attribute				24
#define Anum_pg_attribute_attrelid		1
#define Anum_pg_attribute_attname		2
#define Anum_pg_attribute_atttypid		3
#define Anum_pg_attribute_attstattarget 4
#define Anum_pg_attribute_attlen		5
#define Anum_pg_attribute_attnum		6
#define Anum_pg_attribute_attndims		7
#define Anum_pg_attribute_attcacheoff	8
#define Anum_pg_attribute_atttypmod		9
#define Anum_pg_attribute_attbyval		10
#define Anum_pg_attribute_attstorage	11
#define Anum_pg_attribute_attalign		12
#define Anum_pg_attribute_attnotnull	13
#define Anum_pg_attribute_atthasdef		14
#define Anum_pg_attribute_attisdropped	15
#define Anum_pg_attribute_attislocal	16
#define Anum_pg_attribute_attcmprmode	17
#define Anum_pg_attribute_attinhcount	18
#define Anum_pg_attribute_attcollation	19
#define Anum_pg_attribute_attacl		20
#define Anum_pg_attribute_attoptions	21
#define Anum_pg_attribute_attfdwoptions 22
#define Anum_pg_attribute_attinitdefval 23
#define Anum_pg_attribute_attkvtype		24

/* ----------------
 *		initial contents of pg_attribute
 *
 * The initial contents of pg_attribute are generated at compile time by
 * genbki.pl.  Only "bootstrapped" relations need be included.
 * ----------------
 */
#define ATTRIBUTE_IDENTITY_ALWAYS 'a'
#define ATTRIBUTE_GENERATED_STORED 's'

#ifdef GS_GRAPH
/* graph */
DATA(insert ( 8110 id 8102 -1 8 1 0 -1 -1 FLOAT8PASSBYVAL p d f f f t 0 0 0 _null_ _null_ _null_ _null_ 0));
DATA(insert ( 8110 properties 3802 -1 -1 2 0 -1 -1 f x i f f f t 0 0 0 _null_ _null_ _null_ _null_ 0));
DATA(insert ( 8110 tid 27 -1 6 3 0 -1 -1 f p s f f f t 0 0 0 _null_ _null_ _null_ _null_ 0));
DATA(insert ( 8120 id 8102 -1 8 1 0 -1 -1 FLOAT8PASSBYVAL p d f f f t 0 0 0 _null_ _null_ _null_ _null_ 0));
DATA(insert ( 8120 start 8102 -1 8 2 0 -1 -1 FLOAT8PASSBYVAL p d f f f t 0 0 0 _null_ _null_ _null_ _null_ 0));
DATA(insert ( 8120 end 8102 -1 8 3 0 -1 -1 FLOAT8PASSBYVAL p d f f f t 0 0 0 _null_ _null_ _null_ _null_ 0));
DATA(insert ( 8120 properties 3802 -1 -1 4 0 -1 -1 f x i f f f t 0 0 0 _null_ _null_ _null_ _null_ 0));
DATA(insert ( 8120 tid 27 -1 6 5 0 -1 -1 f p s f f f t 0 0 0 _null_ _null_ _null_ _null_ 0));
DATA(insert ( 8130 vertices 8111 -1 -1 1 1 -1 -1 f x i f f f t 0 0 0 _null_ _null_ _null_ _null_ 0));
DATA(insert ( 8130 edges 8121 -1 -1 2 1 -1 -1 f x i f f f t 0 0 0 _null_ _null_ _null_ _null_ 0));
#endif /* GS_GRAPH */

#endif   /* PG_ATTRIBUTE_H */

