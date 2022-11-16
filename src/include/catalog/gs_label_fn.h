/*
 * gs_label_fn.h
 *	  prototypes for functions in backend/catalog/gs_label.cpp
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/include/catalog/gs_label_fn.h
 */
#ifndef GS_LABEL_FN_H
#define GS_LABEL_FN_H

#include "nodes/parsenodes.h"
#include "nodes/parsenodes_common.h"

extern Oid label_create_with_catalog(RangeVar *label, Oid relid, char labkind,
									 Oid labtablespace);
extern void label_drop_with_catalog(Oid laboid);

#endif	/* GS_LABEL_FN_H */
