/* -------------------------------------------------------------------------
 *
 * keywords.c
 *	  lexical token lookup for key words in openGauss
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/bin/pg_dump/keywords.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "parser/keywords.h"

/*
 * We don't need the token number, so leave it out to avoid requiring other
 * backend headers.
 */
#define PG_KEYWORD(a, b, c) {a, 0, c},

ScanKeyword FEScanKeywords[] = {
#include "parser/kwlist.h"
};

int NumFEScanKeywords = lengthof(FEScanKeywords);
