/*
 * dblink.h
 *
 * Functions returning results from a remote database
 *
 * Joe Conway <mail@joeconway.com>
 * And contributors:
 * Darko Prenosil <Darko.Prenosil@finteh.hr>
 * Shridhar Daithankar <shridhar_daithankar@persistent.co.in>
 *
 * contrib/dblink/dblink.h
 * Copyright (c) 2001-2012, PostgreSQL Global Development Group
 * ALL RIGHTS RESERVED;
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE AUTHOR OR DISTRIBUTORS HAVE BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR AND DISTRIBUTORS SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE AUTHOR AND DISTRIBUTORS HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 */

#ifndef DBLINK_H
#define DBLINK_H

#include "fmgr.h"

/*
 * External declarations
 */
extern "C" Datum dblink_connect(PG_FUNCTION_ARGS);
extern "C" Datum dblink_disconnect(PG_FUNCTION_ARGS);
extern "C" Datum dblink_open(PG_FUNCTION_ARGS);
extern "C" Datum dblink_close(PG_FUNCTION_ARGS);
extern "C" Datum dblink_fetch(PG_FUNCTION_ARGS);
extern "C" Datum dblink_record(PG_FUNCTION_ARGS);
extern "C" Datum dblink_send_query(PG_FUNCTION_ARGS);
extern "C" Datum dblink_get_result(PG_FUNCTION_ARGS);
extern "C" Datum dblink_get_connections(PG_FUNCTION_ARGS);
extern "C" Datum dblink_is_busy(PG_FUNCTION_ARGS);
extern "C" Datum dblink_cancel_query(PG_FUNCTION_ARGS);
extern "C" Datum dblink_error_message(PG_FUNCTION_ARGS);
extern "C" Datum dblink_exec(PG_FUNCTION_ARGS);
extern "C" Datum dblink_get_pkey(PG_FUNCTION_ARGS);
extern "C" Datum dblink_build_sql_insert(PG_FUNCTION_ARGS);
extern "C" Datum dblink_build_sql_delete(PG_FUNCTION_ARGS);
extern "C" Datum dblink_build_sql_update(PG_FUNCTION_ARGS);
extern "C" Datum dblink_current_query(PG_FUNCTION_ARGS);
extern "C" Datum dblink_get_notify(PG_FUNCTION_ARGS);
extern "C" void set_extension_index(uint32 index);
extern "C" void init_session_vars(void);

#endif /* DBLINK_H */
