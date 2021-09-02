/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * pg_functions_support.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_expressions\pg_functions_support.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include "pg_functions_support.h"
#include <utility>
#include <iostream>
#include <sstream>
#include "nodes/parsenodes_common.h"
#include "nodes/feparser_memutils.h"
#include "expr_processor.h"
#include "client_logic_processor/processor_utils.h"
#include "client_logic_common/statement_data.h"
#include "expr_parts_list.h"

bool handle_func_call(const FuncCall *funccall, ExprPartsList *expr_parts_list, StatementData *statement_data)
{
    if (funccall->funcname != NULL && strcmp(strVal(linitial(funccall->funcname)), "repeat") == 0) {
        if (funccall->args == NULL || list_length(funccall->args) != 2) {
            return true;
        }
        if (nodeTag(linitial(funccall->args)) != T_A_Const || nodeTag(lsecond(funccall->args)) != T_A_Const) {
            return true;
        }
        if (((A_Const *)linitial(funccall->args))->val.type != T_String || 
            ((A_Const *)lsecond(funccall->args))->val.type != T_Integer) {
            return true;
        }
        const char *data = ((A_Const *)linitial(funccall->args))->val.val.str;
        int count = ((A_Const *)lsecond(funccall->args))->val.val.ival;

        bool empty_str = false;
        /* Note. If number is less than 1, the repeat function will return an empty string. Or data is empty */
        if (strlen(data) == 0 || count < 1) {
            empty_str = true;
            count = 0;
        }

        size_t total_len = strlen(data) * count;
        char *result = (char *)malloc((total_len + 1) * sizeof(char));
        if (result == NULL) {
            return false;
        }
        check_memset_s(memset_s(result, total_len + 1, 0, total_len + 1));
        for (int i = 0; i < count; i++) {
            check_strncat_s(strncat_s(result, total_len + 1, data, strlen(data)));
        }
        ExprParts expr_parts;
        A_Const *aconst = makeNode(A_Const);
        aconst->val.type = T_String;
        aconst->val.val.str = (char *)feparser_malloc((1 + strlen(result)) * sizeof(char));
        aconst->location = funccall->location;

        errno_t rc = EOK;
        rc = strcpy_s(aconst->val.val.str, strlen(result) + 1, result);
        securec_check_c(rc, "\0", "\0");
        aconst->val.val.str[strlen(result)] = '\0';
        expr_parts.param_ref = NULL;
        expr_parts.column_ref = NULL;
        expr_parts.aconst = aconst;
        expr_parts.is_empty_repeat = empty_str;
        const char *pch = strchr(statement_data->query + funccall->location, ')');
        expr_parts.original_size = pch + 1 - (statement_data->query + funccall->location);
        expr_parts_list->add(&expr_parts);
        free(result);
        return true;
    } else if (funccall->funcname != NULL && strcmp(strVal(linitial(funccall->funcname)), "empty_blob") == 0) {
        if (funccall->args != NULL && list_length(funccall->args) > 0) {
            return true;
        }
        ExprParts expr_parts;
        A_Const *aconst = makeNode(A_Const);
        aconst->val.type = T_Null;
        aconst->location = funccall->location;
        expr_parts.param_ref = NULL;
        expr_parts.column_ref = NULL;
        expr_parts.aconst = aconst;
        const char *pch = strchr(statement_data->query + funccall->location, ')');
        if (pch == NULL) {
            return false;
        }
        expr_parts.original_size = pch + 1 - (statement_data->query + funccall->location);
        expr_parts_list->add(&expr_parts);
        return true;
    } else {
        /* function is not supported on client logic
         * columns yet function is not supported on client logic columns yet. 
         */
        return true;
    }
}
