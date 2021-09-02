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
 * index_defs.cpp
 *    Base index definitions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/storage/index/index_defs.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "index_defs.h"

namespace MOT {
const char* IndexTreeFlavorToString(const IndexTreeFlavor& indexTreeFlavor)
{
    switch (indexTreeFlavor) {
        case IndexTreeFlavor::INDEX_TREE_FLAVOR_MASSTREE:
            return "MasstreeFlavor";
        default:
            return "InvalidFlavor";
    }

    return "InvalidFlavor";
}
}  // namespace MOT
