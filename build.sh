#!/bin/bash
# Copyright (c) 2026 Huawei Technologies Co., Ltd
# CM is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
set -e

SCRIPT_PATH=$(cd $(dirname $0) && pwd)

# which default opt if not pointed
DEFAULT_OPT="-o ${SCRIPT_PATH}/output"

# which must choose
FIX_OPT="-c CM"

sh ${SCRIPT_PATH}/build/build.sh ${DEFAULT_OPT} "$@" ${FIX_OPT}
