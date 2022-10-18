# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms
# and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
# WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ----------------------------------------------------------------------------
# Description  : Common.py includes some public function
#############################################################################

import subprocess
from CMLog import CMLog
from ErrorCode import ErrorCode

def getEnvParam(envFile, param):
    cmd = "source {envFile}; echo ${param}".format(envFile=envFile, param=param)
    status, output = subprocess.getstatusoutput(cmd)
    if status != 0:
        errorDetail = "\nCommand: %s\nStatus: %s\nOutput: %s\n" % (
            cmd, status, output)
        CMLog.exitWithError(ErrorCode.GAUSS_518["GAUSS_51802"] % param)
    return output

def getLocalhostName():
    import socket
    return socket.gethostname()

def executeCmdOnHost(host, cmd, isLocal = False):
    if not isLocal:
        cmd = 'ssh -o ConnectTimeout=5 %s \"%s\"' % (host, cmd)
    status, output = subprocess.getstatusoutput(cmd)
    return status, output
