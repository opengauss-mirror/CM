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
# Description  : InstallImpl.py
#############################################################################

import os
import re
import subprocess
import xml.etree.cElementTree as ETree
from ErrorCode import ErrorCode
from Common import executeCmdOnHost
from CMLog import CMLog

class InstallImpl:
    def __init__(self, install):
        self.cmpkg = install.cmpkg
        self.context = install
        self.envFile = install.envFile
        self.xmlFile = install.xmlFile
        self.cmDirs = install.cmDirs
        self.hostNames = install.hostNames
        self.gaussHome = install.gaussHome
        self.gaussLog = install.gaussLog
        self.toolPath = install.toolPath
        self.tmpPath = install.tmpPath
        self.localhostName = install.localhostName
        self.logger = install.logger

    def executeCmdOnHost(self, host, cmd, isLocal = False):
        if host == self.localhostName:
            isLocal = True
        return executeCmdOnHost(host, cmd, isLocal)

    def prepareCMPath(self):
        """
        create path: cmdir、cmdir/cm_server、cmdir/cm_agent
        """
        self.logger.log("Preparing CM path.")
        for (cmdir, host) in zip(self.cmDirs, self.hostNames):
            cmd = "mkdir -p {cmdir}/cm_server {cmdir}/cm_agent".format(cmdir=cmdir)
            status, output = self.executeCmdOnHost(host, cmd)
            if status != 0:
                self.logger.debug("Command: " + cmd)
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit("Failed to create CM path." + errorDetail)

    def decompressCMPkg(self):
        self.logger.log("Decompressing CM pacakage.")
        if self.cmpkg == "":
            return
        # decompress cm pkg on localhost
        decompressCmd = "tar -zxf %s -C %s" % (self.cmpkg, self.gaussHome)
        status, output = subprocess.getstatusoutput(decompressCmd)
        if status != 0:
            self.logger.debug("Command: " + decompressCmd)
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit("Failed to decompress cm pacakage to on localhost." + errorDetail)

        # If the version of CM pacakage is inconsistent with that of gaussdb,
        # then exit. So no need to send CM pacakage to other nodes.
        self.checkCMPkgVersion()

        # decompress cmpkg on other hosts
        cmpkgName = os.path.basename(self.cmpkg)
        for host in self.hostNames:
            if host == self.localhostName:
                continue
            # copy cm pacakage to other hosts
            scpCmd = "scp %s %s:%s" % (self.cmpkg, host, self.toolPath)
            status, output = subprocess.getstatusoutput(scpCmd)
            if status != 0:
                self.logger.debug("Command: " + scpCmd)
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit(("Failed to send cm pacakage to %s." % host) + errorDetail)
            pkgPath = os.path.join(self.toolPath, cmpkgName)
            decompressCmd = "tar -zxf %s -C %s" % (pkgPath, self.gaussHome)
            status, output = self.executeCmdOnHost(host, decompressCmd)
            if status != 0:
                self.logger.debug("Command: " + decompressCmd)
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit(("Failed to decompress cm pacakage to on host %s." % host) + errorDetail)

    def checkCMPkgVersion(self):
        getCMVersionCmd = "source %s; cm_ctl -V" % self.envFile
        status, output = subprocess.getstatusoutput(getCMVersionCmd)
        if status != 0:
            self.logger.logExit("Failed to get CM pacakage version.")
        cmVersionList = re.findall(r'openGauss CM (\d.*\d) build', output)
        if len(cmVersionList) == 0:
            self.logger.logExit("Failed to get CM pacakage version.")
        cmVersion = cmVersionList[0]

        getGaussdbVersionCmd = "source %s; gaussdb -V" % self.envFile
        status, output = subprocess.getstatusoutput(getGaussdbVersionCmd)
        if status != 0:
            self.logger.logExit("Failed to get gaussdb version.")
        gaussdbVersionList = re.findall(r'openGauss (\d.*\d) build', output)
        if len(gaussdbVersionList) == 0:
            self.logger.logExit("Failed to get gaussdb version.")
        gaussdbVersion = gaussdbVersionList[0]

        if gaussdbVersion != cmVersion:
            self.logger.logExit("The version of CM pacakage(%s) is inconsistent "
                "with that of gaussdb(%s)." % (cmVersion, gaussdbVersion))

    def createManualStartFile(self):
        self.logger.log("Creating cluster_manual_start file.")
        cmd = """
            if [ ! -f {gaussHome}/bin/cluster_manual_start ]; then
                touch {gaussHome}/bin/cluster_manual_start
            fi
            """.format(gaussHome=self.gaussHome)
        for host in self.hostNames:
            status, output = self.executeCmdOnHost(host, cmd)
            if status != 0:
                self.logger.debug("Command: " + cmd)
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit("Failed to create cluster_manual_start file." + errorDetail)

    def initCMServer(self):
        self.logger.log("Initializing cm_server.")
        for (cmdir, host) in zip(self.cmDirs, self.hostNames):
            cmd = """
                cp {gaussHome}/share/config/cm_server.conf.sample {cmdir}/cm_server/cm_server.conf
                sed 's#log_dir = .*#log_dir = {gaussLog}/cm/cm_server#' {cmdir}/cm_server/cm_server.conf -i
                """.format(gaussHome=self.gaussHome, gaussLog=self.gaussLog, cmdir=cmdir)
            status, output = self.executeCmdOnHost(host, cmd)
            if status != 0:
                self.logger.debug("Command: " + cmd)
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit("Failed to initialize cm_server." + errorDetail)

    def initCMAgent(self):
        self.logger.log("Initializing cm_agent.")
        for (cmdir, host) in zip(self.cmDirs, self.hostNames):
            cmd = """
                cp {gaussHome}/share/config/cm_agent.conf.sample {cmdir}/cm_agent/cm_agent.conf && 
                sed 's#log_dir = .*#log_dir = {gaussLog}/cm/cm_agent#' {cmdir}/cm_agent/cm_agent.conf -i && 
                sed 's#unix_socket_directory = .*#unix_socket_directory = {gaussHome}#' {cmdir}/cm_agent/cm_agent.conf -i
                """.format(gaussHome=self.gaussHome, gaussLog=self.gaussLog, cmdir=cmdir)
            status, output = self.executeCmdOnHost(host, cmd)
            if status != 0:
                self.logger.debug("Command: " + cmd)
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit("Failed to initialize cm_agent." + errorDetail)

    def setMonitorCrontab(self):
        """
        set om_monitor crontab
        """
        self.logger.log("Setting om_monitor crontab.")
        # save old crontab content to cronContentTmpFile
        cronContentTmpFile = os.path.join(self.tmpPath, "cronContentTmpFile_" + str(os.getpid()))
        listCronCmd = "crontab -l > %s" % cronContentTmpFile
        status, output = self.executeCmdOnHost(self.localhostName, listCronCmd)
        if status != 0:
            self.logger.debug("Command: " + listCronCmd)
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit(ErrorCode.GAUSS_508["GAUSS_50804"] + errorDetail)
        # if old crontab content contains om_monitor, clear it
        clearMonitorCmd = "sed '/.*om_monitor.*/d' %s -i" % cronContentTmpFile
        status, output = subprocess.getstatusoutput(clearMonitorCmd)
        if status != 0:
            os.remove(cronContentTmpFile)
            self.logger.debug("Command: " + clearMonitorCmd)
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit("Failed to clear old om_monitor crontab." + errorDetail)

        # generate om_monitor crontab command and append it to cronContentTmpFile
        startMonitorCmd = "source /etc/profile;(if [ -f ~/.profile ];" \
                      "then source ~/.profile;fi);source ~/.bashrc;"
        if self.envFile != "~/.bashrc":
            startMonitorCmd += "source %s; " % (self.envFile)
        monitorLogPath = os.path.join(self.gaussLog, "cm")
        if not os.path.exists(monitorLogPath):
            os.makedirs(monitorLogPath)
        startMonitorCmd += "nohup om_monitor -L %s/om_monitor >>/dev/null 2>&1 &" % monitorLogPath
        monitorCron = "*/1 * * * * " + startMonitorCmd + os.linesep
        with open(cronContentTmpFile, 'a+', encoding='utf-8') as fp:
            fp.writelines(monitorCron)
            fp.flush()

        # set crontab on other hosts
        setCronCmd = "crontab %s" % cronContentTmpFile
        cleanTmpFileCmd = "rm %s -f" % cronContentTmpFile
        for host in self.hostNames:
            if host == self.localhostName:
                continue
            # copy cronContentTmpFile to other host
            scpCmd = "scp %s %s:%s" % (cronContentTmpFile, host, self.tmpPath)
            status, output = subprocess.getstatusoutput(scpCmd)
            if status != 0:
                self.logger.debug("Command: " + scpCmd)
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit(("Failed to copy cronContentTmpFile to %s." % host) + errorDetail)
            # set om_monitor crontab
            status, output = self.executeCmdOnHost(host, setCronCmd)
            # cleanup cronContentTmpFile
            self.executeCmdOnHost(host, cleanTmpFileCmd)
            if status != 0:
                self.logger.debug("Command: " + setCronCmd)
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit(ErrorCode.GAUSS_508["GAUSS_50801"] + errorDetail)

            # start om_monitor
            status, output = self.executeCmdOnHost(host, startMonitorCmd)
            if status != 0:
                self.logger.debug("Command: " + startMonitorCmd)
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit((ErrorCode.GAUSS_516["GAUSS_51607"] % "om_monitor") + errorDetail)

        # set crontab on localhost
        status, output = subprocess.getstatusoutput(setCronCmd)
        if status != 0:
            self.logger.debug("Command: " + setCronCmd)
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit(ErrorCode.GAUSS_508["GAUSS_50801"] + errorDetail)
        os.remove(cronContentTmpFile)

        status, output = subprocess.getstatusoutput(startMonitorCmd)
        if status != 0:
            self.logger.debug("Command: " + startMonitorCmd)
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit((ErrorCode.GAUSS_516["GAUSS_51607"] % "om_monitor") + errorDetail)

    def startCluster(self):
        self.logger.log("Starting cluster.")
        startCmd = "source %s; cm_ctl start" % self.envFile
        status, output = subprocess.getstatusoutput(startCmd)
        if status != 0:
            self.logger.debug("Command: " + startCmd)
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit("Failed to start cluster." + errorDetail)
        queryCmd = "source %s; cm_ctl query -Cv" % self.envFile
        status, output = subprocess.getstatusoutput(queryCmd)
        if status != 0:
            self.logger.debug("Command: " + queryCmd)
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit("Failed to query cluster status." + errorDetail)
        self.logger.log(output)
        self.logger.log("Install CM tool success.")

    @staticmethod
    def refreshStaticFile(envFile, xmlFile):
        """
        refresh static and dynamic file using xml file with cm
        """
        # refresh static file
        cmd = """
            source {envFile};
            gs_om -t generateconf -X {xmlFile} --distribute
            """.format(envFile=envFile, xmlFile=xmlFile)
        status, output = subprocess.getstatusoutput(cmd)
        errorDetail = ""
        if status != 0:
            errorDetail = "\nCommand: %s\nStatus: %s\nOutput: %s" % (cmd, status, output)
        return status, errorDetail

    @staticmethod
    def refreshDynamicFile(envFile):
        # refresh dynamic file
        getStatusCmd = "source %s; gs_om -t status --detail | grep 'Primary Normal' > /dev/null" % envFile
        status, output = subprocess.getstatusoutput(getStatusCmd)
        if status != 0:
            CMLog.printMessage("Normal primary doesn't exist in the cluster, no need to refresh dynamic file.")
            return 0, ""
        refreshDynamicFileCmd = "source %s; gs_om -t refreshconf" % envFile
        status, output = subprocess.getstatusoutput(refreshDynamicFileCmd)
        errorDetail = ""
        if status != 0:
            errorDetail = "\nCommand: %s\nStatus: %s\nOutput: %s" % (refreshDynamicFileCmd, status, output)
        return status, errorDetail

    def refreshStaticAndDynamicFile(self):
        self.logger.log("Refreshing static and dynamic file using xml file with cm.")
        status, output  = InstallImpl.refreshStaticFile(self.envFile, self.xmlFile)
        if status != 0:
            self.logger.logExit("Failed to refresh static file." + output)
        status, output = InstallImpl.refreshDynamicFile(self.envFile)
        if status != 0:
            self.logger.logExit("Failed to refresh dynamic file." + output)

    def run(self):
        self.logger.log("Start to install cm tool.")
        self.prepareCMPath()
        self.decompressCMPkg()
        self.createManualStartFile()
        self.initCMServer()
        self.initCMAgent()
        self.refreshStaticAndDynamicFile()
        self.setMonitorCrontab()
        self.startCluster()
