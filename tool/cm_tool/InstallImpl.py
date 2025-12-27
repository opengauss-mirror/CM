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

from curses.ascii import isdigit, islower, isupper
import os
import re
import subprocess
import shlex
import getpass
from ErrorCode import ErrorCode
from Common import *

class InstallImpl:
    def __init__(self, install):
        self.cmpkg = install.cmpkg
        self.context = install
        self.envFile = install.envFile
        self.xmlFile = install.xmlFile
        self.cmDirs = install.cmDirs
        self.hostnames = install.hostnames
        self.gaussHome = install.gaussHome
        self.gaussLog = install.gaussLog
        self.toolPath = install.toolPath
        self.tmpPath = install.tmpPath
        self.localhostName = install.localhostName
        self.logger = install.logger
        self.clusterStopped = install.clusterStopped
        self.primaryTermAbnormal = install.primaryTermAbnormal

    def executeCmdOnHost(self, host, cmd, isLocal = False):
        if host == self.localhostName:
            isLocal = True
        return executeCmdOnHost(host, cmd, isLocal)

    def execute_cmd_on_host_safely(self, host, cmd, is_local = False):
        if host == self.localhostName:
            is_local = True
        return execute_cmd_on_host_safely(host, cmd, is_local)

    def validate_cmdir(cmdir):
        """
        Validate CM directory path legality, prohibit special symbols
        """
        if not isinstance(cmdir, str) or not cmdir.startswith("/"):
            raise ValueError(f"cmdir must be an absolute path: {cmdir}")
        # Allow only letters/numbers/slashes/underscores/hyphens/dots (forbid Shell special chars)
        illegal_chars = re.findall(r'[^a-zA-Z0-9/_\-.]', cmdir)
        if illegal_chars:
            raise ValueError(f"CM directory contains illegal characters {illegal_chars}: {cmdir}")

    def validate_hostname(hostname):
        """
        Validate hostname (IP/domain name) legality
        """
        # IP regex (with validity check, avoid invalid IP like 256.0.0.1)
        ip_Pattern = r"^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$"
        # Domain name regex (simplified version, extendable for actual scenarios)
        domain_pattern = r"^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"
        if not re.match(ip_Pattern, hostname) and not re.match(domain_pattern, hostname):
            raise ValueError(f" Invalid hostname: {hostname}")

    def prepareCMPath(self):
        """
        create path: cmdir、cmdir/cm_server、cmdir/cm_agent
        """
        self.logger.log("Preparing CM path.")
        for (cmdir, host) in zip(self.cmDirs, self.hostnames):
            try:
                InstallImpl.validate_cmdir(cmdir)
            except ValueError as e:
                self.logger.logExit(f"Invalid cmdir parameter: {e}")
            try:
                InstallImpl.validate_hostname(host)
            except ValueError as e:
                self.logger.logExit(f"Invalid host parameter: {e}")

            cmd_args = ["mkdir", "-p", f"{cmdir}/cm_server", f"{cmdir}/cm_agent"]
            status, output = self.execute_cmd_on_host_safely(host, cmd_args)
            if status != 0:
                self.logger.debug("Command: " + str(cmd_args))
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit("Failed to create CM path." + errorDetail)

    def decompressCMPkg(self):
        self.logger.log("Decompressing CM pacakage.")
        if self.cmpkg == "":
            return
        # decompress cm pkg on localhost
        # Use common_execute_cmd to safely execute tar command, avoiding injection risk
        cmd_args = ['tar', '-zxf', self.cmpkg, '-C', self.gaussHome]
        status, output = common_execute_cmd(cmd_args)
        if status != 0:
            self.logger.debug("Command: " + str(cmd_args))
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit("Failed to decompress cm pacakage to on localhost." + errorDetail)

        # If the version of CM pacakage is inconsistent with that of gaussdb,
        # then exit. So no need to send CM pacakage to other nodes.
        self.checkCMPkgVersion()

        # decompress cmpkg on other hosts
        cmpkgName = os.path.basename(self.cmpkg)
        for host in self.hostnames:
            if host == self.localhostName:
                continue
            # copy cm pacakage to other hosts
            # Use common_execute_cmd to safely execute scp command, avoiding injection risk
            if ":" in host:
                host = "[" + host + "]"
            destination = "%s:%s" % (host, self.toolPath)
            cmd_args = ['scp', self.cmpkg, destination]
            status, output = common_execute_cmd(cmd_args)
            if status != 0:
                self.logger.debug("Command: " + str(cmd_args))
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit(("Failed to send cm pacakage to %s." % host) + errorDetail)
            pkgPath = os.path.join(self.toolPath, cmpkgName)
            # Use execute_cmd_on_host_safely to safely execute tar command, avoiding injection risk
            cmd_args = ['tar', '-zxf', pkgPath, '-C', self.gaussHome]
            status, output = self.execute_cmd_on_host_safely(host, cmd_args)
            if status != 0:
                self.logger.debug("Command: " + str(cmd_args))
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit(("Failed to decompress cm pacakage to on host %s." % host) + errorDetail)

    def checkCMPkgVersion(self):
        # Use common_execute_cmd to safely execute command, avoiding injection risk
        safe_env_file = shlex.quote(self.envFile)
        cmd_str = "source %s; cm_ctl -V" % safe_env_file
        cmd_args = ['sh', '-c', cmd_str]
        status, output = common_execute_cmd(cmd_args)
        if status != 0:
            self.logger.logExit("Failed to get CM pacakage version.")
        cmVersionList = re.findall(r'.*CM (\d.*\d) build', output)
        if len(cmVersionList) == 0:
            self.logger.logExit("Failed to get CM pacakage version.")
        cmVersion = cmVersionList[0]

        # Use common_execute_cmd to safely execute command, avoiding injection risk
        cmd_str = "source %s; gaussdb -V" % safe_env_file
        cmd_args = ['sh', '-c', cmd_str]
        status, output = common_execute_cmd(cmd_args)
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
        for host in self.hostnames:
            status, output = self.executeCmdOnHost(host, cmd)
            if status != 0:
                self.logger.debug("Command: " + cmd)
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit("Failed to create cluster_manual_start file." + errorDetail)

    def initCMServer(self):
        self.logger.log("Initializing cm_server.")
        for (cmdir, host) in zip(self.cmDirs, self.hostnames):
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
        for (cmdir, host) in zip(self.cmDirs, self.hostnames):
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
        # Use common_execute_cmd to safely execute crontab command, avoiding injection risk
        cmd_str = "crontab -l > %s" % cronContentTmpFile
        cmd_args = ['sh', '-c', cmd_str]
        status, output = common_execute_cmd(cmd_args)
        if status != 0:
            self.logger.debug("Command: " + str(cmd_args))
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit(ErrorCode.GAUSS_508["GAUSS_50804"] + errorDetail)
        # if old crontab content contains om_monitor, clear it
        clearMonitorCmd = "sed '/.*om_monitor.*/d' %s -i" % cronContentTmpFile
        cmd_args = ['sed', '/.*om_monitor.*/d', cronContentTmpFile, '-i']
        status, output = common_execute_cmd(cmd_args)
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
        checkWritePermissionForDirectory(cronContentTmpFile)
        with open(cronContentTmpFile, 'a+', encoding='utf-8') as fp:
            fp.writelines(monitorCron)
            fp.flush()

        # set crontab on other hosts
        setCronCmd = "crontab %s" % cronContentTmpFile
        cleanTmpFileCmd = "rm %s -f" % cronContentTmpFile
        username = getpass.getuser()
        killMonitorCmd = "pkill om_monitor -u %s; " % username
        for host in self.hostnames:
            if host == self.localhostName:
                continue
            # copy cronContentTmpFile to other host
            if ":" in host:
                host = "[" + host + "]"
            destination = "%s:%s" % (host, self.tmpPath)
            cmd_args = ['scp', cronContentTmpFile, destination]
            status, output = common_execute_cmd(cmd_args)
            if status != 0:
                self.logger.debug("Command: " + str(cmd_args))
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit(("Failed to copy cronContentTmpFile to %s." % host) + errorDetail)
            # set om_monitor crontab
            cmd_args = ['crontab', cronContentTmpFile]
            status, output = self.execute_cmd_on_host_safely(host, cmd_args)
            # cleanup cronContentTmpFile
            cmd_args = ['rm', cronContentTmpFile, '-f']
            status, output = self.execute_cmd_on_host_safely(host, cmd_args)
            if status != 0:
                self.logger.debug("Command: " + setCronCmd)
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit(ErrorCode.GAUSS_508["GAUSS_50801"] + errorDetail)

            # start om_monitor
            # Firstly, kill residual om_monitor, otherwise cm_agent won't be started if there are residual om_monitor process.
            status, output = self.executeCmdOnHost(host, killMonitorCmd + startMonitorCmd)
            if status != 0:
                self.logger.debug("Command: " + startMonitorCmd)
                errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
                self.logger.logExit((ErrorCode.GAUSS_516["GAUSS_51607"] % "om_monitor") + errorDetail)

        # set crontab on localhost
        # Use common_execute_cmd to safely execute crontab command, avoiding injection risk
        cmd_args = ['crontab', cronContentTmpFile]
        status, output = common_execute_cmd(cmd_args)
        os.remove(cronContentTmpFile)
        if status != 0:
            self.logger.debug("Command: " + str(cmd_args))
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit(ErrorCode.GAUSS_508["GAUSS_50801"] + errorDetail)

        # Use common_execute_cmd to safely execute command, avoiding injection risk
        cmd_args = ['sh', '-c', killMonitorCmd + startMonitorCmd]
        status, output = common_execute_cmd(cmd_args)
        if status != 0:
            self.logger.debug("Command: " + str(cmd_args))
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit((ErrorCode.GAUSS_516["GAUSS_51607"] % "om_monitor") + errorDetail)

    def startCluster(self):
        self.logger.log("Starting cluster.")
        # Use common_execute_cmd to safely execute command, avoiding injection risk
        safe_env_file = shlex.quote(self.envFile)
        cmd_str = "source %s; cm_ctl start" % safe_env_file
        cmd_args = ['sh', '-c', cmd_str]
        status, output = common_execute_cmd(cmd_args)
        if status != 0:
            self.logger.debug("Command: " + str(cmd_args))
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit("Failed to start cluster." + errorDetail)

        status, output = InstallImpl.refreshDynamicFile(self.envFile)
        if status != 0:
            self.logger.error("Failed to refresh dynamic file." + output)

        # Use common_execute_cmd to safely execute command, avoiding injection risk
        safe_env_file = shlex.quote(self.envFile)
        cmd_str = "source %s; cm_ctl query -Cv" % safe_env_file
        cmd_args = ['sh', '-c', cmd_str]
        status, output = common_execute_cmd(cmd_args)
        if status != 0:
            self.logger.debug("Command: " + str(cmd_args))
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit("Failed to query cluster status." + errorDetail)
        self.logger.log(output)
        self.logger.log("Install CM tool success.")
        if self.primaryTermAbnormal:
            self.logger.warn("Term of primary is invalid or not maximal.\n"
                "Hint: To avoid CM arbitration anomalies in this situation, "
                "please restart the database.\n"
                "Command : cm_ctl stop && cm_ctl start")

    @staticmethod
    def refreshStaticFile(envFile, xmlFile):
        """
        refresh static and dynamic file using xml file with cm
        """
        # refresh static file
        # Use common_execute_cmd to safely execute command, avoiding injection risk
        safe_env_file = shlex.quote(envFile)
        safe_xml_file = shlex.quote(xmlFile)
        cmd_str = "source %s; gs_om -t generateconf -X %s --distribute" % (safe_env_file, safe_xml_file)
        cmd_args = ['sh', '-c', cmd_str]
        status, output = common_execute_cmd(cmd_args)
        errorDetail = ""
        if status != 0:
            errorDetail = "\nCommand: %s\nStatus: %s\nOutput: %s" % (str(cmd_args), status, output)
        return status, errorDetail

    @staticmethod
    def refreshDynamicFile(envFile):
        # refresh dynamic file
        # Use common_execute_cmd to safely execute command, avoiding injection risk
        safe_env_file = shlex.quote(envFile)
        cmd_str = "source %s; gs_om -t refreshconf" % safe_env_file
        cmd_args = ['sh', '-c', cmd_str]
        status, output = common_execute_cmd(cmd_args)
        errorDetail = ""
        if status != 0:
            errorDetail = "\nCommand: %s\nStatus: %s\nOutput: %s" % (str(cmd_args), status, output)
        return status, errorDetail

    def _refreshStaticFile(self):
        self.logger.log("Refreshing static and dynamic file using xml file with cm.")
        status, output  = InstallImpl.refreshStaticFile(self.envFile, self.xmlFile)
        if status != 0:
            self.logger.logExit("Failed to refresh static file." + output)

    @staticmethod
    def checkPassword(passwordCA):
        minPasswordLen = 8
        maxPasswordLen = 15
        kinds = [0, 0, 0, 0]
        specLetters = "~!@#$%^&*()-_=+\\|[{}];:,<.>/?"
        if len(passwordCA) < minPasswordLen:
            print("Invalid password, it must contain at least eight characters.")
            return False
        if len(passwordCA) > maxPasswordLen:
            print("Invalid password, it must contain at most fifteen characters.")
            return False
        for c in passwordCA:
            if isdigit(c):
                kinds[0] += 1
            elif isupper(c):
                kinds[1] += 1
            elif islower(c):
                kinds[2] += 1
            elif c in specLetters:
                kinds[3] += 1
            else:
                print("The password contains illegal character, please check!")
                return False
        kindsNum = 0
        for k in kinds:
            if k > 0:
                kindsNum += 1
        if kindsNum < 3:
            print("The password must contain at least three kinds of characters.")
            return False
        return True

    def _getPassword(self):
        passwordCA = ""
        passwordCA2 = ""
        tryCount = 0
        while tryCount < 3:
            passwordCA = getpass.getpass("Please input the password for ca cert:")
            passwordCA2 = getpass.getpass("Please input the password for ca cert again:")
            if passwordCA != passwordCA2:
                tryCount += 1
                self.logger.printMessage("The password enterd twice do not match.")
                continue
            if not InstallImpl.checkPassword(passwordCA):
                tryCount += 1
                continue
            break
        if tryCount == 3:
            self.logger.logExit("Maximum number of attempts has been reached.")
        return passwordCA

    def _createCMSslConf(self, certPath):
        """
        Generate config file.
        """
        self.logger.debug("OPENSSL: Create config file.")
        v3CaL = [
            "[ v3_ca ]",
            "subjectKeyIdentifier=hash",
            "authorityKeyIdentifier=keyid:always,issuer:always",
            "basicConstraints = CA:true",
            "keyUsage = keyCertSign,cRLSign",
        ]
        v3Ca = os.linesep.join(v3CaL)

        # Create config file.
        ssl_conf_path = os.path.join(certPath, "openssl.cnf")
        checkWritePermissionForDirectory(ssl_conf_path)
        with open(ssl_conf_path, "w") as fp:
            # Write config item of Signature
            fp.write(v3Ca)
        self.logger.debug("OPENSSL: Successfully create config file.")

    def _cleanUselessFile(self):
        """
        Clean useless files
        :return: NA
        """
        certPath = os.path.join(self.gaussHome, "share/sslcert/cm")
        keyFiles = ["cacert.pem", "server.crt", "server.key", "client.crt", "client.key",
            "server.key.cipher", "server.key.rand", "client.key.cipher", "client.key.rand"]
        for fileName in os.listdir(certPath):
            filePath = os.path.join(certPath, fileName)
            if fileName not in keyFiles:
                os.remove(filePath)

    def _createCMCALocal(self):
        self.logger.debug("Creating Cm ca files locally.")
        certPath = os.path.join(self.gaussHome, "share/sslcert/cm")
        # Use common_execute_cmd to safely execute command, avoiding injection risk
        safe_cert_path = shlex.quote(certPath)
        cmd_str = "rm %s -rf; mkdir %s" % (safe_cert_path, safe_cert_path)
        cmd_args = ['sh', '-c', cmd_str]
        status, output = common_execute_cmd(cmd_args)
        if status != 0:
            self.logger.debug("Command: " + str(cmd_args))
            errorDetail = "\nStatus: %s\nOutput: %s" % (status, output)
            self.logger.logExit("Failed to create cert path." + errorDetail)
        self._createCMSslConf(certPath)
        curPath = os.path.split(os.path.realpath(__file__))[0]
        createCMCACert = os.path.realpath(os.path.join(curPath, "CreateCMCACert.sh"))
        passwd = self._getPassword()

        safe_env_file = shlex.quote(self.envFile)
        safe_script = shlex.quote(createCMCACert)
        cmd = f"source '{safe_env_file}'; sh '{safe_script}'"
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding='utf-8',
            errors='replace'
        )
        output, stderr = proc.communicate(input=f"{passwd}\n")
        status = proc.returncode
        passwd = ""
        del passwd

        cmd = ""
        del cmd
        if status != 0:
            self.logger.logExit("Failed to create cm ca cert file.\n" + output)
        self._cleanUselessFile()

    def _distributeCA(self):
        self.logger.debug("Distributing CM ca files to other hosts.")
        certPath = os.path.join(self.gaussHome, "share/sslcert/cm")
        createCertPathCmd = "rm {certPath} -rf; mkdir {certPath}; chmod 700 {certPath}".format(
            certPath=certPath)
        for host in self.hostnames:
            if host == self.localhostName:
                continue
            status, output = self.executeCmdOnHost(host, createCertPathCmd)
            if status != 0:
                errorDetail = "\nCommand: %s\nStatus: %s\nOutput: %s" % (createCertPathCmd, status, output)
                self.logger.debug(errorDetail)
                self.logger.logExit("Failed to create path of CA for CM on host %s." % host)
            # Determine if the host is an IPv6 address and format accordingly
            if ":" in host:
                formatted_host = "[{}]".format(host)
            else:
                formatted_host = host
        
            # Create the scp command with the formatted host
            # Use common_execute_cmd to safely execute scp command, avoiding injection risk
            safe_cert_path = shlex.quote(certPath)
            safe_host = shlex.quote(formatted_host)
            cmd_str = "scp %s/* %s:%s" % (safe_cert_path, safe_host, safe_cert_path)
            cmd_args = ['sh', '-c', cmd_str]
            status, output = common_execute_cmd(cmd_args)
            if status != 0:
                self.logger.debug("Command: " + str(cmd_args))
                errorDetail = "\nCommand: %s\nStatus: %s\nOutput: %s" % (scpCmd, status, output)
                self.logger.debug(errorDetail)
                self.logger.logExit("Failed to create CA for CM.")

    def createCMCA(self):
        self.logger.log("Creating CM ca files.")
        self._createCMCALocal()
        self._distributeCA()

    def run(self):
        self.logger.log("Start to install cm tool.")
        self.prepareCMPath()
        self.decompressCMPkg()
        self.createManualStartFile()
        self.initCMServer()
        self.initCMAgent()
        self.createCMCA()
        self._refreshStaticFile()
        self.setMonitorCrontab()
        self.startCluster()
