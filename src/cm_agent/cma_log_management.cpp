/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * CM is licensed under Mulan PSL v2.
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
 * cma_log_management.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_agent/cma_log_management.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "zlib.h"
#include "cma_global_params.h"
#include "cma_log_management.h"

/* Initialize log pattern and log count when started */
LogPattern* g_logPattern = NULL;
uint32 g_logLen = 0;

#define ONE_DAY_SECONDS (60 * 60 * 24)
#define LOG_COMPRESS_THRESHOLD ((log_max_size) * 95 / 100 * 1024 * 1024)

/*
 * The trace style is prefix_date_.log, others will be ignored.
 */
int isLogFile(const char* fileName)
{
    uint32 i;
    for (i = 0; i < g_logLen; i++) {
        if (NULL != strstr(fileName, g_logPattern[i].patternName)) {
            return 1;
        }
    }
    return 0;
}

static int isDirectoryProccessed(const char *basePath, const char * const allBasePath[], uint32 cnt)
{
    uint32 i;
    for (i = 0; i < cnt; i++) {
        if (0 == strcmp(basePath, allBasePath[i])) {
            return 0;
        }
    }
    return 1;
}

/*
 * Quick sort of trace file by time asc.
 * This time is part of trace name.
 */
int quickSort(LogFile* logFile, int low, int high)
{
    char tempFileName[MAX_PATH_LEN];
    char tempBasePath[MAX_PATH_LEN];
    char tempTimeStamp[MAX_TIME_LEN];
    char tempPattern[MAX_PATH_LEN];
    int64 fileSize;
    errno_t rc;

    /* Save the values */
    rc = strcpy_s(tempFileName, MAX_PATH_LEN, logFile[low].fileName);
    securec_check_errno(rc, (void)rc);
    rc = strcpy_s(tempBasePath, MAX_PATH_LEN, logFile[low].basePath);
    securec_check_errno(rc, (void)rc);
    rc = strcpy_s(tempPattern, MAX_PATH_LEN, logFile[low].pattern);
    securec_check_errno(rc, (void)rc);
    rc = strcpy_s(tempTimeStamp, MAX_TIME_LEN, logFile[low].timestamp);
    securec_check_errno(rc, (void)rc);
    fileSize = logFile[low].fileSize;

    /* swap the values */
    while (low < high) {
        while (low < high && strcmp(logFile[high].timestamp, tempTimeStamp) >= 0)
            high--;
        rc = strcpy_s(logFile[low].fileName, MAX_PATH_LEN, logFile[high].fileName);
        securec_check_errno(rc, (void)rc);
        rc = strcpy_s(logFile[low].basePath, MAX_PATH_LEN, logFile[high].basePath);
        securec_check_errno(rc, (void)rc);
        rc = strcpy_s(logFile[low].pattern, MAX_PATH_LEN, logFile[high].pattern);
        securec_check_errno(rc, (void)rc);
        rc = strcpy_s(logFile[low].timestamp, MAX_TIME_LEN, logFile[high].timestamp);
        securec_check_errno(rc, (void)rc);
        rc = memcpy_s(&logFile[low].fileSize, sizeof(int64), &logFile[high].fileSize, sizeof(int64));
        securec_check_errno(rc, (void)rc);
        while (low < high && strcmp(logFile[low].timestamp, tempTimeStamp) <= 0)
            low++;
        rc = strcpy_s(logFile[high].fileName, MAX_PATH_LEN, logFile[low].fileName);
        securec_check_errno(rc, (void)rc);
        rc = strcpy_s(logFile[high].basePath, MAX_PATH_LEN, logFile[low].basePath);
        securec_check_errno(rc, (void)rc);
        rc = strcpy_s(logFile[high].pattern, MAX_PATH_LEN, logFile[low].pattern);
        securec_check_errno(rc, (void)rc);
        rc = strcpy_s(logFile[high].timestamp, MAX_TIME_LEN, logFile[low].timestamp);
        securec_check_errno(rc, (void)rc);
        rc = memcpy_s(&logFile[high].fileSize, sizeof(int64), &logFile[low].fileSize, sizeof(int64));
        securec_check_errno(rc, (void)rc);
    }

    /* restore the values */
    rc = strcpy_s(logFile[low].fileName, MAX_PATH_LEN, tempFileName);
    securec_check_errno(rc, (void)rc);
    rc = strcpy_s(logFile[low].basePath, MAX_PATH_LEN, tempBasePath);
    securec_check_errno(rc, (void)rc);
    rc = strcpy_s(logFile[low].pattern, MAX_PATH_LEN, tempPattern);
    securec_check_errno(rc, (void)rc);
    rc = strcpy_s(logFile[low].timestamp, MAX_TIME_LEN, tempTimeStamp);
    securec_check_errno(rc, (void)rc);
    rc = memcpy_s(&logFile[low].fileSize, sizeof(int64), &fileSize, sizeof(int64));
    securec_check_errno(rc, (void)rc);
    return low;
}
/*
 *		Get trace pattern from cm_agent.conf.
 *		All trace pattern to be compressed are defined in cm_agent.conf.
 */
int get_log_pattern()
{
    errno_t rc;
    char* subStr = NULL;
    char* saveptr1 = NULL;
    
#ifdef ENABLE_MULTIPLE_NODES
    char logPatternName[MAX_PATH_LEN * 2] = "cm_client-,cm_ctl-,gs_clean-,gs_ctl-,gs_guc-,gs_dump-,\
gs_dumpall-,gs_restore-,gs_upgrade-,gs_initcm-,gs_initdb-,gs_initgtm-,\
gtm_ctl-,cm_agent-,system_call-,cm_server-,om_monitor-,gs_local-,\
gs_preinstall-,gs_install-,gs_replace-,gs_uninstall-,gs_om-,pssh-,\
gs_upgradectl-,gs_expand-,gs_shrink-,gs_postuninstall-,gs_backup-,\
gs_checkos-,gs_collector-,GaussReplace-,GaussOM-,gs_checkperf-,gs_check-,\
roach-agent-,roach-controller-,sync-,postgresql-,gtm-,sessionstat-,\
system_alarm-,pg_perf-,slow_query_log-,asp-,etcd-,gs_cgroup-,pscp-,\
gs_hotpatch-,cmd_sender-,uploader-,checkRunStatus-,ffic_gaussdb-,key_event-";
#else
    char logPatternName[MAX_PATH_LEN * 2] = "cm_client-,cm_ctl-,gs_clean-,gs_ctl-,gs_guc-,gs_dump-,\
gs_dumpall-,gs_restore-,gs_upgrade-,gs_initcm-,gs_initdb-,\
cm_agent-,system_call-,cm_server-,om_monitor-,gs_local-,\
gs_preinstall-,gs_install-,gs_replace-,gs_uninstall-,gs_om-,pssh-,\
gs_upgradectl-,gs_expand-,gs_shrink-,gs_postuninstall-,gs_backup-,\
gs_checkos-,gs_collector-,GaussReplace-,GaussOM-,gs_checkperf-,gs_check-,\
roach-agent-,roach-controller-,sync-,postgresql-,sessionstat-,\
system_alarm-,pg_perf-,slow_query_log-,asp-,etcd-,gs_cgroup-,pscp-,\
gs_hotpatch-,cmd_sender-,uploader-,checkRunStatus-,ffic_gaussdb-,key_event-";
#endif

    subStr = logPatternName;
    subStr = strtok_r(subStr, ",", &saveptr1);
    while (subStr) {
        rc = memcpy_s(g_logPattern[g_logLen].patternName, MAX_PATH_LEN, subStr, strlen(subStr) + 1);
        securec_check_errno(rc, (void)rc);
        g_logLen++;
        subStr = strtok_r(NULL, ",", &saveptr1);
    }

    return 0;
}

/*
 * Compressed trace to gz by zlib.
 * The gzread() function shall read data from the compressed file referenced by file,
 * which shall have been opened in a read mode (see gzopen() and gzdopen()). The gzread()
 * function shall read data from file, and   *		uncompress it into buf. At most, len
 * bytes of uncompressed data shall be copied to buf. If the file is not compressed,
 * gzread() shall simply copy data from file to buf without alteration.
 * The gzwrite() function shall write data to the compressed file referenced by file, which shall
 * have been opened in a write mode (see gzopen() and gzdopen()). On entry, buf shall point to a
 * buffer containing lenbytes of uncompressed data. The gzwrite() function shall compress this
 * data and write it to file. The gzwrite() function shall return the number of uncompressed
 * bytes actually written.
 */
int GZCompress(char *inpath, uint32 inLen, char *outpath, uint32 outLen)
{
    if (inLen == 0 || outLen == 0) {
        write_runlog(ERROR, "inPath(%s) len(%u) is 0, or outPath(%s) len(%u) is 0.\n", inpath, inLen, outpath, outLen);
        return -1;
    }
    int iLen = 0;
    int rLen = 0;
    gzFile gzfInput;
    gzFile gzfOutput;
    mode_t oumask;
    errno_t rc;

    /* define right of gun zip traces */
    oumask = umask((mode_t)((~(mode_t)(S_IRUSR | S_IWUSR | S_IXUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO)));
    if ((gzfInput = gzopen(inpath, "rb")) == NULL) {
        write_runlog(ERROR, "open input compressed log file failed,logFileName=%s\n", inpath);
        return -1;
    }

    /* Read buffer from trace and write to gun zip trace */
    if ((gzfOutput = gzopen(outpath, "wb")) != NULL) {
        char* cBuffer = (char*)malloc(GZ_BUFFER_LEN + 1);
        if (NULL == cBuffer) {
            (void)gzclose(gzfOutput);
            (void)gzclose(gzfInput);
            write_runlog(ERROR, "malloc for cBuffer failed!\n");
            return -1;
        }
        rc = memset_s(cBuffer, GZ_BUFFER_LEN + 1, 0, GZ_BUFFER_LEN + 1);
        securec_check_errno(rc, (void)rc);

        iLen = gzread(gzfInput, cBuffer, GZ_BUFFER_LEN);
        while (iLen > 0) {
            rLen = gzwrite(gzfOutput, cBuffer, iLen);
            if (rLen != iLen) {
#ifndef ENABLE_LLT
                FREE_AND_RESET(cBuffer);
                (void)gzclose(gzfOutput);
                (void)gzclose(gzfInput);
                return 0;
#endif
            }
            iLen = gzread(gzfInput, cBuffer, GZ_BUFFER_LEN);
        }
        /* set right of gun zip traces */
        (void)umask(oumask);
        FREE_AND_RESET(cBuffer);

        (void)gzclose(gzfOutput);
        (void)gzclose(gzfInput);
    } else {
        (void)gzclose(gzfInput);
        write_runlog(ERROR, "open output compressed log file failed,logFileName=%s\n", outpath);
        return -1;
    }
    return 0;
}

/*
 * Compressed by every directory and pattern.
 * This function can process different data node but have same pattern
 */
void groupByDirectoryAndPattern(LogFile* logFile, LogFile* sortLogFile, const char* pattern, const char* basePath,
                                uint32 count)
{
    errno_t rc;
    char outpath[MAX_PATH_LEN] = {'\0'};
    int32 cnt = 0;
    uint32 jj = 0;

    for (jj = 0; jj < count; jj++) {
        if (0 == strcmp(logFile[jj].pattern, pattern) && 0 == strcmp(logFile[jj].basePath, basePath) &&
            NULL == strstr(logFile[jj].fileName, ".gz")) {
            rc = memcpy_s(sortLogFile[cnt].fileName, MAX_PATH_LEN, logFile[jj].fileName, MAX_PATH_LEN);
            securec_check_errno(rc, (void)rc);
            rc = memcpy_s(sortLogFile[cnt].basePath, MAX_PATH_LEN, logFile[jj].basePath, MAX_PATH_LEN);
            securec_check_errno(rc, (void)rc);
            rc = memcpy_s(sortLogFile[cnt].timestamp, MAX_TIME_LEN, logFile[jj].timestamp, MAX_TIME_LEN);
            securec_check_errno(rc, (void)rc);
            cnt++;
        }
    }

    /* Sort traces asc */
    sortLogFileByTimeAsc(sortLogFile, 0, cnt - 1);

    /* current log will not be compressed,the last trace is current trace */
    if (cnt > 1) {
        for (jj = 0; jj < (uint32)(cnt - 1); jj++) {
            rc = snprintf_s(outpath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s%s", sortLogFile[jj].fileName, ".gz");
            securec_check_intval(rc, (void)rc);
            if (GZCompress(sortLogFile[jj].fileName, MAX_PATH_LEN, outpath, MAX_PATH_LEN) == 0) {
                /* Compress successful then remove the source trace, and chmod */
                (void)chmod(outpath, S_IRUSR);
                delLogFile(sortLogFile[jj].fileName);
            }
        }
    }
}

/*
 * Sort of trace file by time asc.
 * This time is part of trace name.
 */
void sortLogFileByTimeAsc(LogFile* logFile, int low, int high)
{
    if (low >= high) {
        return;
    }
    int pivotloc = quickSort(logFile, low, high);
    sortLogFileByTimeAsc(logFile, low, pivotloc - 1);
    sortLogFileByTimeAsc(logFile, pivotloc + 1, high);
}

/*
 * Remove a file.
 * It's always used to remove a trace compressed.
 */
void delLogFile(const char* fileName)
{
    if (unlink(fileName) < 0) {
        write_runlog(ERROR, "delete log file after compressed failed,logFileName=%s\n", fileName);
    }
}

/*
 * Compressed trace of one directory.
 * Sort all trace in the directory which not compressed,then compressed all traces
 * except the latest one.
 */
static void gzCompressLogFile(const char *pattern)
{
    char* basePath = NULL;
    errno_t rc;
    uint32 cnt = 0;
    uint32 jj = 0;
    uint32 count = 0;
    int64 totalSize = 0;
    uint32 totalCount = 0;

    if (readFileList(g_logBasePath, NULL, &totalCount, &totalSize, LOG_GUARD_COUNT_BUF) < 0) {
        return;
    }

    LogFile* logFile = (LogFile*)malloc(sizeof(LogFile) * totalCount);
    if (logFile == NULL) {
#ifndef ENABLE_LLT
        write_runlog(ERROR, "create compress logFile memory failed");
        return;
#endif
    }
    rc = memset_s(logFile, sizeof(LogFile) * totalCount, 0, sizeof(LogFile) * totalCount);
    securec_check_errno(rc, (void)rc);

    LogFile* sortLogFile = (LogFile*)malloc(sizeof(LogFile) * totalCount);
    if (sortLogFile == NULL) {
#ifndef ENABLE_LLT
        write_runlog(ERROR, "Create sortLogFile memory failed!");
        FREE_AND_RESET(logFile);
        return;
#endif
    }
    rc = memset_s(sortLogFile, sizeof(LogFile) * totalCount, 0, sizeof(LogFile) * totalCount);
    securec_check_errno(rc, (void)rc);

    /* Read all trace files */
    if (readFileList(g_logBasePath, logFile, &count, &totalSize, totalCount) < 0) {
        write_runlog(ERROR, "readFileList() fail.");
        FREE_AND_RESET(sortLogFile);
        FREE_AND_RESET(logFile);
        return;
    }

    if (count == 0) {
        write_runlog(ERROR, "gzCompressLogFile count is 0.\n");
        FREE_AND_RESET(sortLogFile);
        FREE_AND_RESET(logFile);
        return;
    }

    char** allBasePath = (char**)malloc(sizeof(char*) * count);
    if (allBasePath == NULL) {
        FREE_AND_RESET(sortLogFile);
        FREE_AND_RESET(logFile);
        write_runlog(ERROR, "create compress path memory %lu failed", sizeof(char*) * count);
        return;
    }
    
    /* Find traces of one directory */
    for (jj = 0; jj < count; jj++) {
        if (0 == strcmp(logFile[jj].pattern, pattern) && NULL == strstr(logFile[jj].fileName, ".gz")) {
            /* Skip directory that be processed	*/
            if (0 == isDirectoryProccessed(logFile[jj].basePath, allBasePath, cnt)) {
                continue;
            }

            basePath = logFile[jj].basePath;
            allBasePath[cnt] = logFile[jj].basePath;
            groupByDirectoryAndPattern(logFile, sortLogFile, pattern, basePath, count);
            /* Clear sort log buffer for next directory sort */
            rc = memset_s(sortLogFile, sizeof(LogFile) * totalCount, 0, sizeof(LogFile) * totalCount);
            securec_check_errno(rc, (void)rc);
            cnt++;
        }
    }
    write_runlog(LOG, "Compress log directory.Directory Name=%s,File Count=%u\n", pattern, cnt);
    FREE_AND_RESET(sortLogFile);
    FREE_AND_RESET(logFile);
    FREE_AND_RESET(allBasePath);
}

/*
 * Compress trace one by one pattern.
 */
static void gzCompressLogByPattern()
{
    uint32 i;
    for (i = 0; i < g_logLen; i++) {
        gzCompressLogFile(g_logPattern[i].patternName);
    }
}

/*
 * Remove oldest trace by disk capacity threshold.
 */
static void removeLogFileByCapacity()
{
    errno_t rc;
    uint32 count = 0;
    uint32 jj = 0;
    int64 totalSize = 0;
    uint32 totalCount = 0;

    if (readFileList(g_logBasePath, NULL, &totalCount, &totalSize, LOG_GUARD_COUNT_BUF) < 0) {
        return;
    }

    LogFile* logFile = (LogFile*)malloc(sizeof(LogFile) * totalCount);
    if (logFile == NULL) {
#ifndef ENABLE_LLT
        write_runlog(ERROR, "create remove logFile memory failed");
        return;
#endif
    }
    rc = memset_s(logFile, sizeof(LogFile) * totalCount, 0, sizeof(LogFile) * totalCount);
    securec_check_errno(rc, (void)rc);

    if (readFileList(g_logBasePath, logFile, &count, &totalSize, totalCount) < 0) {
        write_runlog(ERROR, "readFileList() fail.");
        FREE_AND_RESET(logFile);
        return;
    }
    sortLogFileByTimeAsc(logFile, 0, count - 1);

    /* compare total bytes of all traces and threshold,remove the oldest gun zip traces until less than threshold */
    write_runlog(
        LOG, "Total size is before deleting.Threshold=%ld,Total Size=%ld\n", LOG_COMPRESS_THRESHOLD, totalSize);
    if (totalSize > LOG_COMPRESS_THRESHOLD) {
#ifndef ENABLE_LLT
        write_runlog(LOG, "Total size is more than threshold,begin deleting.Threshold=%ld,Total Size=%ld\n",
            LOG_COMPRESS_THRESHOLD, totalSize);
        for (jj = 0; jj < count; jj++) {
            if (strstr(logFile[jj].fileName, ".gz") != NULL) {
                write_runlog(LOG, "ClearTrace,logFile[jj].fileName=%s.\n", logFile[jj].fileName);
                if (unlink(logFile[jj].fileName) == 0) {
                    totalSize -= logFile[jj].fileSize;
                    if (totalSize <= LOG_COMPRESS_THRESHOLD) {
                        write_runlog(LOG, "Total size is less than threshold,stop deleting.Threshold=%ld,Total Size=%ld\n",
                            LOG_COMPRESS_THRESHOLD, totalSize);
                        break;
                    }
                }
            }
        }
#endif
    } else {
        write_runlog(LOG, "Total size is less than threshold,needn't deleting.Threshold=%ld,Total Size=%ld\n",
            LOG_COMPRESS_THRESHOLD, totalSize);
    }
    FREE_AND_RESET(logFile);
}

/*
 * Remove by number of traces.This remove condition is limited by save days.
 * If save day is under threshold,The remove operation will not occur unless
 * guard number of trace is reach.
 */
static void removeLogFileBySavedTotality()
{
    errno_t rc;
    uint32 count = 0;
    uint32 leftCnt;
    uint32 jj = 0;
    int64 totalSize = 0;
    uint64 diffTime = 0;
    uint32 totalCount = 0;

    if (readFileList(g_logBasePath, NULL, &totalCount, &totalSize, LOG_GUARD_COUNT_BUF) < 0) {
        return;
    }

    LogFile* logFile = (LogFile*)malloc(sizeof(LogFile) * totalCount);
    if (logFile == NULL) {
#ifndef ENABLE_LLT
        write_runlog(ERROR, "create remove logFile memory failed");
        return;
#endif
    }
    rc = memset_s(logFile, sizeof(LogFile) * totalCount, 0, sizeof(LogFile) * totalCount);
    securec_check_errno(rc, (void)rc);

    /* Read all traces from log directory and sort then by time asc */
    if (readFileList(g_logBasePath, logFile, &count, &totalSize, totalCount) < 0) {
        write_runlog(ERROR, "readFileList() fail.");
        FREE_AND_RESET(logFile);
        return;
    }
    sortLogFileByTimeAsc(logFile, 0, count - 1);
    leftCnt = count;

    /* Transfer current time to integer */
    char current_localtime[LOG_MAX_TIMELEN] = {0};
    pg_time_t current_time;
    struct tm* systm = NULL;
    struct tm systm2 = {0};
    current_time = time(NULL);
    systm = localtime(&current_time);
    if (systm != NULL) {
        (void)strftime(current_localtime, LOG_MAX_TIMELEN, "%Y%m%d%H%M%S", systm);
    }

    /* Process from oldest to latest sort traces if number of traces more than threshold */
    if (count > log_max_count) {
#ifndef ENABLE_LLT
        for (jj = 0; jj < count; jj++) {
            (void)strptime(logFile[jj].timestamp, "%Y%m%d%H%M%S", &systm2);
            if (systm != NULL) {
                diffTime = (uint64)(mktime(systm) - mktime(&systm2));
            }
            /*
             * Remove gun zip traces until total traces less than save days or less than guard threshold or less than
             * maximum threshold
             */
            if (strstr(logFile[jj].fileName, ".gz") != NULL) {
                if (leftCnt > LOG_GUARD_COUNT) {
                    leftCnt -= 1;
                    write_runlog(
                        LOG, "ClearTraceByCount,logFile[jj].fileName=%s,leftCnt=%u.\n", logFile[jj].fileName, leftCnt);
                    delLogFile(logFile[jj].fileName);
                } else if (leftCnt > log_max_count) {
                    leftCnt -= 1;
                    /* Save trace as long as possible */
                    if (diffTime > (uint64)(log_saved_days * ONE_DAY_SECONDS)) {
                        write_runlog(LOG,
                            "ClearTraceByCount,logFile[jj].fileName=%s,leftCnt=%u.\n",
                            logFile[jj].fileName,
                            leftCnt);
                        delLogFile(logFile[jj].fileName);
                    }
                } else {
                    write_runlog(LOG,
                        "Total number or save days is less than threshold,stop "
                        "deleting.Threshold=%lu,CurCount=%u,diffTime=%lu,log_max_count=%u\n",
                        (uint64)(log_saved_days * ONE_DAY_SECONDS),
                        leftCnt,
                        diffTime,
                        log_max_count);
                    break;
                }
            }
        }
#endif
    } else {
        write_runlog(LOG,
            "Total number is less than threshold,needn't "
            "deleting.Threshold=%lu,CurrentCount=%u,LOG_GUARD_COUNT=%d,log_max_count=%u\n",
            (uint64)(log_saved_days * ONE_DAY_SECONDS),
            count,
            LOG_GUARD_COUNT,
            log_max_count);
    }
    FREE_AND_RESET(logFile);
}

/*
 * Execute this task for compressing and removing trace.
 * Compress non current trace at first then remove the oldest
 * traces by threshold.
 */
void* CompressAndRemoveLogFile(void* arg)
{
    for (;;) {
        /* Period of compress and remove operation */
        cm_sleep(log_threshold_check_interval);
        /* Compress trace first */
        write_runlog(LOG, "gzCompressLogByPattern begin.\n");
        gzCompressLogByPattern();
        /* Remove trace by total capacity, which is defined by LOG_COMPRESS_THRESHOLD*/
        write_runlog(LOG, "removeLogFileByCapacity begin.\n");
        removeLogFileByCapacity();
        /* 
         * Remove trace by number of traces.Save days will affect this operation
         * log_max_count = -1, means removing traces only by capacity(defined by LOG_COMPRESS_THRESHOLD).  
         */
        if (log_max_count > 0) {
            write_runlog(LOG, "removeLogFileBySavedTotality begin.\n");
            removeLogFileBySavedTotality();
        }
    }
    return NULL;
}
