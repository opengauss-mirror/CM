#!/usr/bin/env bash
#
# CN failover/switchover 回调脚本
# 由 CN 集群的 CM agent 的 event_triggers 在 CN 发生主备倒换后调用
#
# 前提：部署时已在所有节点的 pg_hba.conf 中预配全量 trust 规则（CN/DN 主备 IP 全互信），
#       因此运行时无需动态修改 pg_hba.conf，也无需 SSH 到其他节点。
#
set -euo pipefail

CONF_FILE="${SPQ_TOPOLOGY_CONF:-/opt/spq/spq_topology.conf}"
[ -f "${CONF_FILE}" ] || { echo "ERROR: topology config not found: ${CONF_FILE}"; exit 1; }
source "${CONF_FILE}"

LOG="${SPQ_HA_LOG:-/var/log/spq_ha.log}"
GSQL_TIMEOUT="${SPQ_GSQL_TIMEOUT:-10}"
GSQL_RETRY_COUNT="${SPQ_GSQL_RETRY_COUNT:-30}"
GSQL_RETRY_INTERVAL="${SPQ_GSQL_RETRY_INTERVAL:-2}"
LOCK_DIR="${SPQ_HA_LOCK_DIR:-/tmp}"

init_log() {
    local log_dir
    log_dir=$(dirname "${LOG}")
    if ! mkdir -p "${log_dir}" 2>/dev/null || ! touch "${LOG}" 2>/dev/null; then
        LOG="/tmp/spq_ha_$(id -un).log"
        mkdir -p "$(dirname "${LOG}")"
        touch "${LOG}"
    fi
}

log() {
    local ts
    ts=$(date '+%Y-%m-%d %H:%M:%S')
    printf '[%s] [cn_role_change] %s\n' "${ts}" "$*" | tee -a "${LOG}" >&2
}

die() {
    log "ERROR: $*"
    exit 1
}

on_error() {
    local rc=$?
    log "ERROR: command failed at line ${BASH_LINENO[0]}: ${BASH_COMMAND} (rc=${rc})"
    exit "${rc}"
}

on_exit() {
    local rc=$?
    log "finished with rc=${rc}"
}

run_with_timeout() {
    local seconds=$1
    shift
    if command -v timeout >/dev/null 2>&1; then
        timeout "${seconds}" "$@"
    else
        "$@"
    fi
}

require_var() {
    local name=$1
    if [ -z "${!name:-}" ]; then
        die "required config ${name} is empty"
    fi
}

require_uint() {
    local name=$1
    require_var "${name}"
    if ! [[ "${!name}" =~ ^[0-9]+$ ]]; then
        die "required config ${name} must be an integer, got '${!name}'"
    fi
}

validate_config() {
    require_var CN_PRIMARY_HOST
    require_var CN_STANDBY_HOSTS
    require_uint CN_PORT
    require_var CN_DB
    require_var CN_USER
    require_var GSQL_BIN
    [ -x "${GSQL_BIN}" ] || die "GSQL_BIN is not executable: ${GSQL_BIN}"
}

acquire_lock() {
    mkdir -p "${LOCK_DIR}"
    exec 9>"${LOCK_DIR}/spq_ha_cn_role_change.lock"
    if command -v flock >/dev/null 2>&1; then
        flock -w "${SPQ_LOCK_WAIT:-5}" 9 || die "another CN role-change callback is still running"
    fi
}

configured_cn_ips() {
    printf '%s\n' "${CN_PRIMARY_HOST}"
    for ip in ${CN_STANDBY_HOSTS}; do
        printf '%s\n' "${ip}"
    done
}

detect_local_cn_ip() {
    local host_ip config_ip
    for host_ip in $(hostname -I); do
        for config_ip in $(configured_cn_ips); do
            if [ "${host_ip}" = "${config_ip}" ]; then
                printf '%s\n' "${host_ip}"
                return 0
            fi
        done
    done
    return 1
}

init_log
trap on_error ERR
trap on_exit EXIT
validate_config
acquire_lock

NEW_CN_IP=$(detect_local_cn_ip) || die "local IPs '$(hostname -I)' do not match configured CN hosts"
log "triggered. new CN primary IP: ${NEW_CN_IP}"

export PGOPTIONS="${PGOPTIONS:--c remotetype=coordinator}"

retry_gsql() {
    local host=$1 port=$2 db=$3 user=$4 sql=$5
    local attempts=0

    while [ "${attempts}" -lt "${GSQL_RETRY_COUNT}" ]; do
        if run_with_timeout "${GSQL_TIMEOUT}" "${GSQL_BIN}" -h "${host}" -p "${port}" -d "${db}" -U "${user}" \
           -c "${sql}" >> "${LOG}" 2>&1; then
            return 0
        fi
        attempts=$((attempts + 1))
        sleep "${GSQL_RETRY_INTERVAL}"
    done
    return 1
}

run_gsql_or_die() {
    local host=$1 port=$2 db=$3 user=$4 sql=$5 desc=$6
    retry_gsql "${host}" "${port}" "${db}" "${user}" "${sql}" || die "${desc} failed"
}

# 等待本地 gaussdb 退出 recovery 状态。
# cm_ctl switchover 完成只意味着角色翻了，但 gaussdb 主进程还可能在做 startup recovery，
# 此时写元数据（spq_set_coordinator_host）会报 "cannot assign TransactionIds during recovery"。
wait_for_recovery_complete() {
    local attempts=0
    local result
    while [ "${attempts}" -lt "${GSQL_RETRY_COUNT}" ]; do
        result=$(run_with_timeout "${GSQL_TIMEOUT}" "${GSQL_BIN}" \
            -h 127.0.0.1 -p "${CN_PORT}" -d "${CN_DB}" -U "${CN_USER}" \
            -t -A -c "SELECT pg_is_in_recovery()" 2>/dev/null | tr -d '[:space:]' || echo "")
        case "${result}" in
            f|false|F|FALSE)
                return 0
                ;;
        esac
        attempts=$((attempts + 1))
        sleep "${GSQL_RETRY_INTERVAL}"
    done
    return 1
}

# step 1: 等待本地 gaussdb 就绪
log "step 1: waiting for local gaussdb to accept connections"
if ! retry_gsql "127.0.0.1" "${CN_PORT}" "${CN_DB}" "${CN_USER}" "SELECT 1"; then
    die "local gaussdb not ready"
fi

# step 2: 等待 gaussdb 退出 recovery（switchover 后角色已翻但仍在 startup recovery 时写元数据会报 'cannot assign TransactionIds during recovery'）
log "step 2: waiting for gaussdb to exit recovery"
if ! wait_for_recovery_complete; then
    die "gaussdb is still in recovery after $((GSQL_RETRY_COUNT * GSQL_RETRY_INTERVAL))s; aborting to avoid TransactionId errors"
fi

# step 3: 更新 SPQ 协调器地址
log "step 3: updating SPQ coordinator host to ${NEW_CN_IP}"
run_gsql_or_die "127.0.0.1" "${CN_PORT}" "${CN_DB}" "${CN_USER}" \
    "SELECT spq_set_coordinator_host('${NEW_CN_IP}', ${CN_PORT})" \
    "spq_set_coordinator_host"

log "done"
