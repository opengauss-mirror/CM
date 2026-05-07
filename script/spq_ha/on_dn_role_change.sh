#!/usr/bin/env bash
#
# DN failover/switchover 回调脚本
# 由 CM agent 的 event_triggers 在 DN 发生主备倒换后调用
#
# 调用方式：
#   on_failover:    on_dn_role_change.sh <new_primary_instance_id>
#   on_switchover:  on_dn_role_change.sh  （无参数）
#
# 功能：连接 CN 的 SPQ，更新 pg_dist_node 中本 DN 集群的地址
#
set -euo pipefail

CONF_FILE="${SPQ_TOPOLOGY_CONF:-/opt/spq/spq_topology.conf}"
[ -f "${CONF_FILE}" ] || { echo "ERROR: topology config not found: ${CONF_FILE}"; exit 1; }
source "${CONF_FILE}"

LOG="${SPQ_HA_LOG:-/var/log/spq_ha.log}"
NEW_PRIM_INST_ID="${1:-}"
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
    printf '[%s] [dn_role_change] %s\n' "${ts}" "$*" | tee -a "${LOG}" >&2
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
    require_uint DN_COUNT
    require_var GSQL_BIN
    [ -x "${GSQL_BIN}" ] || die "GSQL_BIN is not executable: ${GSQL_BIN}"

    local dn_idx
    for dn_idx in $(seq 1 "${DN_COUNT}"); do
        require_var "DN_${dn_idx}_PRIMARY_HOST"
        require_var "DN_${dn_idx}_STANDBY_HOSTS"
        require_uint "DN_${dn_idx}_PORT"
    done
}

acquire_lock() {
    mkdir -p "${LOCK_DIR}"
    exec 9>"${LOCK_DIR}/spq_ha_dn_role_change.lock"
    if command -v flock >/dev/null 2>&1; then
        flock -w "${SPQ_LOCK_WAIT:-5}" 9 || die "another DN role-change callback is still running"
    fi
}

configured_dn_ips() {
    local dn_idx dn_primary_var dn_standbys_var
    for dn_idx in $(seq 1 "${DN_COUNT}"); do
        dn_primary_var="DN_${dn_idx}_PRIMARY_HOST"
        dn_standbys_var="DN_${dn_idx}_STANDBY_HOSTS"
        printf '%s\n' "${!dn_primary_var}"
        for ip in ${!dn_standbys_var}; do
            printf '%s\n' "${ip}"
        done
    done
}

detect_local_dn_ip() {
    local host_ip config_ip
    for host_ip in $(hostname -I); do
        for config_ip in $(configured_dn_ips); do
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

log "triggered. instance_id_arg=${NEW_PRIM_INST_ID:-none}"

MY_IP=$(detect_local_dn_ip) || die "local IPs '$(hostname -I)' do not match configured DN hosts"

log "new DN primary IP: ${MY_IP}"

export PGOPTIONS="${PGOPTIONS:--c remotetype=coordinator}"

# 查找可用的 CN 主（遍历主机和所有备机，排除 recovery 状态的备机）
find_cn_host() {
    local attempt cn_ip result
    for attempt in $(seq 1 "${GSQL_RETRY_COUNT}"); do
        for cn_ip in "${CN_PRIMARY_HOST}" ${CN_STANDBY_HOSTS}; do
            result=$(run_with_timeout "${GSQL_TIMEOUT}" "${GSQL_BIN}" -h "${cn_ip}" -p "${CN_PORT}" -d "${CN_DB}" -U "${CN_USER}" \
                -t -A -c "SELECT pg_is_in_recovery()" 2>/dev/null | tr -d '[:space:]' || echo "")
            case "${result}" in
                f|false|F|FALSE)
                    echo "${cn_ip}"
                    return 0
                    ;;
            esac
        done
        sleep "${GSQL_RETRY_INTERVAL}"
    done
    return 1
}

CN_HOST=$(find_cn_host) || die "cannot connect to CN (tried ${CN_PRIMARY_HOST} ${CN_STANDBY_HOSTS})"
log "connected to CN at ${CN_HOST}"

# 遍历所有 DN 集群，匹配当前 IP 找到自己属于哪个集群
UPDATED=false
for dn_idx in $(seq 1 ${DN_COUNT}); do
    dn_primary_var="DN_${dn_idx}_PRIMARY_HOST"
    dn_standbys_var="DN_${dn_idx}_STANDBY_HOSTS"
    dn_port_var="DN_${dn_idx}_PORT"
    DN_PRIMARY="${!dn_primary_var}"
    DN_STANDBYS="${!dn_standbys_var}"
    DN_PORT_VAL="${!dn_port_var}"

    MATCHED=false
    # 检查主机和所有备机
    for check_ip in "${DN_PRIMARY}" ${DN_STANDBYS}; do
        if [ "${MY_IP}" = "${check_ip}" ]; then
            MATCHED=true
            break
        fi
    done

    if [ "${MATCHED}" = true ]; then
        # 找到本集群，用所有可能的 IP 去 pg_dist_node 里匹配 nodeid
        ALL_IPS="${DN_PRIMARY} ${DN_STANDBYS}"
        WHERE_CLAUSE=""
        for ip in ${ALL_IPS}; do
            if [ -z "${WHERE_CLAUSE}" ]; then
                WHERE_CLAUSE="nodename = '${ip}'"
            else
                WHERE_CLAUSE="${WHERE_CLAUSE} OR nodename = '${ip}'"
            fi
        done

        NODE_ID=$(run_with_timeout "${GSQL_TIMEOUT}" "${GSQL_BIN}" -h "${CN_HOST}" -p "${CN_PORT}" -d "${CN_DB}" -U "${CN_USER}" \
            -t -A -c "SELECT nodeid FROM pg_dist_node
                      WHERE (${WHERE_CLAUSE})
                      AND noderole = 'primary' AND groupid > 0
                      LIMIT 1" 2>/dev/null || true)

        if [ -n "${NODE_ID}" ]; then
            log "updating node ${NODE_ID}: -> ${MY_IP}:${DN_PORT_VAL}"
            run_with_timeout "${GSQL_TIMEOUT}" "${GSQL_BIN}" -h "${CN_HOST}" -p "${CN_PORT}" -d "${CN_DB}" -U "${CN_USER}" \
                -c "SELECT spq_update_node(${NODE_ID}, '${MY_IP}', ${DN_PORT_VAL})" \
                >> "${LOG}" 2>&1 || die "spq_update_node failed for node ${NODE_ID}"
            UPDATED=true
            log "node ${NODE_ID} updated successfully"
        else
            die "could not find nodeid for DN${dn_idx} (IPs: ${ALL_IPS})"
        fi
        break
    fi
done

if [ "${UPDATED}" = false ]; then
    die "MY_IP=${MY_IP} did not match any known DN in topology config"
fi

log "done"
