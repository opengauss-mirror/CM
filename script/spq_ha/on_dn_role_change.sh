#!/usr/bin/env bash
# DN failover/switchover 回调（cm_ctl 对账版）：用继承的 GAUSSHOME 跑 cm_ctl query
# 取本集群当前主 IP/端口与成员 IP，再到 CN 的 pg_dist_node 用 spq_update_node 写回。
set -euo pipefail

CONF_FILE=${SPQ_TOPOLOGY_CONF:-/opt/spq/spq_topology.conf}
[ -f "$CONF_FILE" ] || { echo "ERROR: topology config not found: $CONF_FILE"; exit 1; }
source "$CONF_FILE"

GSQL_TIMEOUT=${SPQ_GSQL_TIMEOUT:-10}
GSQL_RETRY_COUNT=${SPQ_GSQL_RETRY_COUNT:-30}
GSQL_RETRY_INTERVAL=${SPQ_GSQL_RETRY_INTERVAL:-2}
CM_CTL_TIMEOUT=${SPQ_CM_CTL_TIMEOUT:-10}

# 日志与锁默认落到 cm_agent 日志树下的专属子目录 $GAUSSLOG/cm/spq_ha（多集群按各自 $GAUSSLOG 自然隔离、属主天然正确）；
# GAUSSLOG 缺失时回退 /var/log 与 /tmp；仍可被 conf 的 SPQ_HA_LOG / SPQ_HA_LOCK_DIR 覆盖。
if [ -n "${GAUSSLOG:-}" ]; then SPQ_HA_DIR="$GAUSSLOG/cm/spq_ha"; else SPQ_HA_DIR=""; fi
LOG=${SPQ_HA_LOG:-${SPQ_HA_DIR:+$SPQ_HA_DIR/spq_ha.log}}
LOG=${LOG:-/var/log/spq_ha.log}
LOCK_DIR=${SPQ_HA_LOCK_DIR:-${SPQ_HA_DIR:-/tmp}}

mkdir -p "$(dirname "$LOG")" 2>/dev/null || LOG=/tmp/spq_ha_$(id -un).log
touch "$LOG" 2>/dev/null || LOG=/tmp/spq_ha_$(id -un).log

log() { echo "[$(date '+%F %T')] [dn_role_change] $*" | tee -a "$LOG" >&2; }
die() { log "ERROR: $*"; exit 1; }
rt() { if command -v timeout >/dev/null 2>&1; then timeout "$1" "${@:2}"; else "${@:2}"; fi; }
on_error() { local rc=$?; log "ERROR: command failed at line ${BASH_LINENO[0]}: ${BASH_COMMAND} (rc=${rc})"; exit "${rc}"; }
on_exit() { local rc=$?; log "finished with rc=${rc}"; }
trap on_error ERR
trap on_exit EXIT

: "${CN_PRIMARY_HOST:?missing CN_PRIMARY_HOST}" "${CN_STANDBY_HOSTS:?missing CN_STANDBY_HOSTS}" "${CN_PORT:?missing CN_PORT}" "${CN_DB:?missing CN_DB}" "${CN_USER:?missing CN_USER}"

mkdir -p "$LOCK_DIR" 2>/dev/null || { LOCK_DIR=/tmp; mkdir -p "$LOCK_DIR"; }
exec 9>"$LOCK_DIR/spq_ha_dn_role_change.lock"
command -v flock >/dev/null 2>&1 && { flock -w "${SPQ_LOCK_WAIT:-5}" 9 || die "another DN callback is running"; }

log "triggered. arg=${1:-none}; GAUSSHOME=${GAUSSHOME:-unset}"
# 核心依赖判空：GAUSSHOME 由 CM agent 注入，用于定位 cm_ctl 与自带动态库；缺失即环境异常，报错退出。
: "${GAUSSHOME:?missing GAUSSHOME (CM-inherited env absent; cannot locate cm_ctl/libs)}"
[ -d "$GAUSSHOME" ] || die "GAUSSHOME is not a directory: $GAUSSHOME"
# gsql 固定用本机 $GAUSSHOME/bin/gsql（conf 不再单独配置；仍可用环境变量 GSQL_BIN 覆盖）。
GSQL_BIN=${GSQL_BIN:-$GAUSSHOME/bin/gsql}
[ -x "$GSQL_BIN" ] || die "GSQL_BIN not executable: $GSQL_BIN"
# 前置自带 lib，避免 cm_ctl/gsql 找不到 libcjson.so.1 等；写法 set -u 安全且空值不留尾随冒号。
export LD_LIBRARY_PATH="$GAUSSHOME/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

CM_CTL=${CM_CTL_BIN:-}
if [ -z "$CM_CTL" ] && [ -n "${GAUSSHOME:-}" ] && [ -x "$GAUSSHOME/bin/cm_ctl" ]; then CM_CTL=$GAUSSHOME/bin/cm_ctl; fi
if [ -z "$CM_CTL" ]; then CM_CTL=$(command -v cm_ctl || true); fi
[ -n "$CM_CTL" ] && [ -x "$CM_CTL" ] || die "cm_ctl not found (need inherited GAUSSHOME or CM_CTL_BIN)"
log "using cm_ctl: $CM_CTL"

export PGOPTIONS=${PGOPTIONS:--c remotetype=coordinator}

# on_switchover 在切换刚发起时就触发：旧主还是 Primary Demoting、新主还是 Standby Wait promoting，
# 直接读一次会拿到旧主。故轮询等稳定：本 DN 集群恰好 1 个 role=Primary 且 state=Normal，
# 再连该主用 pg_is_in_recovery()=false 二次确认已起来且是主，双确认才写 pg_dist_node。
dn_is_primary() { local r; r=$(rt "$GSQL_TIMEOUT" "$GSQL_BIN" -h "$1" -p "$2" -d "$CN_DB" -U "$CN_USER" -t -A -c "SELECT pg_is_in_recovery()" 2>/dev/null | tr -d '[:space:]' || true); [ "$r" = f ] || [ "$r" = false ] || [ "$r" = F ] || [ "$r" = FALSE ]; }
PARSE='BEGIN{s=0} /Datanode State/{s=1;next} /CMServer State/{s=0} /Cluster State/{s=0} /GTM State/{s=0} /ETCD State/{s=0} s==1{n=split($0,a,"|"); for(i=1;i<=n;i++){m=split(a[i],b," "); if(m>=9 && index(b[3],".")>0) print b[3], b[5], b[8], b[9]}}'
CM_WAIT_COUNT=${SPQ_CM_WAIT_COUNT:-30}
CM_WAIT_INTERVAL=${SPQ_CM_WAIT_INTERVAL:-2}
NEW_PRIMARY_IP=; NEW_PRIMARY_PORT=; ALL_DN_IPS=
for w in $(seq 1 $CM_WAIT_COUNT); do
    CM_OUT=$(rt $CM_CTL_TIMEOUT $CM_CTL query -Cvipd 2>/dev/null) || { sleep $CM_WAIT_INTERVAL; continue; }
    ROWS=$(echo "$CM_OUT" | awk "$PARSE")
    [ -n "$ROWS" ] || { sleep $CM_WAIT_INTERVAL; continue; }
    NP=$(echo "$ROWS" | awk '$3=="Primary" && $4=="Normal"{print $1, $2}')
    NPCOUNT=$(echo "$NP" | grep -c . || true)
    if [ x$NPCOUNT != x1 ]; then log "waiting DN cluster stable (primary-normal=$NPCOUNT)"; sleep $CM_WAIT_INTERVAL; continue; fi
    CAND_IP=$(echo "$NP" | awk '{print $1}')
    CAND_PORT=$(echo "$NP" | awk '{print $2}')
    if dn_is_primary "$CAND_IP" "$CAND_PORT"; then
        NEW_PRIMARY_IP=$CAND_IP; NEW_PRIMARY_PORT=$CAND_PORT
        ALL_DN_IPS=$(echo "$ROWS" | awk '{print $1}' | sort -u)
        break
    fi
    log "candidate $CAND_IP:$CAND_PORT not yet writable, waiting"
    sleep $CM_WAIT_INTERVAL
done
[ -n "$NEW_PRIMARY_IP" ] && [ -n "$NEW_PRIMARY_PORT" ] || { log "no stable+confirmed DN primary; last cm_ctl query output for diagnosis: ${CM_OUT:-<no cm_ctl output captured>}"; die "no stable+confirmed DN primary within $((CM_WAIT_COUNT*CM_WAIT_INTERVAL))s"; }
log "stable primary=$NEW_PRIMARY_IP:$NEW_PRIMARY_PORT members=$(echo $ALL_DN_IPS)"

cn_ok() { local r; r=$(rt "$GSQL_TIMEOUT" "$GSQL_BIN" -h "$1" -p "$CN_PORT" -d "$CN_DB" -U "$CN_USER" -t -A -c "SELECT pg_is_in_recovery()" 2>/dev/null | tr -d '[:space:]' || true); [ "$r" = f ] || [ "$r" = false ] || [ "$r" = F ] || [ "$r" = FALSE ]; }
CN_HOST=
for try in $(seq 1 "$GSQL_RETRY_COUNT"); do
    for c in "$CN_PRIMARY_HOST" $CN_STANDBY_HOSTS; do
        if cn_ok "$c"; then CN_HOST=$c; break 2; fi
    done
    sleep "$GSQL_RETRY_INTERVAL"
done
[ -n "$CN_HOST" ] || die "cannot connect to CN (tried $CN_PRIMARY_HOST $CN_STANDBY_HOSTS)"
log "connected to CN at $CN_HOST"

WHERE=
for ip in $ALL_DN_IPS; do
    if [ -z "$WHERE" ]; then WHERE="nodename = '$ip'"; else WHERE="$WHERE OR nodename = '$ip'"; fi
done

SQL_FIND="SELECT nodeid FROM pg_dist_node WHERE ($WHERE) AND nodeport = $NEW_PRIMARY_PORT AND noderole = 'primary' AND groupid > 0 LIMIT 1"
NODE_ID=$(rt "$GSQL_TIMEOUT" "$GSQL_BIN" -h "$CN_HOST" -p "$CN_PORT" -d "$CN_DB" -U "$CN_USER" -t -A -c "$SQL_FIND" 2>/dev/null || true)
[ -n "$NODE_ID" ] || die "no primary nodeid found in pg_dist_node (members=$(echo $ALL_DN_IPS) port=$NEW_PRIMARY_PORT)"

log "updating node $NODE_ID -> $NEW_PRIMARY_IP:$NEW_PRIMARY_PORT"
rt "$GSQL_TIMEOUT" "$GSQL_BIN" -h "$CN_HOST" -p "$CN_PORT" -d "$CN_DB" -U "$CN_USER" -c "SELECT spq_update_node($NODE_ID, '$NEW_PRIMARY_IP', $NEW_PRIMARY_PORT)" >> "$LOG" 2>&1 || die "spq_update_node failed for node $NODE_ID"
log "node $NODE_ID updated successfully; done"
