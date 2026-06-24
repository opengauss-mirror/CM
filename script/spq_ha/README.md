# SPQ 分布式高可用回调脚本

## 概述

SPQ 分布式部署中，CN 和各 DN 是独立的 openGauss 主备集群，由各自的 CM 实例管理。
当某个集群发生 failover/switchover 时，CM 只更新本集群内的角色，
但 SPQ 的 `pg_dist_node` 全局路由表不会自动感知 IP 变化。

本目录提供的回调脚本通过 CM 的 `event_triggers` 机制，在主备切换完成后自动更新
SPQ 元数据，使分布式查询路由到新主。**不修改 CM 或 SPQ 的任何 C/C++ 代码。**

## 适用场景

- 1 CN + N DN 分布式部署（每个节点可以有一个或多个备机）
- 每个 CN/DN 是独立的 openGauss 主备集群，由独立的 CM 管理
- 以下以 **1CN + 2DN、每组一主一备（共 6 节点）** 为例说明

## 文件清单

| 文件 | 部署位置 | 说明 |
|---|---|---|
| `on_dn_role_change.sh` | 所有 DN 节点 | DN 切换回调：连 CN 调 `spq_update_node()` |
| `on_cn_role_change.sh` | 所有 CN 节点 | CN 切换回调：等 recovery 后调 `spq_set_coordinator_host()` |
| `spq_topology.conf.sample` | 所有节点 | 拓扑配置模板 |

## 前提条件

部署时在所有节点的 `pg_hba.conf` 中预配全量 trust 规则，
使 CN 和所有 DN 的主备节点 IP 互相信任。
脚本运行时**不动态修改** `pg_hba.conf`，也**不需要 SSH** 到其他节点。

## 部署步骤

> 以下步骤 2、3 涉及写入 `/opt/spq/` 目录，需要以 root 用户或具有相应权限的用户执行。

以 1CN + 2DN 一主一备（6 节点）为例：

```
CN  集群: cn-primary(10.0.0.1)   cn-standby(10.0.0.2)
DN1 集群: dn1-primary(10.0.0.3)  dn1-standby(10.0.0.4)
DN2 集群: dn2-primary(10.0.0.5)  dn2-standby(10.0.0.6)
```

### 1. 配置 pg_hba.conf 全量互信

在**所有 6 个节点**的 `pg_hba.conf` 中添加所有节点 IP 的 trust 规则，
使任意节点间均可通过 gsql 免密连接：

```
host all all 10.0.0.1/32 trust
host all all 10.0.0.2/32 trust
host all all 10.0.0.3/32 trust
host all all 10.0.0.4/32 trust
host all all 10.0.0.5/32 trust
host all all 10.0.0.6/32 trust
```

> DN 数量更多时，每增加一组 DN，补上该组主备 IP 即可。

### 2. 编辑 spq_topology.conf

复制模板并按实际环境修改：

```bash
mkdir -p /opt/spq
cp spq_topology.conf.sample /opt/spq/spq_topology.conf
```

**只需配置 `CN_*` 段**，在所有节点上**保持一致**。
DN 的主备/端口由回调脚本运行时用 `cm_ctl` 实时获取，`gsql` 固定用本机 `$GAUSSHOME/bin/gsql`，均无需在此配置。

> 增加备机时，在 `CN_STANDBY_HOSTS` 中追加 IP（空格分隔）。

### 3. 部署脚本

```bash
# DN 节点（所有 DN 集群的主备节点）
install -m 755 on_dn_role_change.sh /opt/spq/

# CN 节点（CN 集群的主备节点）
install -m 755 on_cn_role_change.sh /opt/spq/
```

### 4. 配置 CM event_triggers

在每个集群**所有节点**的 `cm_agent.conf` 添加：

**DN 集群的 cm_agent.conf**：

```
event_triggers = {"on_failover":"/opt/spq/on_dn_role_change.sh","on_switchover":"/opt/spq/on_dn_role_change.sh"}
```

**CN 集群的 cm_agent.conf**：

```
event_triggers = {"on_failover":"/opt/spq/on_cn_role_change.sh","on_switchover":"/opt/spq/on_cn_role_change.sh"}
```

注意，如果参数中已有event_triggers参数，覆盖更新，否则两个不同取值，可能造成覆盖而不生效。

配置后重启 CM 生效：

```bash
cm_ctl stop -m immediate
cm_ctl start
```

## 工作原理

### DN failover/switchover

```
DN 主故障/计划切换
  → CM 提升备为新主
  → cm_agent 调用 on_dn_role_change.sh（继承 GAUSSHOME）
  → 脚本用 $GAUSSHOME/bin/cm_ctl query 解析 Datanode State，取本 DN 集群当前主 IP/端口与全部成员 IP（不再用 hostname 猜本机 IP）
  → 通过 pg_is_in_recovery() 在 CN_PRIMARY_HOST/CN_STANDBY_HOSTS 中找到 CN 主（跳过 CN 备）
  → 连 CN 在 pg_dist_node 中按「成员 IP 集合 + 端口 + primary」定位 nodeid
  → 执行 spq_update_node(<nodeid>, '<新主IP>', <新主端口>)
  → SPQ 后续查询自动路由到新主
```

### CN failover/switchover

```
CN 主故障/计划切换
  → CM 提升备为新主
  → cm_agent 调用 on_cn_role_change.sh
  → 等待本地 gaussdb 就绪（SELECT 1）
  → 等待 gaussdb 退出 recovery（pg_is_in_recovery() = false）
  → 执行 spq_set_coordinator_host('<新CN_IP>', <port>)
```

> **等待 recovery 的原因**：`cm_ctl switchover` 完成只意味着角色翻转，
> gaussdb 可能仍在做 startup recovery。此时写元数据会报
> `cannot assign TransactionIds during recovery`。

## 认证方式

DN 脚本通过 gsql 远程连接 CN 时，设置 `PGOPTIONS="-c remotetype=coordinator"`
模拟 CN 连接方式，配合 `pg_hba.conf` 的 trust 规则实现免密连接。
这与 SPQ 内部 libpq 连接使用相同的机制。

## 可调参数

在 `spq_topology.conf` 中可选配置（均有默认值）：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `SPQ_GSQL_TIMEOUT` | 10 | 单次 gsql 超时（秒） |
| `SPQ_GSQL_RETRY_COUNT` | 30 | 重试次数 |
| `SPQ_GSQL_RETRY_INTERVAL` | 2 | 重试间隔（秒） |
| `SPQ_LOCK_WAIT` | 5 | 并发锁等待（秒） |
| `SPQ_HA_LOG` | `$GAUSSLOG/cm/spq_ha/spq_ha.log` | 回调日志路径（GAUSSLOG 缺失时回退 /var/log、/tmp） |
| `SPQ_HA_LOCK_DIR` | `$GAUSSLOG/cm/spq_ha` | flock 锁目录（多集群按各自 $GAUSSLOG 自然隔离） |

## 注意事项

- `on_failover` 回调参数是新主的实例 ID（整数），`on_switchover` 无参数
- 脚本由 cm_agent 异步执行，不阻塞 CM 的倒换流程
- 如果 CN 和 DN 同时故障，DN 脚本会重试轮询所有已知 CN 地址
- 回调脚本使用 flock 本地锁避免同类事件并发执行；锁文件位于 $GAUSSLOG/cm/spq_ha/，多 DN 集群同机时按各自 $GAUSSLOG 自然隔离、互不阻塞，无需放开 /tmp 权限
- `shared_preload_libraries = 'spq'` 必须在所有节点上都配置
- 脚本日志默认写入 cm_agent 日志目录下的 $GAUSSLOG/cm/spq_ha/spq_ha.log；脚本 stderr 同时被 cm_agent 收进 $GAUSSLOG/cm/cm_agent/system_call-*.log
