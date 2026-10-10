# Speedboat 使用手册

## 目录

- [概述](#概述)
- [快速开始](#快速开始)
- [配置详解](#配置详解)
- [API 参考](#api-参考)
- [部署指南](#部署指南)
- [进阶用法](#进阶用法)
- [故障排查](#故障排查)

---

## 概述

Speedboat 是一个极简的 Raft 共识算法实现，专注于主节点选举场景。

### 核心特性

- **极简 API**：`Speedboat.start(config)` 一行启动
- **集群视角配置**：用户只需配置 `datacenter + nodes`
- **自动匹配**：系统自动检测本机 IP 并匹配节点
- **自动模式**：单机房扁平模式，跨机房级联模式
- **零依赖配置**：默认 PropertiesConfigProvider，可选实现 JSON/YAML

### 适用场景

- 分布式系统主节点选举
- 跨机房高可用部署
- 微服务主备切换
- 多名目互斥发布（命名锁：报价/开幕/定时任务等"同名目恰好一个活跃者"）

---

## 快速开始

### 1. 添加依赖

```xml
<dependency>
    <groupId>cn.itcraft</groupId>
    <artifactId>speedboat</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 2. 创建配置文件

**config.properties**（单机房）：

```properties
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000
```

### 3. 启动集群

```java
import cn.itcraft.speedboat.Speedboat;
import cn.itcraft.speedboat.config.PropertiesConfigProvider;
import cn.itcraft.speedboat.config.SpeedboatConfigProvider;

public class Application {
    public static void main(String[] args) {
        SpeedboatConfigProvider config = new PropertiesConfigProvider("config.properties");
        Speedboat.start(config);
        
        if (Speedboat.isMain()) {
            System.out.println("I am the leader!");
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Speedboat.stop();
        }));
    }
}
```

---

## 配置详解

### Properties 配置格式

#### 单机房场景

```properties
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000

election.intra.timeout.min=1000
election.intra.timeout.max=2000
```

#### 跨机房场景

```properties
datacenter=hangzhou001

nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000

nodes.1.0=10.3.1.1:3000
nodes.1.1=10.3.1.2:3000
nodes.1.2=10.3.1.3:3000

election.intra.timeout.min=1000
election.intra.timeout.max=2000
election.cross.timeout.min=3000
election.cross.timeout.max=5000
```

### 配置项说明

| 配置项 | 必需 | 默认值 | 说明 |
|-------|------|-------|------|
| `nodes.{dc}.{node}` | ✅ | - | 节点地址，格式 `ip:port` |
| `datacenter` | ❌ | `dc-{index}` | 机房 ID |
| `election.intra.timeout.min` | ❌ | 1000 | 机房内选举超时下限（ms） |
| `election.intra.timeout.max` | ❌ | 2000 | 机房内选举超时上限（ms） |
| `election.cross.timeout.min` | ❌ | 3000 | 机房间选举超时下限（ms）——**父组**选举窗口 |
| `election.cross.timeout.max` | ❌ | 5000 | 机房间选举超时上限（ms）——**父组**选举窗口 |
| `cross.port.offset` | ❌ | 1000 | 父组绑定端口 = 子组端口 + 本偏移（**全集群必须一致**，否则两组互不可达） |
| `datacenter.weight.{机房ID}` | ❌ | 按索引派生 | 父组机房权重，键为机房 ID（如 `datacenter.weight.dc-0=2`）；未列出的机房走兜底权重 1 |
| `vote.weight.strategy` | ❌ | none | 投票权重策略：`prefer` / `even` / `none` |
| `consistency.policy` | ❌ | cp | 一致性策略：`cp`（强一致唯一性，绝不双主，整机房死亡退化为无主）/ `ap`（可用性优先，对侧机房失联超阈值后降级接管，分区期间可能短暂双主）。**启动时选定，全生命周期恒定**；双机房级联父组专用，单机房不受影响 |
| `consistency.degraded.timeout.ms` | ❌ | 10000 | 仅 `ap` 生效：对侧机房持续无应答多久后允许降级接管；应显著大于 check-quorum 新鲜阈值（5s）与跨机房选举超时上界（5s） |
| `promote.term.leap` | ❌ | 100 | 人工提升机房优先级时的 term 跃升幅度（须 > 0）；越大越能确保对侧旧任期在愈合后退让，代价是 term 更快增长 |
| `vote.weight.prefer` | ⚠️ prefer 必填 | - | 享受额外权重的节点 ID（格式 `node-<ip>-<port>`） |
| `vote.weight.prefer.weight` | ❌ | 3 | 该节点获得的额外权重 |
| `raft.persistence` | ❌ | mmap | 持久化三档：`mmap`（默认，跨进程挂回）/ `mem`（显式档，重启即全丢） / `none` |
| `raft.persistence.dir` | ❌ | `speedboat-data` | mmap 档持久化目录（按 `{nodeId}` 自动分子目录） |
| `raft.mmap.size.mb` | ❌ | 128 | mmap 档 WAL 区域大小上限 |
| `raft.mmap.file.name` | ❌ | `raft.mmap` | mmap 文件名（可自定义检测 `{nodeId}` 占位） |
| `raft.max.log.size` | ❌ | 4096 | 内存日志上限（defect-20260918-01：重启副本重建判定状态的唯一依据，不可截断过深） |
| `raft.checkpoint.interval` | ❌ | 1024 | 状态机检查点触发阈值（applied 增量；仅 mmap 档生效，放大省检查点/缩小省重启恢复） |
| `lock.lease.ms` | ❌ | 30000 | 分布式锁默认租约时长（ms；越短失联迁移越快、续期开销越高） |

### ⚠️ 投票权重：全网一致性约束

权重表是**集群级拓扑事实**，不是本机视角。"给节点 X 权重 3"的含义是：
**每一台机器的配置都要表达同一个事实** —— 相同的 `vote.weight.prefer`、
相同的 `vote.weight.prefer.weight`（或全部不配置）。

**为什么**：每个节点独立计算候选者权重与 required 多数派，再交换选票。
若 A 机认为权重表是 `(4,1,1,1)` 而 B 机认为 `(1,1,1,1)`，两端会算出
**不同的 total/required**，同一 term 内可能宣布**不同的胜者** —— 双主。

**实机验证**（4 节点、全机器权重表 `(3,1,1,1)` → total 6 / required 4）：

- 三个普通节点 = 3 < 4 → 单独永远构不成多数派
- 权重节点（1+3=4）+ 任一普通 = 5 ≥ 4 ✓
- 结论：**该拓扑下任何 leader 都必须由"包含加权节点"的多数派选出**，
  这正是加权选举的路由语义。


### 节点索引规则

```
nodes.{机房索引}.{节点索引}=ip:port
```

- **机房索引**：从 0 开始，递增
- **节点索引**：从 0 开始，递增
- **示例**：
  - `nodes.0.0` → 机房0，节点0
  - `nodes.0.1` → 机房0，节点1
  - `nodes.1.0` → 机房1，节点0

### 自动模式判断

| nodes 第一层大小 | 模式 | 说明 |
|----------------|------|------|
| `== 1` | 单机房扁平模式 | 所有节点平等选举 |
| `> 1` | 跨机房级联模式 | 本机房选举 + 跨机房级联 |

---

## API 参考

### 静态方法

#### 启动集群

```java
Speedboat.start(SpeedboatConfigProvider config)
```

启动单例实例。如果已运行，则忽略。

**参数**：
- `config` - 配置提供者

**示例**：
```java
SpeedboatConfigProvider config = new PropertiesConfigProvider("config.properties");
Speedboat.start(config);
```

#### 停止集群

```java
Speedboat.stop()
```

停止单例实例并释放资源。

#### 判断是否主节点

```java
boolean isMain = Speedboat.isMain()
```

返回当前节点是否是集群主节点。

**返回**：
- `true` - 当前节点是主节点
- `false` - 当前节点是从节点

#### 获取主节点 ID

```java
String leaderId = Speedboat.getLeaderId()
```

返回当前主节点的 ID。

**返回**：
- 主节点 ID（如 `server01-192.168.10.1-0012`）
- 未选举时返回 `null`

#### 获取当前任期

```java
long term = Speedboat.getTerm()
```

返回当前 Raft 任期号（跨机房级联下为**子组**任期）。

#### 跨机房级联：双层观测 API

跨机房级联模式下每个进程持有**两套彼此独立的 Raft**：

- **子组**（机房内层，端口 `21001`）→ 选出"本机房代表"
- **父组**（跨机房层，端口 `21001 + cross.port.offset`）→ 各机房代表角逐"全局主"

因此状态分两组暴露，两组 `term` **各自单调、互不污染**，没有换算关系。

| 方法 | 说明 |
|---|---|
| `boolean isIntraLeader()` | 是否**子组** Leader，即"本机房代表" |
| `boolean isParentLeader()` | 是否**父组** Leader（单机房无父组，恒为 `true`） |
| `long getIntraTerm()` | 子组（机房内）任期 |
| `long getParentTerm()` | 父组（跨机房）任期；单机房返回 `0` |
| `String getIntraLeaderId()` | 子组当前已知 Leader 的 nodeId |
| `String getParentLeaderId()` | 父组当前已知 Leader 的**父组** nodeId；单机房返回 `null` |
| `String getParentNodeId()` | 本进程的父组 nodeId（按父组端口推导）；单机房返回 `null` |
| `int getParentPort()` | 父组绑定端口；单机房返回 `0` |

**三者关系**：`isMain() == isIntraLeader() && isParentLeader()`。

```java
// 备机房在主机房存活期间的【预期形态】，不是故障
Speedboat.isIntraLeader();   // true  —— 我是本机房代表
Speedboat.isParentLeader();  // false —— 但未在父组当选
Speedboat.isMain();          // false —— 故不构成全局主
```

> ⚠️ **监控口径**：`getParentLeaderId()` 与 `getIntraLeaderId()` 取值域**不同**
> （前者按父组端口推导，如 `node-x.x.x.x-22001`），两者不可混用比较。
> 只看 `leader` 字段会把"两房各有一个 Leader"误判为正常，
> **全局唯一性必须以 `isMain() == true` 的计数为准**：任意时刻 ≤ 1。

#### 获取本节点 ID

```java
String nodeId = Speedboat.getNodeId()
```

返回本节点的自动生成 ID，格式：`hostname-ip-random`。

#### 获取机房 ID

```java
String dcId = Speedboat.getDatacenterId()
```

返回本机房 ID。

#### 检查运行状态

```java
boolean running = Speedboat.isRunning()
```

返回单例实例是否正在运行。

#### 人工提升机房优先级（运维兜底）

```java
boolean ok = Speedboat.promoteDatacenter(operator, datacenterId, reason)
```

跨机房模式下，把本机房在父组的优先级提升到"自投即成主"。详见部署指南
「人工升级与回退」小节。**仅 CP 模式生效**：AP 模式下调用被忽略（只落审计日志，
不改状态、不抛异常），返回 `false`。

#### 人工回退优先级到配置值

```java
boolean ok = Speedboat.restoreDefaultPriorities(operator, reason)
```

把优先级表回退到配置派生值。回退不跃升 term、不夺主。与提升一样**仅 CP 模式生效**，
AP 模式下被忽略。

#### 查看当前优先级快照

```java
DatacenterPriorityTable.Snapshot snap = Speedboat.getPrioritySnapshot()
```

返回父组当前优先级快照（epoch + 权重表 + 来源）；单机房/子组返回 `null`。

### 分布式命名锁

> **语义要点（2026-09-18 设计落地后）**：主节点与锁是两个正交抽象——
> Leader 仅负责锁命令的日志复制与全序化；**任意成员**都可申请任意名目的锁，
> 经 Raft 日志共识成功即持有；同名目任意时刻**至多一个持有者**（互斥硬约束）；
> 持有者失联 → 租约到期 → 他人可公平竞争接管（**非抢占**，不抢存量持有者的锁）。

#### 获取锁对象

```java
DistributedLock lock = Speedboat.getLock("forex")
```

- 锁名即"名目"（如 forex/metals/commodity），同集群可多把锁并存、持有者互不相同
- Leader 本地申请与 follower 转发共用一套判定；判定只发生在状态机 apply（日志全序）

#### 申请 / 释放 / fencing token

```java
LockHandle handle = lock.tryLock(5000);
if (handle.isSuccess()) {
    long epoch = handle.getEpoch();   // fencing token：所有权代际，单调递增
    // ... 业务持有期（框架自动每 leaseMs/2 续期：RENEW 被拒立即停发续期）
    handle.close();                   // 释放（全网生效后返回）
}
```

**关键语义**：

| 语义 | 说明 |
|------|------|
| 互斥 | 同名目锁任意时刻至多一个持有者；被拒时判定结果立即可读（不再超时盲猜） |
| epoch fencing | 下游信凭 `lockName + epoch` 单调递增拒绝旧主（旧持有者分区自认持有的旧 token 一律拒收）|
| 非抢占 | 已被持有的锁不会被夺回；原持有者恢复后只能等新持有者失联或释放 |
| 迁移时延 | 持有者失联 → ≤ leaseMs 到期 → 接管；Leader 宕机额外 ≤ 1 选举超时时锁状态变更暂停（租约倒计时不受影响） |
| 跨锁并存 | 同一节点可同时持多把锁（名目间互不阻塞） |
| 失锁可观测 | `lock.isLost()`：RENEW 被拒或本地视图失效即为 true；**发布前必须校验**（无回调，轮询为唯一通道） |
| 公平性 | 非抢占=不保证无饥饿（先到先得续期即长持）；申请失败重试含 ±50% 抖动防惊群 |

#### 重启副本边界（2026-09-18 P4 起：三档持久化解决）

`raft.persistence` 三档持久化（**默认 mmap**——写代价与 mem 同量级，但跨进程重启安全性严格占优，因此缺省承载最安全档）落地后，进程重启副本可挂回检查点/WAL 确定性地重建锁表与 epoch，epoch 单调性全网成立（真网已复核：重启副本接管迁移 grant 的 epoch 与长驻副本一致）。
- **mem（缺省）**：进程内可恢复，跨进程重启等同"首次启动"，仅适合长驻进程场景；
- **mmap**（`raft.persistence=mmap`，默认目录 `./speedboat-data/{nodeId}/`、默认 128MB 单文件）：重启挂回，锁表 epoch 跨重启保真；
- **none**（`raft.persistence=none`）：NopRaftStore 测试基线。

---

## 部署指南

### 单机房部署

#### 环境要求

- JDK 8+
- 网络互通

#### 部署步骤

1. **创建配置文件** `config.properties`：

```properties
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000
```

2. **分发配置**：每台机器放置相同的配置文件。

3. **启动应用**：每台机器执行：

```bash
java -jar your-app.jar
```

4. **验证**：检查日志确认选举完成。

### 跨机房部署

#### 环境要求

- JDK 8+
- 机房间网络可达，且**子组与父组两个端口都要放行**（见下）
- 机房内网络低延迟

#### 部署步骤

1. **创建配置文件** `config.properties`（**两机房共用同一份**，机房 ID 才能保证唯一）：

```properties
# 杭州机房（机房0）
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000

# 北京机房（机房1）
nodes.1.0=10.3.1.1:3000
nodes.1.1=10.3.1.2:3000
nodes.1.2=10.3.1.3:3000

# 父组端口 = 子组端口 + 偏移 → 3000 + 1000 = 4000（全集群必须一致）
cross.port.offset=1000

# 父组机房权重：主机房 2、备机房 1（键 = 机房 ID，未配置时按索引派生：权重 = 机房总数 - 索引）
datacenter.weight.dc-0=2
datacenter.weight.dc-1=1

# 子组（机房内）选举超时：同机房低延迟，可用短窗口
election.intra.timeout.min=1000
election.intra.timeout.max=2000
# 父组（跨机房）选举超时：跨机房链路抖动大，需更长窗口避免 term 膨胀
election.cross.timeout.min=3000
election.cross.timeout.max=5000
```

> **不要**给两机房配置不同的 `datacenter=` 值——跨机房模式下机房 ID 由
> `datacenter` 配置值 **自动追加 `-<机房索引>`** 生成，两份配置若基准值不同，
> 或两机房被解析到同一 ID，都会让按机房判权的策略失效。
> **推荐不配置 `datacenter`**，直接得到 `dc-0` / `dc-1`，并在 `event=START` 日志的 `dc=` 字段核对。

2. **防火墙**：放行**两个**端口的机房间互访——

   | 组 | 端口 | 用途 |
   |---|---|---|
   | 子组 | `3000` | 机房内选举与复制 |
   | 父组 | `3000 + cross.port.offset` = `4000` | 跨机房级联选举（`isMain` 判定） |

   父组端口不通时**不会报错**，表现为父组永远选不出主、`isMain` 恒为 `false`。

3. **分发配置**：同一份 `config.properties` 分发到全部 6 台机器。

4. **启动应用**：所有机器执行：

```bash
java -jar your-app.jar
```

5. **验证**：检查日志确认两级选举完成——

```text
Parent group built: nodeId=node-x.x.x.x-4000, port=4000, peers=[...], weights={dc-0=2, dc-1=1}
```

   随后确认**全局唯一主**：`isMain == true` 的进程数必须 ≤ 1（详见「跨机房级联：双层观测 API」）。

> ⚠️ **父组唯一性语义**：主机房凭权重 2 ≥ required 2 可**单方成主**（杀光备机房仍为 1 主）；
> 备机房权重 1 < 2 **不可**单方成主，故**杀光主机房会退化为「无主」而非备机房接管**。
> 这是"绝不允许双主"硬约束的必然代价，属**预期行为**，需按 P1 告警处置并走人工介入。

### CP/AP 一致性双模式

父组在"整机房失联"时存在两种互斥取舍，由 `consistency.policy` 在**启动时**选定，
全生命周期恒定（运行期不可切换——切换等同于改变多数派定义，会破坏安全性）。

| 模式 | 取舍 | 整机房失联时 | `isMain` 全网计数 |
|---|---|---|---|
| `cp`（缺省） | 强一致唯一性 | 退化为**无主**（0 主），绝不接管 | 恒 ≤ 1 |
| `ap` | 可用性优先 | 失联超 `consistency.degraded.timeout.ms`（缺省 10s）后**降级接管**，本机房单方成主 | 分区期间可能短暂为 2 |

```properties
# 可用性优先：对侧机房失联 10s 后降级接管
consistency.policy=ap
consistency.degraded.timeout.ms=10000
```

**AP 降级接管的条件（全部满足）**：本进程持有本机房代表席位、存在明确的对侧机房、
对侧机房持续无应答达阈值。降级后把对侧机房剔除出多数派分母——备机房由 `self(1) < required(2)`
变为 `self(1) ≥ required(1)`，从而可单方成主。对侧恢复应答后**自动退出**降级、
分母恢复全量，走标准 Raft 任期机制收敛为单主。

**审计日志**：进入与退出降级各落一条 `WARN` 级 `CONSISTENCY AUDIT` 日志，
含机房、失联时长、阈值与原因，供事后追溯双主窗口。

**观测口径差异**：

- CP 模式：沿用既有口径，`isMain == true` 的进程数**任何时刻 ≤ 1**。
- AP 模式：分区窗口内该计数**可能短暂为 2**（两房各有一个自认的主）。告警规则需按模式区分——
  CP 下"计数 > 1"是 P0 故障，AP 下是**预期内现象**，仅在分区愈合后仍 > 1 才是故障。
- 无论哪种模式，**愈合后**都必须收敛到恰好 1 主（或 0 主，仅 CP 整机房全灭）。

> ⚠️ **AP 的代价**：降级接管意味着分区期间两房可能各自向客户端确认写入。
> 仅在"两房数据可最终合并/业务可容忍短暂双写"的场景启用；对数据强一致有硬要求时保持 `cp`（默认）。

### 人工升级与回退（运维兜底）

跨机房父组在**主机房整体失联**时，备机房因自身权重低（如 `1 < required 2`）无法成主，
CP 下退化为无主。此时可由运维通过门面 API **显式提升**备机房在父组的优先级，
使其自投成主、恢复 `isMain` 唯一主语义。这是一条**人工兜底通路**，不自动触发、不做远程接口、不做控制台。

> **模式适用范围（仅 CP）**：人工切主**只对 `consistency.policy=cp` 生效**。
> AP 模式已显式接受"分区期间短暂双主"，"把某机房提为唯一主"在该模式下没有定义良好的语义，
> 故 AP 下调用被**忽略**——只落一条 `op=IGNORED` 的 `PROMOTE-AUDIT` 审计日志，
> **不改任何状态、不抛异常、不返回错误**，返回值为 `false`。
> 这是刻意的：让运维脚本把"模式不适用"当作可识别的空操作，而非故障去重试。
> `getPrioritySnapshot()` 是只读查询，AP 下照常可用。

#### 提升：`promoteDatacenter`

```java
// 在备机房的任一代表进程上调用（operator 必填，用于审计追溯）
boolean ok = Speedboat.promoteDatacenter("alice", "beijing002", "hangzhou 机房整体下线");
```

**接受前置（全部满足才放行）**：

1. 单例在跑；
2. **CP 模式**（AP 直接忽略，见上）；
3. 跨机房模式，且 `operator` 非空（审计必填）；
4. 目标 `datacenterId` **等于本机房**（不能提升对侧）；
5. **连通性闸门**：探对侧机房全部父组端点，**任一可达即拒绝**——网络仍通时强行提升会打开"双主"窗口。

**放行后的执行序**（raft 单线程内）：换表（本机房权重 = `Σ(其他机房权重) + 1`，对任意机房数恒自投即成主）→ term 跃升（`promote.term.leap`，缺省 100）→ 自投登基 → 追加并复制一条 `PRIORITY_CHANGE` 日志条目 → 写父侧独立持久化文件。提升后本机房恰达法定多数门槛（Σ对侧权重+1 ≥ required），对侧合计永远差一票，双主在**权重数学上**不可能，与网络状态无关，且在三机房及以上拓扑同样成立。

#### 回退：`restoreDefaultPriorities`

```java
// 主机房恢复后，把优先级表交还配置派生值（在当前父组 Leader 上调用）
boolean ok = Speedboat.restoreDefaultPriorities("alice", "hangzhou 机房已恢复");
```

回退以配置派生表为新快照、epoch 递增、来源 `CONFIG`，走同一条复制 + 持久化 + 心跳传播路径。
回退**不跃升 term、不夺主**——只把本机房权重降回配置值。主机房凭更高权重可在下一次选举夺回，
但**不自动发生**：需主机房代表自身发起选举，或在 CP 下由 check-quorum 降级后自然触发。

#### 查看当前优先级快照

```java
DatacenterPriorityTable.Snapshot snap = Speedboat.getPrioritySnapshot();
// snap.epoch() / snap.weights() / snap.source()
```

#### 审计日志

每次提升/回退调用（无论接受/忽略/拒绝）都落一条 `WARN` 级 `PROMOTE-AUDIT` 结构化日志，
固定字段顺序便于 grep：`op / operator / node / dc / target / reason / detail /
termBefore / termAfter / epochBefore / epochAfter / weightsBefore / weightsAfter`。

`op` 取值：`ACCEPTED`（已生效）、`EXEC-FAILED`（raft 线程内执行失败）、
`REJECTED`（前置校验/连通性闸门拒绝，`detail` 给出具体原因）、
`IGNORED`（AP 模式下人工切主被忽略，`detail=manual-switch-ignored-in-ap-mode`）。
`IGNORED` 表示"模式不适用"，**不是故障**，不应触发告警重试。

#### 持久化与重启

优先级表写入**独立文件** `parent-priority.properties`（位于父组持久化目录），与 `RaftStore` 的
mmap term 档彻底分离。父组启动时若该文件存在，以其为初值覆盖配置派生表——**重启不丢人工提升状态**。

#### 风险警示（务必阅读）

> ⚠️ **切勿把提升/回退自动化**。这条通路的前提是"运维已用带外手段确认主机房确已终止或确已分区"。
> 连通性闸门只是最后一道机械防线，它只探"对侧父组端点是否可达"，无法区分"主机房真死"与
> "仅探测链路抖动"。若在主机房仅是**临时抖动**时贸然提升，仍可能在窗口内出现短暂双主。
>
> ⚠️ **回退同样需谨慎**。回退后若主机房尚未真正恢复，系统会回到"备机房无主"状态。
> 回退应在确认主机房已可参与选举后再执行。
>
> ⚠️ **提升只解决"唯一主"，不解决数据**。它让备机房成主以恢复写入语义，但主机房历史数据
> 仍需按 Raft 日志复制机制追平；不会因提升而跳过日志复制或状态机重放。

### 容器化部署

#### Docker 示例

```dockerfile
FROM openjdk:8-jdk-alpine
COPY target/your-app.jar app.jar
COPY config.properties config.properties
ENTRYPOINT ["java", "-jar", "app.jar"]
```

#### Kubernetes 示例

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: speedboat
spec:
  serviceName: speedboat
  replicas: 3
  template:
    spec:
      containers:
      - name: speedboat
        image: your-image
        volumeMounts:
        - name: config
          mountPath: /app/config.properties
          subPath: config.properties
      volumes:
      - name: config
        configMap:
          name: speedboat-config
```

---

## 进阶用法

### 自定义配置源

实现 `SpeedboatConfigProvider` 接口支持 JSON/YAML/数据库等配置源。

```java
public class JsonConfigProvider implements SpeedboatConfigProvider {
    
    private final JsonObject config;
    
    public JsonConfigProvider(String jsonFile) {
        this.config = loadJson(jsonFile);
    }
    
    @Override
    public String getDatacenter() {
        return config.getString("datacenter");
    }
    
    @Override
    public List<List<String>> getNodes() {
        return parseNodes(config.getJsonArray("nodes"));
    }
    
    @Override
    public int getIntraDatacenterElectionTimeoutMin() {
        return config.getInt("election.intra.timeout.min", 1000);
    }
    
    @Override
    public int getIntraDatacenterElectionTimeoutMax() {
        return config.getInt("election.intra.timeout.max", 2000);
    }
    
    @Override
    public int getCrossDatacenterElectionTimeoutMin() {
        return config.getInt("election.cross.timeout.min", 3000);
    }
    
    @Override
    public int getCrossDatacenterElectionTimeoutMax() {
        return config.getInt("election.cross.timeout.max", 5000);
    }
}
```

### 监听主节点变化

```java
public class LeaderChangeListener {
    
    private String currentLeader;
    
    public void checkLeader() {
        String leader = Speedboat.getLeaderId();
        if (!Objects.equals(leader, currentLeader)) {
            onLeaderChange(currentLeader, leader);
            currentLeader = leader;
        }
    }
    
    private void onLeaderChange(String oldLeader, String newLeader) {
        System.out.println("Leader changed: " + oldLeader + " -> " + newLeader);
    }
}
```

### 集成 Spring Boot

```java
@Component
public class SpeedboatLifecycle implements ApplicationRunner, DisposableBean {
    
    @Value("${speedboat.config}")
    private String configFile;
    
    @Override
    public void run(ApplicationArguments args) {
        SpeedboatConfigProvider config = new PropertiesConfigProvider(configFile);
        Speedboat.start(config);
    }
    
    @Override
    public void destroy() {
        Speedboat.stop();
    }
}
```

---

## 故障排查

### 常见问题

#### 1. 选举超时

**现象**：长时间无主节点。

**原因**：
- 网络分区
- 选举超时配置过短
- 节点数量不足（偶数节点）

**解决**：
- 检查网络连通性
- 调大 `election.intra.timeout`
- 使用奇数节点（推荐 3/5/7）

#### 2. 节点无法启动

**现象**：启动报错 `Local IP not found in configured nodes`。

**原因**：本机 IP 不在配置的节点列表中。

**解决**：
- 检查本机 IP：`hostname -I` 或 `ifconfig`
- 确保配置文件中包含本机 IP

#### 3. 跨机房选举失败

**现象**：机房内选举成功，但跨机房选举失败。

**原因**：
- 机房间网络不可达
- `election.cross.timeout` 配置过短

**解决**：
- 检查机房网络延迟
- 调大 `election.cross.timeout`（建议 3000-5000ms）

### 日志级别

调整日志级别查看详细信息：

```xml
<logger name="cn.itcraft.speedboat" level="DEBUG"/>
```

### 健康检查接口

```java
@RestController
public class HealthController {
    
    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> health = new HashMap<>();
        health.put("running", Speedboat.isRunning());
        health.put("isMain", Speedboat.isMain());
        health.put("leaderId", Speedboat.getLeaderId());
        health.put("term", Speedboat.getTerm());
        health.put("nodeId", Speedboat.getNodeId());
        health.put("datacenter", Speedboat.getDatacenterId());
        return health;
    }
}
```

---

## 附录

### nodeId 生成规则

```
{hostname}-{ip}-{random4位}
```

示例：`server01-192.168.10.1-0012`

### 选举超时建议值

| 场景 | intra 超时 | cross 超时 |
|-----|-----------|-----------|
| 本地测试 | 500-1000ms | 1000-2000ms |
| 单机房生产 | 1000-2000ms | - |
| 跨机房生产 | 1000-2000ms | 3000-5000ms |

### 推荐节点数量

- **开发/测试**：1 节点（无容错）
- **生产环境**：3/5/7 节点（奇数）
- **跨机房**：每个机房 3 节点，至少 2 个机房
