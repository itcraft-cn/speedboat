# 一进程多组（多名目主从）设计文档

- 编号：2026-09-18-01
- 状态：设计评审中
- 关联：README「设计约束：单一集群」、MEMORY.md 2026-09-18 条目、memrec e52efa98
- 决策人已拍板：上策 = 一进程多组；每名目一个 Raft 组

---

## 1. 背景与需求

### 1.1 场景

报价系统，三台服务器 A/B/C：

- A 报外汇，需要 A 是"外汇名目"的主；
- B 报贵金属，需要 B 是"贵金属名目"的主；
- C 报商品，需要 C 是"商品名目"的主。

三个名目相互独立，各自需要"恰好一个主"的互斥硬约束。业务接管（备机如何补数据、如何切换发布）不在框架职责内；框架只负责**每个名目上的主（锁）唯一性**与**主（锁）的迁移语义**。

### 1.2 与现有约束的关系

README 已明确"一进程 = 一个集群拓扑"，并指出多集群场景需"自行扩展配置层与传输层（协议消息需增加 groupId 多路复用）"。本设计即对该扩展路径的正式化：**把门面从单例改为多实例，每个名目对应一个独立的 Raft 组**。

### 1.3 硬约束

1. **互斥**：同一时刻，同一 groupId 内恰好零个或一个 Leader；绝不出现两个（Raft 每 term 至多一主，天然保证；框架需防御的是配置错误导致的"组间串扰"）。
2. **隔离**：任一组的选举/故障/迁移不得影响其他组的主从关系与可用性。
3. **兼容**：现有单组用法（`Speedboat.start(config)` 静态门面）行为与语义完全不变。

---

## 2. 语义定义

| 术语 | 定义 |
|------|------|
| 名目（subject） | 业务侧概念：外汇/贵金属/商品。框架内对应一个 **groupId** |
| 组（group） | 一个独立的 Raft 集群拓扑：独立的成员列表、独立的 term/日志/Leader、独立的端口 |
| 组句柄（handle） | 某进程内某组的运行态对象，持有该组的 Transport/RaftNode/锁状态机 |
| 主（main） | 某组内当前 Leader。`handle.isMain()` 即该名目下的主判定 |
| 锁迁移 | 某组内 Leader 从节点 X 迁移到节点 Y（故障触发或主动让位）。term 单调递增，fencing token = 该组 term |

**关键澄清**：本方案中"多名目"不是一把锁的多个名字，而是**多个独立的共识组**。每个名目的互斥由各自组的 Raft 选举直接保证（Leader 即主，无需再叠加锁语义）；`DistributedLock` 仍可在每组内部按原有语义使用（leader 持锁），与本设计正交。

---

## 3. 总体架构

### 3.1 分层变化

```
现状：
  Speedboat(静态单例) ──> NettyTransport ──> RaftNode(1个)

目标：
  Speedboat(静态门面，兼容层)
      ├── default 组 ──> SpeedboatHandle ──> NettyTransport(端口P0) ──> RaftNode
      └── startNamed(groupId, config) ──> SpeedboatHandle ──> NettyTransport(端口Pn) ──> RaftNode
```

- 新增 `SpeedboatHandle`：把现在 `Speedboat` 的实例逻辑（身份匹配、传输构建、RaftNode 组装、锁缓存、生命周期）原样抽取为一个可多实例化的类。**纯重构，行为不变**。
- `Speedboat` 静态门面退化为 default 组的委托者 + 命名组注册表。
- 每组一个 `NettyTransport`、一个 `RaftNode`、一个 `RaftNodeExecutor`、一个 `RaftStore`、一个 `LockStateMachine`——**组间零共享可写状态**。

### 3.2 传输层选型：每组独立端口（一期），groupId 协议复用（二期演进）

| 维度 | A. 每组独立端口 | B. 单端口 + 协议加 groupId |
|------|----------------|--------------------------|
| 协议/序列化改动 | 无 | 5 个 RPC 消息各加字段，需全集群同步升级 |
| 实现风险 | 低（NettyTransport 原样复用） | 中（handler 路由表、序列化兼容） |
| 故障域 | 端口级隔离 | 单端口，handler bug 全组连坐 |
| 端口/配置成本 | 每节点 N 个端口、N 套 nodes | 1 端口、1 套 nodes |
| 名目规模适应性 | ≤ 8 组舒适，两位数变痛 | 无上限 |

**决策**：一期采用 A（最快落地、协议零风险）；B 作为二期演进项，触发条件为"单节点组数 > 8 或端口管理成本反转"。此决策与"极简"原则一致：先用最少的改动满足真实场景（当前 3 名目），不为假想的规模预付协议复杂度。

### 3.3 线程与资源预算（每组）

- raft 单消费者执行器：1 线程
- 选举超时/心跳/check-quorum 定时任务：挂在执行器调度上（不新增线程）
- NettyTransport：boss 1 + worker N（一期每组独立创建；worker 共享优化后置为可选）
- 锁续期线程：仅在实际获取锁后创建（现状行为）
- 3 组 ≈ 12~15 线程，可接受；文档给出"单进程建议 ≤ 8 组"的软上限

---

## 4. 配置层设计

### 4.1 多组配置格式（新增，向后兼容）

```properties
# 组列表（存在此键即进入多组模式）
groups=forex,metals,commodity

# 每组独立拓扑（键前缀 group.{groupId}.）
group.forex.nodes.0.0=192.168.10.1:3001
group.forex.nodes.0.1=192.168.10.2:3001
group.forex.nodes.0.2=192.168.10.3:3001
group.forex.vote.weight.strategy=prefer
group.forex.vote.weight.prefer=<A的nodeId>

group.metals.nodes.0.0=192.168.10.1:3002
...
group.metals.vote.weight.prefer=<B的nodeId>

group.commodity.nodes.0.0=192.168.10.1:3003
...
group.commodity.vote.weight.prefer=<C的nodeId>

# 每组可选独立参数（缺省回退全局值）
group.forex.election.intra.timeout.min=1000
```

- 不配置 `groups` 键：单组模式，走现有 `nodes.*` 格式，**零变化**。
- `vote.weight.prefer` 是把 A/B/C 各自钉到各自名目的关键——沿用现有 `PreferNodeVoteWeightStrategy`，且每组一份权重配置（权重表是组级拓扑事实，组内全网一致即可）。
- 端口约定：同节点各组的端口必须互不相同；冲突时 bind fail-fast（Netty 现有行为，补充启动期预检并给出可读错误）。

### 4.2 配置接口扩展

`SpeedboatConfigProvider` 新增 default 方法：

- `default List<String> getGroupIds()` —— 返回 null/空即单组模式
- `default SpeedboatConfigProvider getGroupConfig(String groupId)` —— 返回该组的子配置视图（实现类负责把 `group.{id}.*` 键空间投影为既有键空间，`PropertiesConfigProvider` 提供实现）

投影复用意味着：**组级配置解析逻辑与单组完全一致**，不产生第二套解析代码路径。

### 4.3 跨机房限制（一期）

多组一期仅支持单机房扁平拓扑（每组 `nodes.size() == 1`）。跨机房级联 × 多组（`group.{id}.nodes.{dc}.{node}` 双层键）列入二期，理由：级联模式本身的身份匹配/超时叠加逻辑与多组相乘后测试面过大，且当前报价场景无跨机房诉求。

---

## 5. 门面 API 设计

### 5.1 新增 API（实例化句柄）

```java
// 启动命名组（幂等：同 groupId 二次启动被忽略并告警）
SpeedboatHandle forex = Speedboat.startNamed("forex", config);
SpeedboatHandle metals = Speedboat.startNamed("metals", config);

// 组级查询（与现有静态 API 同名同义）
forex.isMain();          // 本进程在该名目下是否为主
forex.getLeaderId();     // 该组当前 Leader
forex.getTerm();         // 该组当前 term（fencing token）
forex.getNodeId();
forex.getLock(name);     // 该组内的分布式锁（原有语义）
forex.report();          // 该组 RaftNodeReport（Phase E 通道）

// 组级主变更监听（锁迁移的对外可见点，见 §6）
forex.addMainChangeListener(listener);

// 生命周期
Speedboat.stopNamed("forex");
Speedboat.stopAll();     // 含 default 组
```

### 5.2 兼容 API

`Speedboat.start(config)` / `isMain()` / `getTerm()` / `getLock()` 等全部静态方法保留，内部委托 default 组句柄。**语义不变：default 组 = 现有单组模式**。

### 5.3 防错规则

- groupId 命名约束：`[a-z0-9-]{1,32}`，用作持久化目录名与线程名前缀，启动期校验。
- 同一进程内 groupId 重复启动：忽略并告警（与现有单例幂等行为一致）。
- 本机 IP 在某组拓扑中匹配不到：仅该组启动失败（fail-fast 抛出），**不影响其他组**；多组启动采用"全或无可配置"策略，缺省全或无（避免半启动的静默降级）。
- 各节点同名目的 groupId/成员拓扑必须一致：启动期对成员列表做规范化比对日志告警（成员视图不一致是双主根源，与权重全网一致性约束同理，文档化）。

---

## 6. 锁（主）迁移语义

这是需求方唯一要求框架讲清楚的部分。

### 6.1 迁移的触发与过程

迁移 = 某组内 Leader 变更，过程完全由既有 Raft 机制承担，框架不新增协议：

1. 老 Leader 失联（宕机/网络分区）或被 check-quorum 降级（Phase C 已实现：多数派心跳超时主动 stepdown）；
2. 组内剩余节点 PreVote/RequestVote 选出新 Leader（Phase C：PreVote + leader stickiness 防扰乱）；
3. 新 Leader term = 老 term + 1，**fencing token 单调递增**；
4. 每组独立的选举超时（单机房 1000-2000ms 缺省）→ 名目主迁移时延即该组选主时延，组间互不拖累。

### 6.2 对业务的可见性（迁移通知）

新增组级监听接口 `MainChangeListener`（每句柄可注册多个）：

- `onBecomeMain(groupId, term)`：本进程在该组登主——业务在此切换"发布权开";
- `onLoseMain(groupId, term)`：本进程在该组失主（降级/停机前）——业务在此关发布权。

投递保证与注意点（写入 MANUAL）：

- 回调在该组 raft 线程外的单一通知线程串行执行；listener 异常捕获告警，不反压算法线程（沿用 Phase E listener 自愈思路）；
- `onBecomeMain` 到达时业务必须以 `getTerm()` 作为发布信封的 epoch，下游按 term 单调拒旧——这是"报价唯一"三件套中 fencing 的落点，组级 term 天然分名目；
- 失主回调是尽力通知（进程崩溃时不可达），正确性不依赖它，依赖下游 term 校验。

### 6.3 组内分布式锁与迁移的关系

每组 `LockStateMachine`/锁表随该组日志独立恢复；Leader 迁移后，新 Leader 从已提交日志重建锁表，未过期的租约继续有效（现状语义，组内自洽）。组间无任何锁交互。

### 6.4 双主防御清单（互斥硬约束的落地检查点）

| 风险 | 防御 |
|------|------|
| 同组不同节点成员视图不一致 → 分裂选举 | 启动期成员列表规范化比对 + 告警；文档约束（同权重一致性约束） |
| 两组误配相同端口（同机） | bind fail-fast + 启动期预检可读报错 |
| 同节点身份在不同组重复（同 ip:port 出现在两组） | 启动期校验：同进程内 (ip,port) 全局唯一 |
| 老主分区自认主继续发布 | term fencing：下游按组 term 单调校验（Phase C check-quorum 缩小窗口） |
| 半启动（部分组失败部分成功） | 缺省全或无；显式配置可放宽 |

---

## 7. 非功能性设计

- **可用性**：组间零共享可写状态、端口级故障域隔离——一组选主风暴不影响其他组心跳。
- **性能**：每组独立 raft 线程，选举/心跳互不抢占；锁操作仍走组内日志复制，时延特性不变。
- **可观测**：`report()` 组级聚合 + 建议日志 MDC 加 `groupId` 维度（实现为 handle 内 logger 前缀，避免引入 MDC 依赖争议，二选一在实现期定）。
- **持久化**：`RaftStore` 每组独立实例，目录约定 `{base}/{groupId}/`；缺省 NopRaftStore 行为不变。
- **安全性**：无新增外部输入面；groupId 校验防目录穿越。
- **可扩展性**：二期预留——groupId 协议复用、跨机房 × 多组、EventLoopGroup 共享、组动态增删（`startNamed` 运行期调用即已支持"动态加组"，动态删组 = `stopNamed`）。

---

## 8. 实施计划（分期）

### P1 重构抽取（行为不变，纯等价重构）
- 抽取 `SpeedboatHandle`；`Speedboat` 静态门面委托 default 句柄
- 回归标准：现有全部测试绿（含 flaky 旋转用例），静态 API 行为零差异

### P2 多组骨架
- 配置层：`getGroupIds()` / `getGroupConfig(id)` 投影 + `PropertiesConfigProvider` 实现 + 启动期防错校验（§5.3、§6.4）
- 门面：`startNamed/stopNamed/getGroup/stopAll`
- 持久化分目录
- 测试：配置解析单测、双组/三组单进程集成（loopback 端口）、每组独立选主 + prefer 钉主验证

### P3 迁移语义与可观测
- `MainChangeListener` + 单通知线程投递
- 组级 report 聚合
- kill-leader 组间隔离集成测试：一组杀主，其余组 term/leader 零扰动
- 文档：README 设计约束章节改写（"一进程一集群" → "一进程多组"）、MANUAL 配置项、CHANGELOG

### P4 二期演进（按需，不在本次范围）
- groupId 协议复用（单端口）
- 跨机房级联 × 多组
- Netty worker 共享

---

## 9. 测试计划要点

1. **单元**：多组配置解析（含缺省回退、非法 groupId、端口冲突、IP 匹配失败）；handle 生命周期幂等。
2. **集成（单进程多实例）**：3 组 × 3 节点 loopback；每组独立选主唯一；prefer 权重钉主（复用 T5~T8 验证法）；任一组 kill leader 后该组 ≤ 选举超时迁主且其余组 leader/term 不变。
3. **实机（174/176/88）**：3 组 × 3 节点 × 3 端口，重复集成用例 T1/T2/T4 的组级版本。
4. **回归**：P1 后全量测试必须零差异。

---

## 10. 风险与开放问题

| 风险/问题 | 处置 |
|-----------|------|
| P1 重构触碰 400 行门面 + 1600 行 RaftNodeImpl 边界 | P1 严格只移动不改逻辑；以全量测试回归兜底 |
| 组数增长后的线程/端口膨胀 | 软上限 8 组文档化；P4 演进项兜底 |
| 跨机房 × 多组需求提前到来 | 配置键空间已预留 `group.{id}.nodes.{dc}.{node}` |
| 迁移通知与业务发布切换的时隙竞态 | 以 term fencing 为正确性锚点，通知仅为优化路径（§6.2 已声明） |
