# Speedboat 总体设计（Overall Design）

- 编号：2026-09-18-00
- 状态：已实现（与代码同步；实现提交链 6ef0303 → 10ae50c）
- 分项设计索引：
  - [01 分布式命名锁详细设计](2026-09-18-01-named-lock-design.md)
  - [02 持久化三档（RaftStore）详细设计](2026-09-18-02-persistence-three-tier-design.md)
- 关联：docs/plans/2026-09-18-02-named-lock-plan.md（计划稿）、docs/analysis/defect-20260918-01（已关闭缺陷）

---

## 1. 定位与设计原则

Speedboat 是极简 Raft 主节点选举组件（JDK 8 / Netty 4.1 / Protostuff），提供：

1. **主节点选举**（Raft 共识核心，单机房扁平 / 跨机房级联自动模式）；
2. **分布式命名锁**（多名目互斥原语，任意成员可持有）；
3. **持久化三档**（Nop / Mem / Mmap，覆盖测试到极稳定环境）。

设计铁律（贯穿各分项）：

- **极简 API**：`Speedboat.start(config)` 一行启动；锁使用 `Speedboat.getLock(name)` 一行获取；
- **确定性一致**：凡跨节点可观测的判定（锁持有、epoch、term），只能产生于日志全序 apply；
- **时钟安全**：进程内超时一律 `System.nanoTime()`；跨节点 lease 用 Leader 打点墙钟传播（全网一致到期点）；
- **同机一端口一身份**：nodeId = `node-<ip>-<port>`，端口即身份，同机同端口不可双起；
- **组合优于继承**：策略接口扩展（vote-weight / group / membership），核心类不拆分。

## 2. 分层结构

```
Speedboat(静态门面, default 单组兼容层)
   │  PropertiesConfigProvider / 自定义 SpeedboatConfigProvider
   ├── NettyTransport  ── RPC: RequestVote / PreVote / Heartbeat / AppendEntries
   │                       + LockOp（命名锁转发，消息类型 9/10）
   ├── RaftNode(Impl)  ── Actor 单消费者执行器；选举/心跳/check-quorum/成员变更
   │       ├── Term/日志（内存 + RaftStore 持久化三档）
   │       ├── StateMachine(LockStateMachine)：锁表裁决 + 检查点
   │       └── RaftNodeReport（Phase E 可观测性快照/监听）
   ├── lock/           DistributedLock + DistributedLockImpl（转发/本地双路合一）
   ├── persistence/    NopRaftStore(Builder 缺省) / InMemoryRaftStore(显式) / MmapRaftStore(生产缺省)
   ├── strategy/       vote-weight（prefer/even）· group · membership（健康检查/变更验证/注册）
   └── config/         SpeedboatConfigProvider + PropertiesConfigProvider（全部键见 §5）
```

## 3. 共识核心（现状要点）

- **角色与选举**：Follower/Candidate/Leader 单线程 Actor（MP+MPSC+SC 契约，`RaftNodeExecutor`）；选举超时随机化 + PreVote 探测 + leader stickiness + check-quorum 主动降级（Phase C 三件套）；
- **日志**：内存 CopyOnWriteArrayList + O(1) 索引 map；`maxLogSize` 上限（缺省 4096，见 02 分项"读回链"一节）；冲突由 AppendEntries 全量重建收口；
- **持久化挂钩点**：term/votedFor/leaderId 经 `persistAndFlushTerm` 原子落盘；日志经 `persistLogEntries`（"调用方全量列表"语义）；重启读 `restoreTerm()` / `restoredLogEntries()`；
- **成员变更**：默认静态成员（自动剔除默认关闭——实机教训：开启会在网络抖动时误剔除致孤岛选主）；
- **可观测**：`RaftNodeReport` 事件 + 周期双通道 listener 外挂（不绑 metrics 库）。

## 4. 主与锁的正交关系（顶层语义）

| 维度 | 主节点（Leader） | 分布式命名锁 |
|------|------------------|--------------|
| 本质 | 共识角色，每组至多一个 | 资源互斥原语，一名目一把 |
| 产生 | 选举 | 任意成员申请，日志共识成功即持有 |
| 生命周期 | 选举/降级 | 申请→持有→续期→释放→失联到期 |
| 与名目 | 无关（只负责命令全序化） | 锁名即名词空间，N 把并存 |

报价场景（A 持外汇锁 / B 持贵金属锁 / C 持商品锁）= **一个 Raft 组**承载，
不需要多进程/多端口。详细语义见 01 分项。

## 5. 配置全景（键 → 默认值 → 语义）

| 键 | 默认 | 说明 |
|----|------|------|
| `datacenter` | null/dc-{i} | 机房 ID，跨机房场景必填 |
| `nodes.{dc}.{i}` | 必填 | 拓扑，ip:port |
| `election.intra.timeout.min/max` | 1000/2000 | 机房内选举超时 |
| `election.cross.timeout.min/max` | 3000/5000 | 跨机房选举超时 |
| `vote.weight.strategy` | none | prefer / even / none；**全网一致约束** |
| `vote.weight.prefer` / `.prefer.weight` | -/3 | prefer 钉主键值 |
| `membership.auto.removal` | false | 自动剔除（不建议开启） |
| `raft.persistence` | **mmap** | none / mem / mmap 三档（mem=显式无盘档；mmap 生产缺省） |
| `raft.persistence.dir` | speedboat-data | mmap 目录（{nodeId} 自动分子目录） |
| `raft.mmap.size.mb` | 128 | mmap 单文件上限 |
| `raft.mmap.file.name` | raft.mmap | 文件名占比 {nodeId} |
| `raft.max.log.size` | 4096 | 内存日志上限（双安全裁剪路径，见 02 分项 §6） |
| `raft.checkpoint.interval` | 1024 | 状态机检查点触发阈值（applied 增量，仅 Mmap 档生效） |
| `lock.lease.ms` | 30000 | 分布式锁默认租约时长（越短迁移越快、续期开销越高） |

### 5.1 锁内部常量（当前未配置化，改需评估）

| 常量 | 值 | 语义 |
|------|-----|------|
| 转发等待上限 | 6000ms | 略大于传输层 5s 兜底；**阻塞在调用方线程**，非 Netty IO |
| 重试间隔 | 100ms（±50% jitter） | 申请失败后退避；jitter 防多竞争者同拍重试（惊群） |
| 判定结果 TTL | 60s | 结果表惰性清理窗口；调用方实际等待仅 1s，远小于 TTL |
| 结果表软容量 | 4096 | 超限才触发按时间清理 |
| 续期间隔 | leaseMs/2 | 每个锁实例一个单线程调度器（锁名数量级=业务名目数） |

样例：`config.properties`（单机房全量注释化）、`config-named-lock.properties`（报价三名目场景）。

## 6. 线程模型与故障域

- **raft 单消费者线程**：全部状态变更（角色/term/日志/成员/commitIndex/锁表 apply/检查点/日志裁剪）收口在该线程；外部同步调用经 `onRaftThread / onRaftThreadAsync` 收敛——**禁止在 raft 线程上调用阻塞锁 API**（DistributedLock 的等待发生在调用方线程）；
- **Netty IO 线程**：只负责收发帧与异步 dispatch；**转发等待（上限 6s）发生在调用方线程**（business / 续期线程），不占用 IO 线程（此前文档措辞易误解，已更正）；
- **锁续期线程**：每个 `DistributedLockImpl` 实例一个单线程调度器（按锁名缓存，量级=业务名目数）；锁名数量级很大时建议改共享调度器（演进项）；
- **持久化 IO**：term 写、日志 append 均在 raft 线程；Mmap `force()` 只在 term / 检查点 / 停机三个精确点发生，压实回收亦在 raft 线程（容量压力触发）。

## 7. 非功能性承诺

- **可用性**：多数派存活即收敛；Leader 迁移 ≤ 同机房选举超时（实机 T2 ≤6s，同机房目标 150-300ms 基线）；
- **正确性**：同名目锁互斥由日志全序保证（0 双持；"失主窗口"只允许 **零持有**，不出现双持）；
- **性能**：锁操作 Leader 本地 <10ms / follower 转发 RTT 级；mmap 写 = 内存级消耗；心跳 50ms 折叠为空 AppendEntries；
- **可观测**：重启/挂回、检查点、压实回收、锁授予/拒绝/RENEW 被拒均有语义化 INFO/WARN 日志锚点（`[PASS]`/`[FAIL]`、`restored term`、`rebuilt log`、`snapshot written`、`compacted`）；
- **兼容**：`Speedboat.start(config)` 单组语义零变化；`raft.persistence` 缺省 mmap——写代价与 mem 同量级而跨进程重启安全性严格占优（P4-M5 收敛）；
- **持久化 durability 边界（精确口径）**：
  - **term**：每次写槽后 `force()`（msync）——选主安全性零丢失；
  - **日志**：append 入 page cache；`force()` 点 = 检查点 / 停机 / 压实。**未 force 的尾部断电可退**——此处的"未提交"指 *尚未进入本轮多数派 AppendEntries 确认前缀* 的尾部；已提交条目若尚未 force 且全体同时断电，属于本档的 durability 极限（要 power-loss 级保证需每次提交后 force 或接 WAL fsync 策略，列为演进项）；
  - **锁语义下的后果**：极端场景可能"丢已提交锁授予"→ 表现为**零持有者**（安全方向：不双持）；重启后由检查点 + WAL 重放 + Leader 重同步收敛；
- **Leader 时钟（跨机房监控项）**：`grantTimestampMs` 由 Leader 墙钟打点并随日志传播，全网到期点一致；代价是单机部署下租约时长跟随 Leader 时钟，跨机房部署应将 Leader 时钟漂移纳入监控（NTP 偏差会整体平移租约到期）。

## 8. 关键历史决策索引

| 决策 | 结论 | 记录 |
|------|------|------|
| 多名目场景路线 | 撤销"一进程多组"，落 单组+命名锁 | ad7969e / 01 分项 §1 |
| 锁与 Leader 关系 | 正交；Leader 只做全序仲裁 | 01 分项 §2 |
| 抢占策略 | 非抢占公平竞争（无 REVOKE） | 01 分项 §6 |
| 重启副本 epoch 分叉 | defect-20260918-01 → 三档持久化根治关闭 | 02 分项 §7 |
| 快照范围 | 仅"自身重启恢复"语义；INSTALL_SNAPSHOT 不做（期末） | 02 分项 §6 |
