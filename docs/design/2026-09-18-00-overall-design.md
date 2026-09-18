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
   ├── persistence/    NopRaftStore / InMemoryRaftStore(默认 mem) / MmapRaftStore
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
| `raft.persistence` | **mem** | none / mem / mmap 三档 |
| `raft.persistence.dir` | speedboat-data | mmap 目录（{nodeId} 自动分子目录） |
| `raft.mmap.size.mb` | 128 | mmap 单文件上限 |
| `raft.mmap.file.name` | raft.mmap | 文件名占比 {nodeId} |
| `raft.max.log.size` | 4096 | 内存日志上限（defect-20260918-01 校正） |

样例：`config.properties`（单机房全量注释化）、`config-named-lock.properties`（报价三名目场景）。

## 6. 线程模型与故障域

- **raft 单消费者线程**：全部状态变更（角色/term/日志/成员/commitIndex/锁表 apply/检查点）收口在该线程；外部同步调用经 `onRaftThread / onRaftThreadAsync` 收敛——**禁止在 raft 线程上调用阻塞锁 API**（DistributedLock 的等待发生在调用方线程）；
- **Netty IO 线程**：收发帧、异步 dispatch；handler 注册面向 `TransportLayer` 契约（mock 可替换）；
- **锁续期线程**（每锁实例一把，leaseMs/2 周期）、命名锁转发复用 Netty IO——无独立线程池扩散；
- **故障域**：端口级隔离（bind 失败 fail-fast）；持久化档位切换不影响算法层；锁转发失败为**客户端可重试失败**，不进入共识裁决。

## 7. 非功能性承诺

- **可用性**：多数派存活即收敛；Leader 迁移 ≤ 同机房选举超时（实机 T2 ≤6s，同机房目标 150-300ms 基线）；
- **正确性**：同名目锁互斥由日志全序保证（0 双持；"失主窗口"只允许 **零持有**，不出现双持）；
- **性能**：锁操作 Leader 本地 <10ms / follower 转发 RTT 级；mmap 写 = 内存级消耗；心跳 50ms 折叠为空 AppendEntries；
- **可观测**：重启/挂回、检查点、锁授予/拒绝/RENEW 被拒均有语义化 INFO/WARN 日志锚点（`[PASS]`/`[FAIL]`、`restored term`、`rebuilt log`、`snapshot written`）；
- **兼容**：`Speedboat.start(config)` 单组语义零变化；`raft.persistence` 缺省 mem 不扰既有部署。

## 8. 关键历史决策索引

| 决策 | 结论 | 记录 |
|------|------|------|
| 多名目场景路线 | 撤销"一进程多组"，落 单组+命名锁 | ad7969e / 01 分项 §1 |
| 锁与 Leader 关系 | 正交；Leader 只做全序仲裁 | 01 分项 §2 |
| 抢占策略 | 非抢占公平竞争（无 REVOKE） | 01 分项 §6 |
| 重启副本 epoch 分叉 | defect-20260918-01 → 三档持久化根治关闭 | 02 分项 §7 |
| 快照范围 | 仅"自身重启恢复"语义；INSTALL_SNAPSHOT 不做（期末） | 02 分项 §6 |
