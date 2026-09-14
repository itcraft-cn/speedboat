# MicroRaft 先进经验对标分析 — speedboat 传递方案

> 日期：2026-09-14
> 依据：MicroRaft（io.microraft，Java 8 纯库）源码深度解构 + speedboat 全模块现状盘点
> 结论性质：设计/计划文档（不承载代码，仅少量伪代码）

---

## 一、背景与定位差异（先讲清楚"能学什么、不能学什么"）

| 维度 | MicroRaft | speedboat |
|---|---|---|
| 定位 | 通用复制状态机**纯库**（算法+契约，零网络零存储依赖） | **主节点选举组件** + 分布式锁，极简 API 一行启动 |
| 持久化 | RaftStore 抽象 + SQLite 实现，分级 flush | 无（用户已决策不做持久化与认证） |
| 快照 | 分块 INSTALL_SNAPSHOT + 从 follower 并行拉取 | 无，仅 maxLogSize=10 丢头截断（有安全隐患） |
| 线程模型 | Actor 单线程（3 方法 executor 契约，零锁） | Netty worker 线程直接改状态，靠 synchronized 兜底 |
| 高级选举 | PreVote + leader stickiness + check-quorum | 无，纯加权重选 |
| 可观测性 | RaftNodeReport 双通道 + Micrometer 外挂 | 无指标暴露，心跳路径日志洪水 |

**核心取舍**：speedboat 的定位是"选举组件"而非"复制状态机库"。因此不必照搬 MicroRaft 的全部（尤其 Leader 附加日志、快照、成员变更复杂工程），而应挑选**对"选主正确性与运维性"收益最大、改动面最小**的经验。

---

## 二、MicroRaft 可传递经验清单（按 ROI 排序）

### A 级：直接决定选主正确性，强烈建议吸收

1. **Actor 单线程化（零锁状态机）** — MicroRaft：`RaftNodeExecutor`（3 方法契约）+ 单线程 ScheduledExecutor，所有消息/定时任务串行执行，内部完全无 synchronized。
   speedboat 现状：Netty worker 线程直接调 `RaftNode.handleXxx`，靠对象锁兜底，存在持锁阻塞 IO、锁粒度死的隐患。
   传递方式：新增"raft 单线程调度器"入队点——Netty handler 只做"解码 + 入队"，所有状态变更在一个 raft 线程内串行执行。
   伪代码：
   ```
   channelRead -> serializedExecutor.execute(() -> raftNode.onMessage(msg))
   定时心跳/选举超时 -> 同一 serializedExecutor 自续期调度
   ```

2. **check-quorum（法定人数心跳自检，leader 主动降级）** — MicroRaft：`quorumResponseTimestamp()`，leader 周期检查多数 follower 最近响应时间，超时主动 `toFollower`。
   speedboat 现状：网络分区后孤立 leader 只要没收到更高 term，会一直自认 leader，加锁业务持续"看似成功"。
   这是**选举组件最该有而缺的特性**，收益/成本比最高。

3. **PreVote 预投票 + leader stickiness** — 不递增 term 先握手；被分区节点重连不会用暴涨的 term 打十四下任跳动 leader。
   speedboat 现状：分区恢复瞬间落后的 follower 会 term+1 强制切换，造成"分区恢复=主节点震荡"。

4. **不可变 TermState "先持久化后可见"模式** — 即便 speedboat 不做磁盘持久化，该模式也可用于**内存快照式的原子替换**（volatile 引用替换整对象，读侧无锁）；一旦将来补持久化，契约已就位。

### B 级：显著提升运维性与鲁棒性

5. **RaftNodeReport 双通道可观测性** — 事件触发 + 周期快照发布角色/term/成员/follower matchIndex；metrics 通过 `ReportListener` 外挂（Micrometer 模块核心零依赖）。speedboat 需要的正是这套"不绑定 metrics 库"的钩子。

6. **Transport 契约化（send 非阻塞、不抛异常、丢包归一化为超时重试）** — speedboat 的 NettyTransport 部分满足但无契约文档；更重要的教训是 **消息模型与序列化解耦**：MicroRaft 消息只是接口，序列化由传输层决定。speedboat 的 CustomSerializer 已有雏形，可借鉴其"约束性契约"写法。

7. **结构化异常分类** — `NotLeaderException`（携带候选 leader 提示）/`CannotReplicateException`/`IndeterminateStateException`。speedboat 的锁 API 报"false/异常字符串"，调用方无法程序化重试。至少先落地 `NotLeaderException`。

8. ** улучш的回退提示** — MicroRaft `AppendEntriesFailureResponse.expectedNextIndex` 一条消息直接跳回正确 index；speedboat 现状按响应逐条退。日志复制场景可重用。（评级中，依赖 append 路径体量）

### C 级：记录在案，随定位演进再说

9. 快照分块 + 从 follower 并行拉取（只有日志体量上来才有意义，speedboat 日志用后即弃）
10. LEARNER 角色 + voting 标志持久化（选主组件暂无成员灰度需求）
11. leader FlushTask 并行 fsync / flushedLogIndex 参与 quorum（无持久化，不适用）
12. Firewall 故障注入测试框架 + LocalRaftGroup（测试基建，值得抄形态）

### 不可学 / 需避免的点（负面经验）

- MicroRaft **无 JMH 基准、无内置传输实现**——别误当成有。
- 其 RaftNodeImpl 本身 1900 行"上帝类"（门面+分发混在一个类）——speedboat 若照抄分层，应把 handler 拆出而不是再养一个上帝类。
- speedboat 现存 `maxLogSize=10` 丢头截断在引入"日志复制依赖项"（锁）后已是**正确性风险**：nextIndex 回退永远失败。若不引入快照，至少要把截断改为"仅丢弃已 apply 且无回退风险的条目"或干脆只保留策略而非物理删除（配合 C-9）。

---

## 三、上 / 中 / 下三策

### 下策（成本 0.5~1 天，纯文档+异常+两个小修）

只吸收零架构改动的经验：
- 落地 `NotLeaderException`（带建议 leader 字段），锁 API 程序化可重试；
- 修复 `maxLogSize` 截断的正确性风险（改为保留 learned/committed 头部，仅做提示式收缩或禁物理删除）；
- 在 Transport/StateMachine 接口上补 MicroRaft 式契约 Javadoc（非阻塞/幂等/先持久化后可见），为将来演进立规矩。

风险：最小。收益：修掉 1 个正确性隐患 + 错误协议化。
适用：只想"体检式吸收"，近期不动架构。

### 中策（成本 3~5 天，推荐 ⭐）

= 下策 + 两大主线：

1. **Actor 单线程化改造**（A-1）
   - 引入 `RaftDispatcher`（单线程 ScheduledExecutor，命名 `speedboat-raft-N`）；
   - Netty 入站 handler 改为入队；定时任务（心跳/选举超时/成员健康检测）全部迁入同一执行器自续期；
   - 逐步拆除 RaftNode 上 synchronized（由"消息串行"天然保证 happens-before）；
   - 验收：现有 496 项测试全绿 + 新增"并发消息乱序注入"测试。
2. **check-quorum + PreVote/stickiness**（A-2/A-3）
   - leader 维护多数 follower 最近响应时间，超时主动降级；
   - 选举前发 PreVote（不涨 term），stickiness 规则拒绝合法 leader 心跳存续时的投票；
   - 验收：新增分区/恢复 IT：断 leader 网络→新 leader 当选→老 leader 恢复后必须降级且不得干扰（term 不震荡）。

配套：RaftNodeReport 最小版（角色/term/leader/follower 响应时间）+ 每对消息落类的日志降噪（心跳路径 INFO→DEBUG）。

风险：中。触碰 RaftNode 主干，但不动序列化与外部 API。
收益：选举正确性三个硬伤（分区脑裂窗口、恢复期震荡、单线程化）一次到位。

### 上策（成本 10~15 天）

= 中策 + 结构性分层：

1. **API/Impl 分层重构**：`RaftNode` 提为纯接口 + `RaftNodeImpl`，拆出 `handler/`（VoteRequestHandler、AppendEntriesHandler 系列）与 `task/`（HeartbeatTask、ElectionTimeoutTask 自续期），消灭现在 1253 行上帝类；
2. **RaftStore 抽象前置**：即便不实现磁盘 store，也先把 term/votedFor 的存取钩子接口化（NopRaftStore 空实现兜底），为"P0 地基：持久化"铺路——将来启用时零接口变动；
3. **Transport 契约强约束**：send 非阻塞/不抛异常写死为接口 Javadoc 契约，maxFrame 上限、每 peer 背压队列补齐（P1-5）；
4. **Report→Micrometer 外挂模块**（可选独立 artifact）。

风险：中偏大，一次动 IO/主干/分层三层，需分 PR 推进。
收益：完成 lab-grade → "带契约的工程组件"的形态跃迁，持久化/迁移成本降到最低。

---

## 四、推荐决策

**推荐中策**。理由：
- 上策的分层收益大但可在他日现有检验后再做（MicroRaft 的分层我们自己已能复述，随时可复制）；
- 下策只消一半风险（单线程化留下的 synchronized 锁与 IO 线程耦合是并发事故温床）；
- 中策三件事——单线程化、check-quorum、PreVote/stickiness——正是用户前期"持久化不做、认证不做"决策后，**在纯内存前提下把选主正确性提升到最好**的组合，且与"极简"定位兼容（不引外部依赖、不增 RPC 复杂度）。

---

## 五、MicroRaft 经验速查表（备档）

| # | 特性 | MicroRaft 证据 | 借鉴评级 |
|---|---|---|---|
| 1 | `CompletableFuture<Ordered<T>>` + OrderedFuture 封闭写入 | Ordered.java / OrderedFuture.java | 高 |
| 2 | Actor 单线程 + 3 方法 RaftNodeExecutor 契约 | executor/RaftNodeExecutor.java | 高 |
| 3 | [PERSISTENT] 注释 + 不可变 RaftTermState 先持久化后可见 | impl/state/RaftState.java | 高 |
| 4 | Transport 2 方法非阻塞/不抛异常/幂等契约 | transport/Transport.java | 高 |
| 5 | RaftStore 分级 flush（term 原子、log 异步） | persistence/RaftStore.java | 高 |
| 6 | Leader FlushTask 并行化 + flushedLogIndex quorum | RaftNodeImpl.java:1392-1585 | 高 |
| 7 | PreVote / sticky / TriggerLeaderElection 加速收敛 | handler/PreVote*Handler | 高 |
| 8 | LEARNER + voting 标志持久化 | RaftState.java:511-529 | 中 |
| 9 | check-quorum（quorumResponseTimestamp 主动降级） | RaftNodeImpl.java:1830-1845 | 高 |
| 10 | 快照分块 + 从 follower 并行拉取 | InstallSnapshotRequestHandler.java:64-76 | 高 |
| 11 | 智能截断：按最小 matchIndex 保留 trailing log | RaftNodeImpl.java:1063-1090 | 中 |
| 12 | 指数退避流控 + flowControlSequenceNumber 防陈旧 ACK | impl/state/FollowerState.java:111-115 | 高 |
| 13 | FalutWall 故障注入 / InMemoryRaftStore 测试形态 | impl/local/ | 高 |
