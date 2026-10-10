# 代码审查报告：speedboat（全量走查 src/main/java）

## 项目概述

- **审查时间**：2026-10-10
- **项目语言**：Java 8（`<release>8</release>`）
- **审查范围**：`src/main/java` 全部 **121 个文件 / 15871 行**（不含 `src/test`）
- **审查模式**：全量（SQLite codegraph 索引 + ast-grep 规则扫描 + PMD 6.19.0 + 人工逐文件走查）
- **规则来源**：`${AI_SPEC_ROOT}/lang-spec/spec.java.md` 1.2.0 / `review.java.md` 1.2.0 + 通用审查框架
- **审查工具**：ast-grep-mcp + PMD 6.19.0 + codegraph + DeepSeek V4.1 Flash

> 说明：本轮为**全量复审**，前序增量报告 `code-review-20261010-001-java.md` 覆盖 `965cce9..35a9cbd` 增量；本轮独立复核全量并提出新发现（其中 H1/H2 为本轮新增）。

---

## 核心原则

1. **谦逊**：目的是治病救人，不是羞辱人；
2. **客观**：基于事实与规范，不带个人偏见；
3. **建设性**：不仅指出问题，还给出建议；
4. **优先级**：严重问题优先，区分必须修正 / 应当修正 / 建议改进。

---

## 审查结论（先行）

整体质量高：注释链完整、策略接口收敛决策点、Actor 单线程纪律明确、时钟回拨审查（`nanoTime` 差值）通过、新增行零硬性违例（无 SQL 注入、无硬编码密钥、生产路径无 `System.out`）。

本轮发现 **2 项高优先**（H1/H2，均为全量走查首次定位）、**7 项中优先**、**若干低优先建议**。
其中 **H1（父组 WAL 压实导致优先级状态丢失）为正确性缺陷**，建议优先修复。

---

## 通用审查结果

### 安全问题

| 项 | 结论 |
|---|---|
| 硬编码敏感信息 | 无（ast-grep 扫描 `password/secret/key/token` 零命中） |
| System.out / System.err | 仅 `cluster/ClusterTestRunner.java`（命令行自测工具，非库调用路径）命中；生产库路径零命中 |
| printStackTrace | 零命中 |
| SQL 注入 / XSS | 无数据库、无 Web 层，不适用 |
| 敏感字段日志输出 | 无；人工切主审计对自由文本做了 `sanitizeForLog` 换行净化（`PromoteGateway`） |
| 消息通道清单 | 无 MQ 中间件；跨进程通道仅为 Netty TCP（子组/父组两组），生产/消费双向对称，无幽灵/孤儿通道 |

### 性能问题

- **P-1（通过）**：选举超时、check-quorum、锁等待、续期等待全部使用 `System.nanoTime()` 差值，无时钟回拨风险（`review.java.md` 强制项通过）。
- **P-2（中）**：`MmapRaftStore.persistAndFlushTerm` 每次任期变更对**整个 128MB 映射**执行 `mmap.force()`（`MmapRaftStore.java:233`）。选主/投票时 term 写入较频繁，全量 msync 代价高；建议按需局部 force 或合并写入。
- **P-3（中）**：`LockOpGateway.doHandleLockOp` 每次请求 `new ProtostuffSerializer()` 两次（`:88`、`:109`），应提升为字段复用；`DistributedLockImpl` 已有 `serializer` 字段但 `LockOpGateway` 未复用。
- **P-4（低）**：Netty 连接查找 `NettyTransport.findEndpoint` 为线性扫描（`:194`），peer 数量级下影响可忽略。
- **P-5（中-低）**：日志级别滥用导致高频信息刷屏——见 Q-3。

### 代码质量问题

- **Q-1（中）**：`Speedboat.java` 仍达 981 行，承担单机启动/级联编排/双层观测/人工切主桥接/持久化收尾五类职责（相较 1241 行已通过 `ParentGroupBuilder`/`PromoteGateway` 抽取改善，仍有上帝类趋势）。
- **Q-2（中）**：`MembershipCoordinator` 与 `raft/MembershipManager` 功能**双实现**，且 `MembershipCoordinator` 未被门面装配路径使用（疑似遗留组件），存在双份逻辑漂移风险。
- **Q-3（中）**：日志级别不当——正常竞争/心跳路径使用 `INFO`：`ReplicationPump.java:59`（非 Leader 每次调用打 INFO）、`DistributedLockImpl.java:210`（锁被占用打 `error`）、`LockStateMachine` 多处 `info`。
- **Q-4（低）**：死代码 / 未使用成员——`MmapRaftStore.activeFile`、`CustomSerializer.HEADER_LEN`、`RaftLogStore.quorum`、`NettyTransport.invalidateChannel`、`ReportPublisher.clusterMemberIds`、`EvenNodeVoteWeightStrategy`（`startupFirst`/`electionInitiator` 恒 false，附加权重恒 0）、大量未使用 import（`Speedboat`、`NodeContext`、`ElectionCoordinator`、`NettyTransport` 等，PMD 命中）。
- **Q-5（低）**：`while (true)` 出现 2 处（`PropertiesConfigProvider.java:121,141`），均有显式 `break`；建议改显式循环条件以符合 `review.java.md` "禁止无限循环" 精神。
- **Q-6（低）**：魔数/魔法字符串散落——`PropertiesConfigProvider` 默认值（`1024/128/30000`）、`LockStateMachine.RESULT_WINDOW_MILLIS` 等；部分已有常量，部分仍硬编码。

### 并发安全

- **C-1（高，见 H1 之外的独立项）**：`NodeContext.peerIds` 声明为普通 `ArrayList`（`NodeContext.java:116`），成员变更在 raft 单线程 `add/remove`（`MembershipManager.java:217,231`），而 `RaftNode.getPeerIds() → QuorumCalculator.peerIds() → new ArrayList<>(ctx.peerIds)`（`QuorumCalculator.java:344`）可由任意线程调用**复制该 ArrayList**。复制过程与 raft 线程 `add/remove` 并发即构成数据竞争，可能抛 `ArrayIndexOutOfBoundsException` 或产生撕裂快照。这与 `NodeContext` javadoc 声称的 "可变状态仅由 raft 单线程写、外部只读 volatile 快照" 契约**不一致**。建议改用 `CopyOnWriteArrayList`（与 `ctx.log` 一致）。
- **C-2（中）**：`Term.current` 为普通 `long`（`Term.java:11`），非 `volatile`。但 `RaftNode` 接口契约（`RaftNode.java:41`）声明 `getTerm` 等读取方法为 "volatile 快照读"；外部线程通过 `Speedboat.getTerm()/(getIntraTerm/getParentTerm)` 读取可能得到陈旧值。观测层影响，但契约失真，建议 `current` 改 `volatile`。
- **C-3（低）**：`ApConsistencyPolicy.degradedAgainst/degradedSelfDatacenter` 非 volatile（`ApConsistencyPolicy.java:49,52`），设计上仅 raft 单线程读写，`reset()` 若被其他线程调用则存在可见性缺口（当前调用方受限于测试，风险低）。

---

## 语言特定审查结果（Java 规则应用）

| 规则 | 结果 |
|---|---|
| 时钟回拨（`currentTimeMillis` 用于间隔） | 选举/心跳/check-quorum/锁等待均通过；**唯一例外** `DistributedLockImpl.tryLock` 用墙钟计算 deadline（见 M-6，客户端等待上限，影响低） |
| 无限循环 | 仅 `PropertiesConfigProvider` 两处 `while(true)`，均有 break |
| `System.exit` / `System.gc` / `printStackTrace` | 生产路径零命中（`System.exit` 仅 ClusterTestRunner） |
| 资源关闭 | `LockStateMachine`/`PriorityStore`/`MmapRaftStore` 均 try-with-resources；PMD 对 `raf.getChannel()` 的 FileChannel 告警为误报（随 raf 关闭） |
| Protostuff 字段追加纪律 | 通过：`datacenter/priorityEpoch/priorityWeights` 追加于类末尾 |
| 序列化 `serialVersionUID` | 不适用（Protostuff 运行时 schema，无 Java 原生序列化） |
| `equals/hashCode` | `MembershipConfig`/`FailureRecord` 成对实现，通过 |
| 空 catch | 零命中（`PromoteGateway.captureAuditState` 的 catch 有 debug 留痕） |
| 捕获 `Throwable` | `MmapRaftStore.close`（`:182`）/`RaftNodeImpl.onRaftThreadAsync`（`:264`）捕获 `Throwable`——为隔离停机/任务异常，属有意为之，可接受 |
| `MembershipConfig` 缺省一致性 | **不一致**：无参构造 `enableAutoRemoval=false`，`Builder` 缺省 `true`（见 M-4） |

### 静态分析摘要（PMD 6.19.0，rulesets/java/quickstart）

| 类别 | 数量 | 代表 |
|---|---|---|
| volatile 使用建议（多为误报/设计） | 33 | NodeContext、LockEntry 等 |
| 控制语句缺大括号 | 11 | MembershipConfig、LogEntry 等 |
| 日志缺级别守卫 | 11 | RaftNodeImpl、ApConsistencyPolicy 等 |
| 未使用 import | ~30 | Speedboat、NodeContext、NettyTransport 等 |
| 无用括号 | 4 | ParentGroupBuilder、MmapRaftStore 等 |
| 未使用私有字段/方法/局部变量 | ~12 | activeFile、HEADER_LEN、quorum、serializerTypeId、invalidateChannel 等 |
| 硬编码 IP | 3 | ClusterTestRunner、NetworkUtils（回环缺省） |
| switch 缺 break | 2 | LockStateMachine:116、MmapRaftStore:383（实为最后一分支，误报） |

ast-grep 自动扫描：硬编码密钥 0、`printStackTrace` 0、生产 `System.out` 0、`System.gc` 0。

---

## 问题清单（按严重级别）

### 高优先级（必须修正）

#### H1 父组 WAL 压实后优先级状态丢失（正确性）

- 位置：`Checkpointer.java:90-108`（`maybeCheckpoint`）、`NoopStateMachine.java:32-44`、`MmapRaftStore.java:515-542`（`maybeCompact/compactNow`）、`RaftNodeImpl.java:152-161`
- 机理：
  1. 父组状态机为 `NoopStateMachine`，`snapshot()` 为空操作、**不产生任何检查点文件**；
  2. `Checkpointer.maybeCheckpoint` 只要 `stateMachine != null && checkpointFile != null` 即认为可检查点，调用 `snapshot()`（无副作用）后仍执行 `markCheckpoint(lastApplied)`，把 `MmapRaftStore.compactFloor` 推进；
  3. WAL 压力触发 `compactNow()`，按 `compactFloor` **丢弃前缀日志帧**；
  4. 重启时 `restoreStateCheckpoint` 经 `NoopStateMachine.restore` 得到 `lastAppliedIndex=0`，**无检查点可恢复**；被压实的 PRIORITY_CHANGE/leader 条目已从 WAL 消失，`applyCommittedEntries` 从 0 重放却找不到条目 → **优先级提升状态永久丢失**。
- 影响：长生命周期 + 多次选举/提升 + WAL 容量压力下，父组重启后丢失已生效的机房优先级；子组（`LockStateMachine` 有真实快照）不受影响。
- 建议：`Checkpointer` 增补"状态机是否可检查点"判定（如 `NoopStateMachine` 显式返回"不可检查点"标志，或在父组路径令 `getCheckpointFile()` 返回 null）；`MmapRaftStore.markCheckpoint` 仅在确认检查点文件写入成功后推进水位。

#### H2 `ctx.peerIds` 并发读写数据竞争

- 位置：`NodeContext.java:116`、`MembershipManager.java:217/231`、`QuorumCalculator.java:344`、`RaftNodeImpl.java:401`
- 机理：见的 C-1。`peerIds` 为普通 `ArrayList`，raft 线程增删，外部线程 `getPeerIds()` 复制。
- 影响：成员变更瞬间外部读取可能抛 `ArrayIndexOutOfBoundsException` 或得到不一致成员快照（监控/选举分母都消费该集合）。
- 建议：改用 `CopyOnWriteArrayList<String>`；或在 `peerIds()` 复制处加同步并保证发布语义。

### 中优先级（应当修正）

#### M-1 `ElectionTimeout.reset` 在 `min == max` 时抛异常

- 位置：`ElectionTimeout.java:28`：`nextLong(maxMs - minMs)`，`maxMs == minMs` 时 `nextLong(0)` 抛 `IllegalArgumentException`。
- 影响：配置 `election.*.timeout.min == max` 时选举定时器武装崩溃；`ElectionTimeout` 构造/`Builder` 均无 `min < max` 校验。
- 建议：构造期校验 `min < max` 并 fail-fast；`maxMs - minMs == 0` 时退化为固定超时。

#### M-2 `MembershipConfig.Builder` 与无参构造缺省不一致

- 位置：`MembershipConfig.java:37`（`enableAutoRemoval=false`）vs `:123`（Builder 缺省 `true`）。
- 影响：经 Builder 构建的配置会**默认开启自动剔除**，与"静态成员表才是安全缺省"的文档结论相悖；误剔除健康节点存在风险。
- 建议：Builder 缺省改为 `false`，与构造器/文档统一。

#### M-3 日志级别不当（高频 INFO / error）

- 位置：`ReplicationPump.java:59`（非 Leader 每次调用 INFO）、`DistributedLockImpl.java:210`（正常锁竞争打 `error`）、`InboundAppendHandler`/`LockStateMachine` 多处竞争路径 INFO。
- 影响：心跳/竞争高频路径刷屏，污染日志、影响吞吐与排障。
- 建议：降为 `DEBUG`，仅异常/状态跃迁保留 `INFO/WARN`。

#### M-4 `MembershipCoordinator` 双实现/疑似遗留

- 位置：`membership/MembershipCoordinator.java`（271 行）vs `raft/MembershipManager.java`。
- 机理：两者实现同一职责；门面 `Speedboat` 未装配 `MembershipCoordinator`。`MembershipCoordinator` 还裸建 `newSingleThreadScheduledExecutor()`（未用 `NamedThreadFactory`）。
- 建议：确认无外部使用后删除，或明确二者边界并收敛为唯一实现。

#### M-5 `CustomSerializer.unwrap` 未校验序列化器/消息类型保护

- 位置：`CustomSerializer.java:122,135`。
- 机理：读取 `serializerTypeId` 后**未使用**；CRC 仅覆盖 payload，`serializerTypeId`/`messageTypeId` 两个字节不在校验范围。
- 影响：未来引入第二种序列化器时无法识别；单字节位翻转若落在类型头则 CRC 无法拦截（落入未知 messageType 会抛异常，属半防护）。
- 建议：将类型头纳入 CRC 覆盖，或对 `serializerTypeId` 做校验。

#### M-6 `DistributedLockImpl.tryLock` 用墙钟计算等待上限

- 位置：`DistributedLockImpl.java:157-161`。
- 机理：`deadline = currentTimeMillis() + timeoutMs`，时钟回拨可拉长/截断等待窗口。`review.java.md` 强制超时用单调时钟。
- 影响：客户端等待上限偏差（非共识安全性）。`waitForApplyResult`/`waitForRelease` 已正确用 `nanoTime`。
- 建议：`tryLock` 改用 `System.nanoTime()`。

#### M-7 `RpcMessageHandler.dispatch` 类型判定顺序不安全

- 位置：`RpcMessageHandler.java:94-99`。
- 机理：`else` 分支先 `(RequestVoteResponse) response` 强转，再 `instanceof` 判断；若未来出现未列举的响应类型将 `ClassCastException`（被外层 catch 关闭连接）。
- 建议：调整为 `instanceof` 判定优先，非法类型记录并忽略。

### 低优先级（建议改进）

- **L-1** 死代码清理：`MmapRaftStore.activeFile`、`CustomSerializer.HEADER_LEN`、`RaftLogStore.quorum`、`NettyTransport.invalidateChannel`、`ReportPublisher.clusterMemberIds`、`EvenNodeVoteWeightStrategy`（恒等 0）。删除可降低维护噪音。
- **L-2** 未使用 import 批量清理（PMD 命中约 30 处）。
- **L-3** `RaftNodeImpl.java:241` 附近异常包装：`new IllegalStateException("Raft task failed", cause)` 已保留 cause（PMD 该条为误报），无需处理。
- **L-4** `RoleMachine.doTransitionTo` 在 `LEADER→FOLLOWER` 时 `hooks.onEnterFollower()` 与 `FollowerRole.enter→lifecycle` 双重执行（撤销心跳 + 重置超时各两次）。幂等无害，但建议去重。
- **L-5** `NettyTransport.MAX_FRAME_LENGTH` 固定 1MB（`:36`），无分片协议；大批量 AppendEntries 超限将被 `LengthFieldBasedFrameDecoder` 丢弃。建议评估批量上限或增加分片。
- **L-6** `RaftNodeMicrometerListener` 基础 Gauge（term/commitIndex 等）未加 `nodeId` tag（`RaftNodeMicrometerListener.java:71-79`），同 registry 多节点会重复注册冲突。
- **L-7** `DatacenterVoteWeightStrategy.calculateAdditionalWeight` 对其他机房返回 `-1`（`:30`），使对侧节点权重降为 0，等于剥夺对侧投票权。该类为示例策略（配置入口不可达），建议加 `@Deprecated` 或防御性下限。
- **L-8** `ClusterTestRunner` 硬编码测试 IP/端口，仅自测工具，建议抽取参数。
- **L-9** `Speedboat` 的 `parentGroup` 字段非 volatile（`:116`），当前依赖 `running` volatile 建立 happens-before；建议补注释或改 volatile 以防后续重构破坏可见性。

---

## 优秀设计（值得肯定）

1. **Actor 纪律贯彻**：全部状态变更收敛到 raft 单线程，入站消息经 `RpcBridge` 异步搬运，杜绝跨节点 raft 线程互等死锁；组件（`ElectionCoordinator`/`ReplicationPump`/`ApplyEngine` 等）单一职责、单向依赖消环。
2. **策略层收敛决策点**：`VoteWeightStrategy`/`LeadershipPolicy`/`ConsistencyPolicy`/`GroupStrategy`/成员策略将拓扑知识外置，`peer` 缺省零行为变更，向后兼容设计精细。
3. **优先级表热更新**：`DatacenterPriorityTable` volatile 不可变快照 + epoch 单调收敛，读无需加锁，传播（心跳/投票）"只升不降"。
4. **席位门控三入口一致**：`doStartElection`/`doBecomeLeader`/`doHandleRequestVote`/`doHandlePreVoteRequest` 对 `seatHeld` 同口径把关，防同机房多代表虚增父组票数。
5. **持久化完整性**：term 双槽交替 + CRC + msync；快照/优先级文件临时文件 + rename 原子替换，失败兜底清理。
6. **手动切主纵深防御**：门面 `PromoteGateway`（CP 闸门 + 连通性闸门 + 审计净化）与 `RaftNodeImpl` 内部 AP 闸门双重把关，`sanitizeForLog` 防日志注入。
7. **时钟回拨防护**：选举/心跳/看门狗/锁等待全部使用 `System.nanoTime()`。

---

## 改进建议（汇总）

### 必须修正（优先级：高）

1. **H1** 修复父组 WAL 压实与 `NoopStateMachine` 无检查点导致的优先级状态丢失；
2. **H2** `ctx.peerIds` 改 `CopyOnWriteArrayList` 或加同步。

### 应当修正（优先级：中）

1. **M-1** `ElectionTimeout` 校验 `min < max`；
2. **M-2** 统一 `MembershipConfig` 缺省（`enableAutoRemoval`）；
3. **M-3** 收敛高频日志级别；
4. **M-4** 消除 `MembershipCoordinator` 双实现；
5. **M-5** 序列化类型头纳入校验；
6. **M-6** `tryLock` 改 `nanoTime`；
7. **M-7** `RpcMessageHandler` 判定顺序调整。

### 建议改进（优先级：低）

1. 死代码与未使用 import 清理（L-1/L-2）；
2. 双重 Follower 副作用去重（L-4）；
3. 帧长/分片评估（L-5）；
4. metrics tag 补齐（L-6）；
5. 示例策略标注/防御（L-7）。

---

## 数据库设计审查结果

本项目无数据库、无 ORM、无 MyBatis；持久化由 `RaftStore` 三档实现（Nop/Mem/Mmap）与 `PriorityStore` 文件承载。数据库审查项不适用。

---

## 总结

- **总体评价**：架构清晰、分层合理、注释与设计文档质量高，Actor 模型与策略模式落地扎实；核心算法（选举/复制/成员变更/优先级）方向正确。相较 2026-09 审查，历史 P0/P1 已闭环，代码进一步模块化（`ParentGroupBuilder`/`PromoteGateway` 抽取）。
- **关键风险**：**H1** 是唯一影响正确性的缺陷，且仅在长运行 + WAL 压力下触发，隐蔽性高，建议优先修复；**H2** 是并发契约的显式违反，修复成本低。
- **实施建议**：先修 H1/H2 与 M-1（配置健壮性），再收敛日志级别与死代码；增量审查已覆盖的策略/优先级改造方向无需回退。

---

**审查工具**：ast-grep-mcp + PMD 6.19.0 + codegraph + DeepSeek V4.1 Flash
**规则来源**：`${AI_SPEC_ROOT}/lang-spec/spec.java.md` 1.2.0 / `review.java.md` 1.2.0
**审查完成时间**：2026-10-10

---

## 修复记录（2026-10-10，同日落地；另含对报告两处判定的复核修正）

**两项复核修正**（不改变"`是问题就修"的行动结论，但为日后追溯保留事实）：

1. **H1 影响链整段高估**：机理前三步（Noop 空快照可越级推进 `compactFloor`、WAL 压实丢前缀、
   重放静默跳过）全部成立，但第 4 步"优先级提升状态永久丢失"错误——优先级状态的**权威恢复源
   是 `PriorityStore`**（`parent-priority.properties`，apply/adopt 成功即写、启动 load 覆盖配置
   派生表，与 Checkpointer 水位零耦合）；文件损坏时尚有对侧心跳 `priorityEpoch` 高于本表即采纳
   的自愈通道；`applyCommittedEntries` 对被压实条目是 `findEntryAt == null` **静默跳过**（父组
   状态机 Noop 本无业务状态可丢）。正确定性：**Checkpointer 语义缺口（防御性·中）**，仍按
   "跳过"修复。特例：`raft.persistence=none` 部署下无 PriorityStore 文件时影响回到原表述。
2. **L-7 的"配置入口不可达"** 错误：`vote.weight.strategy=even` 分支实际可达
   （`PropertiesConfigProvider#getVoteWeightStrategy`）；EvenNodeVoteWeightStrategy
   附加权重恒 0，等价于"无策略"语义，行为无害——改为加 `@Deprecated` 并在 javadoc 更正口径，
   保留为显式声明等权拓扑的用户可读路径。

**修复落地**（全量 616/616 通过，因 M-4 移除被冻结遗留组件的专属集成测试 -6）：

| 项 | 落地 |
|---|---|
| H1 | `StateMachine` 增 `default boolean canSnapshot()`；`NoopStateMachine` 显式返回 false；`Checkpointer.maybeCheckpoint/snapshotOnShutdown` 在不可检查点的状态机上一律**不推进 compactFloor**（停止前仅 flush） |
| H2/C-1 | `NodeContext.peerIds` 改 `CopyOnWriteArrayList`（raft 单线程写 × 外部观测读的弱一致安全快照） |
| C-2 | `Term.current` 改 `volatile`（对齐 `RaftNode.getTerm` 契约）；C-3 注明降级细节字段"仅 raft 单线程" |
| M-1 | `ElectionTimeout` 构造期校验 `0 ≤ min ≤ max`；`reset` 在 `min==max` 时退化为固定超时（不经 `nextLong(0)`） |
| M-2 | `MembershipConfig.Builder.enableAutoRemoval` 缺省与无参构造统一为 `false` |
| M-3 | `ReplicationPump` 非 Leader 路径 INFO→DEBUG；`DistributedLockImpl` 锁竞争判负 error→debug |
| M-4 | 删除 `MembershipCoordinator`（含其专属集成测试）——生产无装配、双实现漂移点、裸建 scheduler |
| M-5 | `CustomSerializer` CRC 覆盖范围扩为 **ser/msg 类型头 + payload**（wrap/unwrap 同口径生成），`serializerTypeId` 由"读取未用"收紧为显式校验；`CustomSerializerTest` 帧断言同步 |
| M-6 | `tryLock` 等待上限改 `System.nanoTime()` 单调时钟 |
| M-7 | `RpcMessageHandler.dispatch` 判定顺序改为 `instanceof` 优先，未列举类型记 warn 丢弃 |
| Q-4/L-1 | 删除死代码：`MmapRaftStore.activeFile`、`CustomSerializer.HEADER_LEN`、`RaftLogStore.quorum`（字段+构造参数+调用点）、`NettyTransport.invalidateChannel`、`ReportPublisher.clusterMemberIds` |
| Q-5 | `PropertiesConfigProvider` 两处 `while(true)` 显式化循环条件 |
| L-2 | 未使用 import 全量清理 |
| L-6 | Micrometer 基础 Gauge 统一补 `nodeId` tag（多节点同 registry 不再互相覆盖） |
| L-7 | `EvenNodeVoteWeightStrategy` 加 `@Deprecated` **并更正报告口径**（实际可达，语义等价默认） |

**Q-1（Speedboat 981 行）**：本轮不再进一步拆分（五职责经 M1 拆分后各自带域，981 行량在可控带内），留待后续按业务边界单独成题；**L-3/L-4/L-5/L-8**：报告已自标"幂等无害/Pearl/纯工具"性质，保留现状，另行根据业务实际需求取舍。表中的 H2 与 M-4 联动提醒（核对 peerIds 竞争的唯一入口正是 M-4 组件）即在此闭环落地。
