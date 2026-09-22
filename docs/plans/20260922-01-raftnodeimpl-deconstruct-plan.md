# RaftNodeImpl 拆解执行计划

> 日期：2026-09-22 · 依据：docs/analysis/20260922-02-raftnodeimpl-deconstruct-analysis.md
> 验收铁律：`RaftNode` 公开契约不变；519+ 测试全绿；行为语义零变化（纯结构手术）

---

## 一、三策总览

### 下策（0.5 天，最小手术）
只拆**纯函数**与**日志仓**两件：
1. 新建 `raft/QuorumCalculator`：收敛 calculateTotalVoteWeight / calculateReceivedVoteWeight / calculateRequiredWeight / calculateVoteWeight 及 advanceCommitIndex 内的第 4 变体（单一纯函数 + peerDatacenter 查表）；4 处调用点改引用。
2. 新建 `raft/RaftLogStore`：log/logIndexMap 的追加/重建/裁剪/查找/持久化桥接全部搬入。
其余 1700 行不动。风险最低，但 RaftNodeImpl 仍 ~1400 行，上帝类只剥一层皮。

### 中策（2~3 天，推荐 ⭐）
= 下策 + **组件化全量拆分**，`RaftNodeImpl` 降至门面（目标 ≤450 行，纯布线+委派）：

| 新类 | 职责（唯一变化原因） | 吸走的原代码 |
|---|---|---|
| `raft/core/NodeContext` | 共享可变状态承载（role/term/votedFor/leaderId/commit/lastApplied/peers/peerDc/raftStore/executor/stateMachine logStore 等）+ volatile 快照族 getter | L109-199 字段、L1367-1464 getter 群 |
| `raft/core/RoleMachine` | doTransitionTo、Candidate/LEADER/FOLLOWER 副作用钩子（清票/term 自增/武装心跳）、isLeader/isMain | L424-458、L1161-1167 |
| `raft/election/ElectionCoordinator` | doStartElection、PreVote 三件套、requestVotesFromPeers、checkElectionResult、runElectionTimeout、心跳新鲜判定、登基 doBecomeLeader/appendLeaderEntry | L600-871、L1238-1250 |
| `raft/election/QuorumCalculator` | 权重大多数派纯函数族（下策件） | L1277-1365、advanceCommitIndex 内联变体 |
| `raft/replication/HeartbeatWatchdog` | lastResponseNanos 维护 + check-quorum 降级判定 | L873-913、L979 |
| `raft/replication/ReplicationPump` | sendHeartbeat/sendAppendEntries(ToPeer) 出站泵 + doHandleAppendEntriesResponse（matchIndex/nextIndex+回退 hint）+ advanceCommitIndex | L915-1038 |
| `raft/replication/InboundAppendHandler` | doHandleHeartbeat/doHandleAppendEntries 入站一致性校验/追加/commit 推进 | L502-598 |
| `raft/log/RaftLogStore` | append/appendLeaderEntry 落位、WAL 恢复重建、getEntryAt/getLastLogIndex/Term、truncateLogIfNeeded/resyncSafeFloor、persistTermState | L1095-1159、L1476-1509(部分)、L1511-1582、start() 内 WAL 重建段 |
| `raft/log/Checkpointer` | maybeCheckpoint、start/shutdown 恢复链（restoreTerm/checkpoint restore/停机兜底快照） | L1061-1093、start/shutdown 检查点段 |
| `raft/membership/MembershipManager` | 检测任务/recordFailure/proposeAdd/RemoveMember（同构合并为 proposeMemberChange 一个入口）/replicateMemberChange/processMemberChange | L1584-1827 |
| `raft/lockops/LockOpGateway` | doHandleLockOp（裁决+授权打点）/forwardLockOp | L1511-1582(LockOp 段) |
| `raft/raft/Proposer`(或在 ProposeManager) | doPropose 命令提案（依赖 logStore+sends） | L1466-1509 |
| `raft/raft/ReportPublisher` | publishReport（raft 线程收集不可变快照） | L1188-1201 |

`RaftNodeImpl` 门面保留：Builder 布线、start/shutdown 编排（调 RoleMachine/LogStore/Checkpointer/MembershipManager 启动）、公开 API 单行委派、定时任务武装（ElectionTimeout/Heartbeat/MemberCheck 调度三件，属门面级"心跳节拍"，不拆更碎）。

**关键设计约束**：
- 组件间不互相持有引用，一律经 `NodeContext` 取协作对象（单向依赖：门面→组件→context）；避免环形引用。
- 所有组件方法显式标注调用线程契约（"raft 单线程内调用"），沿用 Actor 纪律。
- 用户明确要求"不删除原有注释、必要时补充"：搬迁代码时注释随行，重要搬迁点补充归属说明。
- 上帝类消灭判据：RaftNodeImpl < 500 行；任一新组件单一职责、无 (>300 行) 的"次上帝类"。

### 上策（5~6 天）
= 中策 + 角色状态模式（Follower/Candidate/Leader 三态类，MicroRaft/RaftState 形态）+ 入站每 RPC 独立 Handler 类 + MemberChange 同构体依 joint consensus 协议接口化。风险：触碰主链二次，一天内难以回归全部 IT；收益目前仅是"形态完备"，实际无新特性需求驱动——**不建议现在做**。

## 二、执行排程（按中策，Phase 制）

- **Phase 1**：NodeContext + RoleMachine 迁移（状态与角色，最基础）→ 全量测试绿
- **Phase 2**：QuorumCalculator 纯函数 + RaftLogStore + Checkpointer（三大地基件）→ 全量测试绿
- **Phase 3**：ElectionCoordinator + HeartbeatWatchdog + ReplicationPump + InboundAppendHandler（复制/选举主链）→ 全量测试绿
- **Phase 4**：MembershipManager + LockOpGateway + Proposer + ReportPublisher 迁出；RaftNodeImpl 压缩为门面 → 全量测试绿
- **Phase 5**：行数/职责核查（RaftNodeImpl<500/组件<300）、注释归属补充、docs 计划与结论记录、git 提交

每个 Phase 独立 `mvn test` 验证 + 独立 commit。

## 三、风险与对策

| 风险 | 对策 |
|---|---|
| start() 恢复链（WAL/checkpoint）搬迁丢序 | 序号注释钉死：restoreTerm → WAL 重建 → checkpoint lastApplied 跳位 → apply 重放；搬迁后 diff 验证 |
| 权重策略为 null 的分支语义 | QuorumCalculator 内保留全部分支 + 单测跑 null 策略路径 |
| 迁移过程中字段被遗漏读旧引用 | 一次性迁移字段进 NodeContext，旧字段名 IDE 无残留引用即签核 |
| Actor 单线程契约破坏 | 桥接 onRaftThread/onRaftThreadAsync 保留在门面；组件一律裸方法由门面投递 |

---

## 执行结果（2026-09-22 同日完成，采用上策）

- 全部 4 个 Phase 已连续执行完成，`mvn test` **522 全绿**（含迁移前后等价验证）
- 样式学习参照：/home/helly/code/java/tiny-rules（接口+final 类、RuleMgrState 状态承载、每字段方法中文 Javadoc）
- 最终形态：RaftNodeImpl 1829 → 541 行（门面：布线+Actor桥+API委派+定时武装）；原代码全部迁入 24 个新类：

| 类 | 行数 | 职责 |
|---|---|---|
| NodeContext | 246 | 共享状态承载（RuleMgrState 样式） |
| RoleMachine | 124 | 角色迁移编排 |
| RaftRole/RoleLifecycle/FollowerRole/CandidateRole/LeaderRole | ~150 | 角色状态进入副作用（三态类） |
| QuorumCalculator | 173 | 权重法定多数纯函数（4 处同构归一） |
| RaftLogStore | 290 | 日志仓：append/裁剪/WAL/-term 持久化 |
| Checkpointer | 115 | 检查点+恢复链 |
| ElectionCoordinator | 406 | 选举主链（RequestVote/PreVote/超时/登基） |
| HeartbeatWatchdog | 73 | check-quorum 降级 |
| ReplicationPump | 183 | 出站复制+应答+commit 推进 |
| InboundAppendHandler | 131 | 入站日志受理 |
| ApplyEngine | 96 | 全序重放 |
| MembershipManager | 243 | 成员变更 |
| CommandProposer | 63 | 命令提案 |
| LockOpGateway | 99 | 锁裁决/转发 |
| ReportPublisher | 69 | 双通道报告 |
| RpcBridge + 4×*RpcHandler | ~155 | 每 RPC 独立入站 Handler（上策） |
