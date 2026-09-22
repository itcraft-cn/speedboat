# RaftNodeImpl 拆解分析报告

> 日期：2026-09-22
> 对象：src/main/java/cn/itcraft/speedboat/raft/RaftNodeImpl.java（1829 行 / 80+ 方法）
> 目标：按功能分类，一个功能独立成一个类；对外 `RaftNode` 契约与 519 绿测试不动

## 一、现状盘点（按代码实测归位）

| 区域 | 行数（约含注释） | 代表方法/状态 |
|---|---|---|
| 1. 状态与字段声明 | L109-199 | 30 个字段：身份/term/日志/成员/锁/预投票/check-quorum 混排 |
| 2. 构建 & 生命周期 | L200-367 | 构造器、start()（恢复链+WAL重建+checkpoint+transport接线+定时任务）、shutdown() |
| 3. Actor 桥接 | L369-422 | onRaftThread（同步门面）、onRaftThreadAsync（异步桥） |
| 4. 角色迁移 | L424-458 | doTransitionTo（含 CANDIDATE 投票清零/term 自增、LEADER 武装心跳） |
| 5. 入站投票处理 | L460-500 | doHandleRequestVote（stickiness 判定内联） |
| 6. 入站日志复制处理 | L502-598 | doHandleHeartbeat→doHandleAppendEntries（冲突校验/追加/commit 推进） |
| 7. 选举主链 | L600-825 | doStartElection、PreVote 三件套（doStartPreVote/doHandlePreVoteResponse/doHandlePreVoteRequest）、requestVotesFromPeers、checkElectionResult、runElectionTimeout、心跳新鲜判定 |
| 8. 登基/LeaderEntry | L826-871 | doBecomeLeader、appendLeaderEntry |
| 9. 心跳+check-quorum | L873-913 | sendHeartbeat、doCheckQuorumMaybeDemote（lastResponseNanos 权重判定） |
| 10. 出站复制 | L915-996 | sendAppendEntries(ToPeer)、doHandleAppendEntriesResponse（matchIndex/nextIndex 维护+回退 hint） |
| 11. commit 推进 & apply | L998-1059 | advanceCommitIndex（权重多数判定）、applyCommittedEntries（COMMAND/MEMBER_CHANGE 分派） |
| 12. 检查点 | L1061-1093 | maybeCheckpoint（先落盘后 markCheckpoint 铁律） |
| 13. 日志仓 & 裁剪 | L1095-1159 | truncateLogIfNeeded（双安全路径）、resyncSafeFloor；log/logIndexMap 全部读写 |
| 14. 发布报告 | L1188-1201 | publishReport |
| 15. 定时任务武装 | L1203-1275 | resetElectionTimeout、runElectionTimeout、startHeartbeat/cancelHeartbeat |
| 16. 权重四件套 | L1277-1365 | calculateTotalVoteWeight/Received/Required/calculateVoteWeight（4 处同构循环） |
| 17. 观测 getter 群 | L1367-1464 | 大约 20 个 getter/setter |
| 18. 提案 | L1466-1509 | doPropose |
| 19. 锁转发协议 | L1511-1582 | doHandleLockOp（Leader 裁决+授权打点）、forwardLockOp |
| 20. 成员变更 | L1584-1827 | 检测任务、recordFailure、proposeAdd/RemoveMember（两份同构体）、replicateMemberChange、processMemberChange |

## 二、职责纠缠点（拆分的理由所在）

1. **一个类同时是：角色机、日志仓、选举器、复制泵、成员变更器、锁网关、调度器**——七种原因变化。
2. **状态裸露**：votedFor/votedForTerm/leaderId/commitIndex 等 volatile 裸字段，被 4 类子系统直接读写；没有任何舞台演出时的一致性快照。
3. **权重计算四连**（L1283-1353）同一"peer 权重求和"逻辑以 3 种变体复制内联；`advanceCommitIndex` 又内联了一份第 4 变体——反复横跳的多数派判定应为一处纯函数。
4. **持久化链散落**：restoreTerm/WAL 重建/checkpoint 三段恢复逻辑埋在 start()；persistTermState 只有一个调用点对（term 提升与投票授予）——持久化时机知识散在选举与复制两条主链里。
5. **定时任务武装**（选举超时/心跳/成员检测）与执行器交互逻辑占据三段，生命期 Future 泄漏风险靠"cancel-before-schedule"注释约定而非类型约束。

## 三、可行拆解方向（边界）

- `RaftNode` 接口方法（20 个公开方法）保持逐字不动；`RaftNodeImpl` 降级为**门面协调器**（构造布线 + 公开 API 委派 + 超时/心跳武装）。
- 拆出的组件只允许通过"共享上下文 NodeContext"协作，其上收敛全部可变 Raft 状态（term/role/leader/commit/lastApplied/peers），每个组件单一写职责。
- 测试面：`RaftNodeTest/RaftNodeLogReplicationTest/RaftProtocolIntegrationTest/RaftGroupTest` 等全部经接口与 Builder 调用，无需改动即验证行为等价。
