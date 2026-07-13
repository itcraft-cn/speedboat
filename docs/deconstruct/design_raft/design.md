# Raft 模块设计文档

## 模块概述

`cn.itcraft.speedboat.raft` 是 Speedboat 的核心模块，基于 Raft 共识算法实现分布式主节点选举。模块包含标准 Raft 协议的核心组件：Term（任期）、LogEntry（日志条目）、NodeState（节点状态）、ElectionTimeout（选举超时）、VoteContext（投票上下文）、FailureRecord（故障记录）、LeaderRecord（Leader 记录），以及核心的 RaftNode（Raft 节点）和 RaftGroup（Raft 组）。

## RaftNode

### 职责

Raft 协议的核心实现，管理单个节点的完整生命周期，包括选举、日志复制、心跳维护、成员变更。

### 设计模式

Builder 模式——通过 `RaftNode.Builder` 构建，支持链式配置所有组件。

### 核心状态字段

| 字段 | 类型 | 线程安全机制 | 说明 |
|------|------|-------------|------|
| `nodeId` | `String` | final | 节点唯一标识 |
| `currentState` | `volatile NodeState` | volatile 可见性 | 当前节点状态 |
| `term` | `Term` | final | 任期计数器 |
| `votedFor` | `volatile String` | volatile 可见性 | 当前任期投票对象 |
| `votedForTerm` | `volatile long` | volatile 可见性 | 投票时任期 |
| `leaderId` | `volatile String` | volatile 可见性 | 当前已知 Leader |
| `lastHeartbeatNanos` | `volatile long` | volatile 可见性 | 最后收到心跳时间（纳秒） |
| `log` | `CopyOnWriteArrayList<LogEntry>` | 并发安全集合 | 日志条目列表 |
| `commitIndex` | `volatile long` | volatile 可见性 | 已提交日志索引 |
| `lastApplied` | `volatile long` | volatile 可见性 | 最后应用的日志索引 |
| `votesReceived` | `ConcurrentHashMap` | 并发安全集合 | 投票记录 |
| `nextIndex` | `ConcurrentHashMap` | 并发安全集合 | 每个 Follower 的下一条待发送日志索引 |
| `matchIndex` | `ConcurrentHashMap` | 并发安全集合 | 每个 Follower 已匹配的日志索引 |
| `failureRecords` | `ConcurrentHashMap` | 并发安全集合 | 故障记录表 |

### 生命周期管理

**start()**：设置 `running=true`，创建 2 线程的 `ScheduledExecutorService`，注册 3 个 RPC 处理器到 TransportLayer，重置选举超时。

**shutdown()**：取消所有定时任务（electionTimeoutFuture、heartbeatFuture、membershipChangeFuture），关闭调度器，等待 `SHUTDOWN_TIMEOUT_SECONDS`（1 秒）终止。

### 状态转换算法

`transitionTo(NodeState)` 为 `synchronized` 方法，确保原子性：

1. 验证 `currentState.canTransitionTo(newState)` 合法性，非法时抛出 `IllegalStateException`
2. 记录旧状态，更新 `currentState`
3. 按新状态执行初始化操作：
   - **CANDIDATE**：`votesReceived.clear()`，`votedFor = nodeId`（自投票），`term.increment()`
   - **LEADER**：`startHeartbeat()`
   - **FOLLOWER**：`cancelHeartbeat()`，`resetElectionTimeout()`
4. 从 LEADER 降级为 FOLLOWER 时额外取消心跳

### 选举流程

**startElection()**：
1. 仅当 FOLLOWER/CANDIDATE 时触发，转换到 CANDIDATE
2. 自投票：`votesReceived.put(nodeId, true)`
3. 计算 `totalVoteWeight`（基础 1 + 策略附加权重）和 `votesNeeded`（N/2+1）
4. 若自身权重已满足多数，直接 `becomeLeader()`
5. 否则调用 `requestVotesFromPeers(votesNeeded)`

**requestVotesFromPeers()**：遍历所有 peer，发送 `RequestVoteRequest`（含 term、candidateId、voteWeight），异步处理响应。收到投票时 `synchronized(votesReceived)` 记录并检查是否达到多数。同时以 `electionTimeout.getNext()` 为周期设置超时重试。

**becomeLeader()**：仅当 CANDIDATE 时执行。转换到 LEADER，设置 `leaderId = nodeId`，初始化所有 peer 的 `nextIndex` 和 `matchIndex`，追加 LEADER_INFO 条目到日志，发送 AppendEntries，启动心跳。

### 日志复制

**sendAppendEntries()**：仅 Leader 执行，遍历所有 peer 调用 `sendAppendEntriesToPeer()`。

**sendAppendEntriesToPeer()**：计算 prevLogIndex/prevLogTerm，收集从 nextIndex 开始的日志条目，构造 `AppendEntriesRequest` 发送。

**handleAppendEntriesResponse()**：`synchronized` 方法。成功则更新 matchIndex/nextIndex 并调用 `advanceCommitIndex()`；失败且 `response.getTerm() > term.getCurrent()` 则降级为 FOLLOWER；失败且 term 不变则递减 nextIndex。

**advanceCommitIndex()**：从 `getLastLogIndex()` 向 `commitIndex` 扫描，找到首条在当前任期内且被多数节点复制（matchCount >= N/2+1）的日志，更新 commitIndex 并调用 `applyCommittedEntries()`。

**applyCommittedEntries()**：从 lastApplied+1 到 commitIndex，按 EntryType 分发：
- `COMMAND`：委托给 `stateMachine.apply(entry)`
- `MEMBER_CHANGE`：调用 `processMemberChange()`
- `LEADER_INFO`：更新 `leaderId`

### 日志截断

`truncateLogIfNeeded()`：当日志条目数超过 `maxLogSize` 时，从头部移除最旧条目，但不会移除已提交（index <= commitIndex）的条目。

### 成员变更

**校验流程**：`proposeAddMember()` 和 `proposeRemoveMember()` 均需验证：是否是 Leader、是否通过 `changeValidationStrategy.validateAdd/validateRemove()` 校验。

**ADD 流程**：创建 `MemberChangeEntry.add()`，追加到日志，调用 `replicateMemberChange()` 复制到所有 peer。

**REMOVE 流程**：创建 `MemberChangeEntry.remove()`，追加到日志，复制时跳过被移除节点。

**processMemberChange()**：ADD 时添加 peer 到 `peerIds` 并初始化 nextIndex/matchIndex；REMOVE 时从 `peerIds` 移除并清理相关状态。

**recordFailure()**：使用 `ConcurrentHashMap.compute()` 原子更新故障记录，当 `shouldProposeRemoval()` 返回 true 时提议移除。

### 线程安全策略

| 场景 | 机制 |
|------|------|
| 状态转换（transitionTo, handleRequestVote, handleAppendEntries, becomeLeader） | `synchronized` 方法级锁 |
| 日志列表 | `CopyOnWriteArrayList`，适合读多写少 |
| 索引映射（nextIndex, matchIndex, failureRecords, votesReceived） | `ConcurrentHashMap` |
| 关键状态字段（currentState, leaderId, votedFor 等） | `volatile` 保证可见性 |
| 定时任务（选举超时、心跳、成员变更检测） | `ScheduledExecutorService` |

### Builder 配置项

| 配置项 | 类型 | 必需 | 默认值 |
|--------|------|------|--------|
| `nodeId` | `String` | 是 | 无（抛出异常） |
| `peerIds` | `List<String>` | 否 | 空列表 |
| `electionTimeout` | `ElectionTimeout` | 否 | `new ElectionTimeout(150, 300)` |
| `voteWeightStrategy` | `VoteWeightStrategy` | 否 | null |
| `groupStrategy` | `GroupStrategy` | 否 | null |
| `transportLayer` | `TransportLayer` | 否 | null |
| `maxLogSize` | `int` | 否 | `DEFAULT_MAX_LOG_SIZE`（10） |
| `membershipConfig` | `MembershipConfig` | 否 | `new MembershipConfig()` |
| `healthCheckStrategy` | `HealthCheckStrategy` | 否 | `PassiveHealthCheckStrategy` |
| `registryStrategy` | `RegistryStrategy` | 否 | `NoOpRegistryStrategy` |
| `changeValidationStrategy` | `ChangeValidationStrategy` | 否 | `DefaultChangeValidationStrategy` |
| `stateMachine` | `StateMachine` | 否 | null |

## RaftGroup

### 职责

管理同一组内的多个 RaftNode，支持级联架构（父子组关系）。

### 核心字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `groupId` | `String` | 组唯一标识 |
| `nodes` | `List<RaftNode>` | 组内节点列表（不可变视图） |
| `leader` | `volatile RaftNode` | 当前 Leader 缓存 |
| `groupStrategy` | `GroupStrategy` | 分组策略 |
| `parentGroup` | `RaftGroup` | 父组（级联架构） |
| `childGroups` | `List<RaftGroup>` | 子组列表 |

### 级联架构

跨机房场景中，每个机房内的 Raft 组是子组（Child Group），跨机房组成的 Raft 组是父组（Parent Group）。子组先选出 Leader，子组 Leader 参与父组选举。

**父子关系维护**：`setParentGroup()` 和 `addChildGroup()` 自动维护双向引用，确保一致性。

### 节点创建

`createNodes()` 从 URL 列表创建 RaftNode 列表：解析每个 URL 为 NodeEndpoint，提取 nodeId，为每个节点构建 peerIds（排除自身），使用 `groupStrategy` 的 min/max 超时创建 ElectionTimeout。

### 生命周期

- `start()`：遍历所有节点调用 `start()`
- `shutdown()`：遍历所有节点调用 `shutdown()`
- `electLeader()`：遍历节点查找 Leader 并缓存
- `isMain()`：遍历节点检查是否有 Leader
- `checkChildGroupHealth()`：检查所有子组运行状态

## Term

### 职责

单调递增的任期计数器，是 Raft 协议"时间"的核心。

### 关键方法

| 方法 | 逻辑 | 使用场景 |
|------|------|----------|
| `increment()` | `current++` | 成为 Candidate 时 |
| `updateIfHigher(long)` | 仅当 newTerm > current 时更新 | 收到更高任期消息时 |
| `isMonotonicViolation(long)` | 判断 proposedTerm < current | 拒绝旧任期请求 |
| `reset()` | 重置为 0 | 测试/恢复 |

**设计要点**：`updateIfHigher` 体现 Raft 协议核心规则——节点总是接受更高任期，发现更高任期即降级为 FOLLOWER。

## LogEntry

### 职责

Raft 日志条目的抽象基类，支持三种条目类型。

### EntryType 枚举

| 类型 | 含义 | 产生时机 |
|------|------|----------|
| `LEADER_INFO` | Leader 标识 | Leader 当选时追加 |
| `MEMBER_CHANGE` | 成员变更 | 添加/移除节点时 |
| `COMMAND` | 命令 | 分布式锁等业务操作 |

### 核心字段

`index`、`term`、`leaderId`、`entryType`、`data`（可选字节数组）。默认构造器创建 `LEADER_INFO` 类型条目。

### 继承体系

- **CommandLogEntry extends LogEntry**：固定 `EntryType.COMMAND`，提供静态工厂 `create()`
- **MemberChangeEntry extends LogEntry**：固定 `EntryType.MEMBER_CHANGE`，增加 `ChangeType`（ADD/REMOVE）、`nodeId`、`address` 字段，提供 `add()`/`remove()` 静态工厂。同时提供 `getPeerId()` 别名方法。

## NodeState

### 职责

定义 Raft 节点三种状态及其合法转换。使用静态初始化块为每个枚举值配置 `validNextStates` 集合，`canTransitionTo()` 实现 O(1) 校验。

### 合法转换表

| 当前状态 | 可转换到 |
|----------|----------|
| LEADER | FOLLOWER |
| FOLLOWER | CANDIDATE, FOLLOWER |
| CANDIDATE | LEADER, FOLLOWER, CANDIDATE |

## ElectionTimeout

### 职责

随机选举超时生成器，防止选举活锁。

**算法**：`currentTimeout = minMs + ThreadLocalRandom.current().nextLong(maxMs - minMs)`，在 [minMs, maxMs) 区间均匀随机。使用 `ThreadLocalRandom` 避免锁竞争。

## VoteContext

### 职责

投票决策的上下文元数据，传递给 VoteWeightStrategy 计算附加权重。

**字段**：`candidateId`、`candidateDatacenter`、`startupFirst`、`electionInitiator`、`term`。

**工厂方法**：`forCandidate(candidateId, term)` 和 `forDatacenter(candidateId, datacenter, term)`。

## FailureRecord

### 职责

跟踪单个节点的故障状态，支持故障计数和自动移除判定。

**字段**：`nodeId`（String）、`firstFailureTime`（long，纳秒）、`failureCount`（AtomicInteger，线程安全）、`confirmed`（volatile boolean）。

**关键方法**：
- `incrementFailureCount()/incrementFailures()`：原子递增故障计数
- `getDurationNanos()`：计算从首次故障到现在的持续时间
- `shouldProposeRemoval(failureThreshold, confirmationNanos)`：当故障次数 >= 阈值且持续时间 >= 确认窗口时返回 true
- `reset()`：清零故障计数和确认状态

## LeaderRecord

### 职责

不可变的 term + leaderId 对，用于记录 Leader 历史。

**字段**：`term`（long）、`leaderId`（String），均为 final。

## 线程安全总结

- `RaftNode`：混合使用 `synchronized`、`volatile`、`ConcurrentHashMap`、`CopyOnWriteArrayList`、`ScheduledExecutorService`
- `ElectionTimeout`：`ThreadLocalRandom`，无锁
- `Term`：非线程安全，由调用方（RaftNode 的 synchronized 方法）保证
- `FailureRecord`：`AtomicInteger` + `volatile`，无锁
- `RaftGroup`：`volatile` 用于 leader 缓存，其余集合操作由调用方控制

## 扩展点

1. 通过 `StateMachine` 接口注入自定义命令处理逻辑
2. 通过 `VoteWeightStrategy` 控制投票权重
3. 通过 `GroupStrategy` 控制选举超时和心跳策略
4. 通过 `MembershipConfig` 配置成员变更行为
5. 通过 `HealthCheckStrategy`/`RegistryStrategy`/`ChangeValidationStrategy` 定制成员管理