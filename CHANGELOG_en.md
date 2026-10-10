# Changelog

All notable changes to Speedboat will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

> [中文](CHANGELOG.md) · [README](README_en.md) · [User Manual](MANUAL_en.md)

## [Unreleased 4] - 2026-10-10

### Added (full cross-datacenter cascading chain)

- **Cross-DC cascading parent group**: two-level Raft skeleton (intra-DC child group + cross-DC parent group); protocol P1/P2/P4 carries datacenter identity; datacenter priority weight as the hard uniqueness guarantee -- the parent-group quorum **aggregates its denominator per datacenter** (a datacenter cannot be bought with one vote), and the primary datacenter cannot win without one standby vote; the parent group is fully meshed with election-entry seat gating
- **CP/AP consistency dual mode**: `consistency.policy=cp` (default, strong consistency + unique leadership; full datacenter loss degrades to leaderless, `isMain` count always <= 1) / `ap` (degraded takeover after the peer stays unreachable past `consistency.degraded.timeout.ms`; brief dual leaders allowed during partition; entering/leaving degradation each emits a `CONSISTENCY AUDIT` entry); chosen at startup, constant for the whole lifecycle
- **Manual datacenter priority promote/restore API**: `promoteDatacenter` / `restoreDefaultPriorities` / `getPrioritySnapshot` (operational fallback) -- table swap (local weight = sum(others) + 1, so one's own vote wins for any number of datacenters) + `promote.term.leap` term leap + `PRIORITY_CHANGE` log replication + standalone `parent-priority.properties` persistence; reachability gate (rejected if any peer parent endpoint is reachable); **effective in CP only**, ignored under AP with an `op=IGNORED` audit entry
- **LeadershipPolicy family**: `crossdc.leadership=peer` (default) / `dominant` (DR: PreVote sticky yield -- yield only when the candidate's datacenter weight is strictly greater -- plus heartbeat-freeze exception for takeover probes); weight binding deferred to `ParentGroupBuilder` assembly time
- **Two-level observation API**: `isIntraLeader`/`isParentLeader`/`getIntraTerm`/`getParentTerm`/`getIntraLeaderId`/`getParentLeaderId` etc.; `isMain() == isIntraLeader() && isParentLeader()`
- **Orchestration tooling**: `real-crossdc3-test.sh` for a real 3/2/2 nine-node CP/AP deployment; `vm-crossdc-test` forks assertions by `SB_CONSISTENCY`; vboxnet0 network-partition scenario; same-IP multi-process identity pinning

### Fixed

- **Parent-group quorum denominator aggregated per datacenter**: corrected node-level denominator inflation that prevented baseline elections; parent group switched to full mesh + election-entry seat gating
- **H1 closure for the promote weight formula**: `computePromotedWeights` now uses `local = sum(other datacenter weights) + 1`, so one's own vote always wins regardless of datacenter count (incl. three datacenters)
- **dominant weight binding time**: deferred to `ParentGroupBuilder` assembly, fixing null-weight identity binding that froze the heartbeat forever
- **Two review rounds**: 001 incremental (1 high + 4 medium + L1-L8, incl. splitting `ParentGroupBuilder`/`ParentGroupBundle`/`PromoteGateway` out of `Speedboat`, 1255 -> 978 lines); 002 full (H1 `StateMachine.canSnapshot()` contract / H2 `peerIds` race / C-2 `Term.current` volatile / M-1..M-7 incl. CRC coverage extended to type header + payload, tryLock switched to nanoTime / Q and L series)
- **Five items confirmed by external review (deepseek/hunyuan)**: tryLock hot-path attempt logs INFO->DEBUG; shared lease-renewal scheduler singleton (per-lock 100 threads -> 1 global thread); `ctx.log` CopyOnWriteArrayList->ArrayList + `tailFrom` by-index slicing (O(n*m) heartbeat replication and O(n^2) append collapsed); `MmapRaftStore.scanRecords` skips damaged CRC frames with a warning (writeOffset back-filled, so valid frames after a damaged segment are no longer overwritten away); `LogEntry` null defense / `NodeEndpoint.parse` nodeId unification / `truncateLogEntriesUntil` inclusive annotation / `lastApplied` two-watermark semantics note
- **logback-core aligned to 1.2.13**: fixed dependabot mismatched upgrade

### Changed

- **Inline fully-qualified class names globally converged to imports** (src/main + src/test, two batches for the standard library and cn.itcraft)
- **Removed `MembershipCoordinator`** (legacy superseded by the membership strategy family) and its integration test

### Verified

- Unit: full suite 616 green (622 - 6, M-4 dropped the integration test)
- Real machines: 3/2/2 nine-node across three datacenters (51/53/55 | 108/109/110 | 174/175/176) -- **CP 41/41 + AP 48/48 all PASS**; six-node re-verification 61/61

## [Unreleased 3] - 2026-09-22

### Added (first batch of SOFAJRaft/Ratis takeaways)

- **expectedNextIndex fast fallback** (protocol extension shared by SOFAJRaft/Ratis): `AppendEntriesResponse` gains an `expectedNextIndex` hint; all three follower failure paths (log too short -> firstLogIndex+1 / conflicting term -> first entry of the term range) return the fallback point in one step; the leader still clamps adoption to <= current-1 (split-log protection, never regresses)
- **Lock-op idempotent dedup** (SOFAJRaft RequestMap discipline): `LockOpGateway` adds an LRU cache of requestId -> verdict (cap 4096); a "accepted but reply lost" retry idempotently replays the original entryIndex instead of re-entering the log; deterministic rejections are cached too; conditional non-Leader rejections are not cached
- **Membership bookkeeping cleanup** (SOFAJRaft memory discipline): `MembershipManager.processMemberChange` clears lastResponseNanos/votesReceived/prevotesReceived on both ADD and REMOVE (nextIndex/matchIndex/failureRecords were already covered)
- **BoundedCache** (raft/util): hard cap + access-order LRU + thread-safe; used by the lock-op idempotency cache (reuse for every "bookkeeping map keyed by unbounded input" scenario)
- **RaftNodeMicrometerListener** (Ratis TlsConf/metrics layered pluggability): speedboat.raft.term/commitIndex/lastApplied/lastLogIndex/leader/followerMatch(peer tag) metric families; micrometer-core `<optional>` dependency -- zero contact if not pulled in (core stays free of metrics libs)

### Verified

- Unit: 532 green (+4 BoundedCacheTest / +3 LockOpGatewayDedupTest / +4 fast-fallback semantics)

## [Unreleased 2] - 2026-09-18

### Added (P4 three persistence tiers)

- **RaftStore three-tier strategy**: `NopRaftStore` (test baseline) / `InMemoryRaftStore` (**default mem**: recoverable in-process) / `MmapRaftStore` (very stable: 128MB single-file mmap WAL -- "reattach after loss", default dir `./speedboat-data/{nodeId}/`, file name/size configurable)
- **LockStateMachine checkpointing**: snapshot/restore (crc + temp-rename atomic replace), applied every 1024 entries or once at shutdown; epoch/holder/lease survive restarts faithfully
- **Read-back chain**: RaftNodeImpl.start() wires restoreTerm (existing) + `restoredLogEntries()` WAL rebuild + checkpoint lastApplied skip + shutdown snapshot fallback
- **Config**: `raft.persistence=mmap|mem|none`, `raft.persistence.dir`, `raft.mmap.size.mb`, `raft.mmap.file.name`

### Fixed

- **defect-20260918-01 eliminated** (restarted-replica epoch diverged from long-running nodes): mmap re-attach WAL/checkpoint + "list is total" TRUNC marker closure; real-network loopback re-verification -- kill -> restarted replica takes over migration grant epoch=2 consistent with the long-running one
- MmapRaftStore frame.flip defect (marker/normal frame bodyLen=0 empty frame confused the scanner) + MEMBER_CHANGE frame body length validation and op byte fix

### Verified

- Unit: full suite 519 green (+6 MmapRaftStoreTest contract, +3 LockStateMachineCheckpointTest)
- Real network: loopback three-JVM mmap mode -- restarted replica `restored term + rebuilt log entries=98`; cross-restart migration takeover epoch=2 consistent on both replicas

## [Unreleased] - 2026-09-18

### Added (important)

- **General-purpose named distributed lock**: primacy and locking are decoupled -- any member may acquire any named lock (no longer Leader-only); possession follows log-consensus success; `tryLock/unlock/renew` keep the original API, plus `LockHandle.getEpoch()` fencing token
- **Lock migration semantics**: holder loses contact -> lease expires -> fair takeover race; **non-preemptive** (never steals from a live holder); decided only at state-machine apply (log total order guarantees mutual exclusion)
- **LockOp forwarding protocol**: `LockOpRequest/Response` RPC (message types 9/10); followers forward lock ops to the Leader for proposal
- **Verdict table**: `LockOpResult` (GRANTED/DENIED + epoch), requestId idempotent dedup
- **Config**: `raft.max.log.size` (default 4096)

### Fixed

- **defect-20260918-01**: in-memory log cap 10 -> 4096 (the sole basis on which a restarted replica rebuilds its decision state; over-truncation caused epoch divergence across replicas); root fix deferred to snapshot/persistence (P4)
- A rejected renew (RENEW_LOST) stops renewal immediately (better to stop wrongly than to dual-hold)

### Verified

- Unit: full suite 510 green (added NamedLockMultiHolderTest with 5 semantic cases)
- Real network (three real nodes + local three-JVM loopback): three locks with three holders coexisting and converging cluster-wide, non-Leader forwarding acquires, migration takeover grant epoch=2 consistent on both long-running replicas

## [1.0.1] - 2026-07-13

### Added

- **README_cn.md**：中文版 README，方便中文用户快速上手
- **README.md 英文版**：重写为英文版 README，面向国际用户
- **CHANGELOG_cn.md**：中文版变更日志
- **MANUAL_cn.md**：中文版使用手册
- **MANUAL.md 英文版**：重写为英文版使用手册

### Changed

- **README.md**：从中文改为英文，与 README_cn.md 形成中英文双文档体系
- **README.md**：文档引用改为中英文对应（MANUAL.md / MANUAL_cn.md, CHANGELOG.md / CHANGELOG_cn.md）
- **README_cn.md**：文档引用改为中英文对应（MANUAL_cn.md / MANUAL.md, CHANGELOG_cn.md / CHANGELOG.md）

---

## [1.0.0] - 2026-07-09

### Added

#### 极简 API 重构

- **SpeedboatConfigProvider 接口**：配置接口化，支持用户自定义配置源（JSON/YAML/数据库）
- **PropertiesConfigProvider**：默认实现，零依赖，解析 Properties 文件
- **NetworkUtils**：自动检测 hostname/IP，生成 nodeId（格式：`hostname-ip-random`）
- **集群视角配置**：用户只需配置 `datacenter + nodes`，系统自动匹配本节点
- **极简门面 API**：
  - `Speedboat.start(config)` - 一行启动
  - `Speedboat.stop()` - 停止并释放资源
  - `Speedboat.isMain()` - 判断是否主节点
  - `Speedboat.getLeaderId()` - 获取主节点 ID
  - `Speedboat.getTerm()` - 获取当前任期
  - `Speedboat.getNodeId()` - 获取本节点 ID
  - `Speedboat.getDatacenterId()` - 获取机房 ID
  - `Speedboat.isRunning()` - 检查运行状态

#### 两套选举超时配置

- **机房内选举超时**：`getIntraDatacenterElectionTimeoutMin/Max()`（默认 1000-2000ms）
- **机房间选举超时**：`getCrossDatacenterElectionTimeoutMin/Max()`（默认 3000-5000ms）
- **Properties 配置**：
  - `election.intra.timeout.min/max`
  - `election.cross.timeout.min/max`

#### 自动模式判断

- **单机房扁平模式**：`nodes.size() == 1`，所有节点平等选举，使用 intra 超时
- **跨机房级联模式**：`nodes.size() > 1`，本机房选举使用 intra 超时，跨机房选举使用 cross 超时

#### 配置文件示例

- **config.properties**：单机房示例
- **config-cross-dc.properties**：跨机房示例

#### 使用示例

- **SimpleExample.java**：极简使用示例

### Changed

- **Speedboat.java**：
  - 重构为单例模式 + 静态方法
  - 添加 `crossDatacenterMode` 自动判断
  - 分离 `doStartSingleDatacenterMode()` 和 `doStartCrossDatacenterMode()`
  - 移除旧 API（`init/destroy/initCascade`）

### Removed

- **SpeedboatTest.java**：使用旧 API 的测试文件
- **ClusterExample.java**：使用旧 API 的示例文件
- **ClusterExampleMain.java**：使用旧 API 的示例文件
- **RealClusterExample.java**：使用旧 API 的示例文件
- **ClusterElectionIntegrationTest.java**：使用旧 API 的集成测试

### Fixed

- 修复测试编译错误（方法名冲突、旧 API 调用）

---

## [0.9.0] - 2026-07-08

### Added

#### Raft 核心实现

- **RaftNode**：Raft 节点核心实现，支持 Leader/Follower/Candidate 状态转换
- **ElectionTimeout**：随机选举超时，防止选举冲突
- **LogEntry**：Raft 日志条目
- **Term**：任期管理

#### RPC 通信

- **NettyTransport**：基于 Netty 的 RPC 传输层
- **ProtostuffSerializer**：Protostuff 序列化器
- **CustomSerializer**：自定义序列化包装器
- **NodeEndpoint**：节点地址封装

#### 级联选举

- **RaftGroup**：Raft 组管理器，支持级联架构
- **GroupStrategy**：分组策略接口
- **DefaultGroupStrategy**：默认分组策略实现

#### 成员变更

- **MembershipCoordinator**：成员变更协调器
- **HealthCheckStrategy**：健康检测策略接口
- **PassiveHealthCheckStrategy**：被动健康检测实现
- **RegistryStrategy**：注册中心策略接口
- **ChangeValidationStrategy**：变更验证策略接口

#### 投票权重

- **VoteWeightStrategy**：投票权重策略接口
- **DefaultVoteWeightStrategy**：默认投票权重实现

#### 测试

- **单元测试**：核心组件测试覆盖
- **集成测试**：集群选举测试
- **ClusterFailoverTest**：故障转移测试

---

## [0.1.0] - 2023-02-01

### Added

- 项目初始化
- 基础 Raft 概念验证