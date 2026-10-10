# 变更日志

所有重要的变更都会记录在此文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)。

> [English](CHANGELOG_en.md) · [README](README.md) · [使用手册](MANUAL.md)

## [Unreleased 4] - 2026-10-10

### Added（跨机房双层级联全链路）

- **跨机房级联父组**：双层 Raft 骨架（子组机房内 + 父组跨机房），协议 P1/P2/P4 携带机房标识；机房优先级权重作唯一性硬保证——父组法定多数**按机房聚合分母**（机房不能被一票买通），主机房缺一备房票不成主；父组全网互联并补齐选举入口席位门控
- **CP/AP 一致性双模式**：`consistency.policy=cp`（缺省，强一致唯一性，整机房失联退化无主，`isMain` 恒 ≤ 1）/ `ap`（对侧失联超 `consistency.degraded.timeout.ms` 降级接管，分区期间允许短暂双主，进出降级各落 `CONSISTENCY AUDIT` 审计）；启动时选定、全生命周期恒定
- **人工提升/回退机房优先级 API**：`promoteDatacenter` / `restoreDefaultPriorities` / `getPrioritySnapshot`（运维兜底）——换表（本房权重 = Σ(其他)+1，任意机房数恒自投即成主）+ `promote.term.leap` term 跃升 + `PRIORITY_CHANGE` 日志复制 + `parent-priority.properties` 独立持久化；连通性闸门（对侧任一父组端点可达即拒）；**仅 CP 生效**，AP 下忽略并落 `op=IGNORED` 审计
- **LeadershipPolicy 领袖优先级策略族**：`crossdc.leadership=peer`（缺省，对等）/ `dominant`（灾备：PreVote sticky 让位——候选者机房权重严格大于现任才让 + 心跳冻结例外的夺主探测）；权重绑定延至父组装时
- **双层观测 API**：`isIntraLeader`/`isParentLeader`/`getIntraTerm`/`getParentTerm`/`getIntraLeaderId`/`getParentLeaderId` 等；`isMain() == isIntraLeader() && isParentLeader()`
- **编排工具**：`real-crossdc3-test.sh` 三机房 3/2/2 九节点 CP/AP 实机编排；`vm-crossdc-test` 按 `SB_CONSISTENCY` 分叉断言；vboxnet0 网络分区专项编排；同 IP 多进程身份钉定

### Fixed

- **父组法定多数按机房聚合分母**：修正节点级分母虚增致基线选不出主；父组改全网互联 + 选举入口席位门控
- **promote 权重公式 H1 闭环**：`computePromotedWeights` 改 `本房 = Σ(其他机房权重) + 1`，任意机房数（含三机房）恒自投即成主
- **dominant 权重绑定时机**：延至 `ParentGroupBuilder` 组装时绑定，修正早绑 null 权重恒等恒冻结
- **两轮代码审查修复**：001 增量（1 高 4 中 + L1-L8，含 `Speedboat` 拆分出 `ParentGroupBuilder`/`ParentGroupBundle`/`PromoteGateway`，1255→978 行）；002 全量（H1 `StateMachine.canSnapshot()` 契约 / H2 `peerIds` 竞态 / C-2 `Term.current` volatile / M-1..M-7 含 CRC 覆盖扩为类型头+payload、tryLock 改 nanoTime / Q、L 系列）
- **外部点评（deepseek/hunyuan）复核五项**：tryLock 热路径 attempt 日志 INFO→DEBUG；续租调度池共享单例（per-lock 100 线程 → 全局 1 线程）；`ctx.log` CopyOnWriteArrayList→ArrayList + `tailFrom` 按下标切除（O(n·m) 心跳复制与追加 O(n²) 收敛）；`MmapRaftStore.scanRecords` CRC 坏帧告警跳过（writeOffset 回填，坏段后有效帧不再被覆写丢失）；`LogEntry` null 防御 / `NodeEndpoint.parse` nodeId 统一 / `truncateLogEntriesUntil` inclusive 标注 / `lastApplied` 两套水位口径注释
- **logback-core 对齐 1.2.13**：修复 dependabot 错配升级

### Changed

- **内联全限定类名全局收敛为 import**（src/main + src/test，标准库与 cn.itcraft 两批）
- **删除 `MembershipCoordinator`**（被 membership 策略族取代的遗留件）及集成测试

### Verified

- 单元：全量 616 绿（622 − 6，M-4 删集成测试）
- 实机：三机房 3/2/2 九节点（51/53/55 | 108/109/110 | 174/175/176）——**CP 41/41 + AP 48/48 全 PASS**；六机复验 61/61

## [Unreleased 3] - 2026-09-22

### Added（SOFAJRaft/Ratis 可吸收点第一批落地）

- **expectedNextIndex 快速回退**（SOFAJRaft/Ratis 同款协议扩展）：`AppendEntriesResponse` 新增 `expectedNextIndex` 失败提示；follower 三失败路径（日志过短→firstLogIndex+1 / 同岗任期冲突→任期区间首条）一步给出回退点；leader 侧采纳前仍强制 ≤ current-1 钳制（分割日志保护不回退）
- **锁 op 幂等去重**（SOFAJRaft RequestMap 纪律）：`LockOpGateway` 增加 requestId→裁决结果 LRU 缓存（上界 4096）；"受理成功但应答丢失"重试幂等回放原 entryIndex，不再重复入日志；确定性拒绝同样缓存，非 Leader 条件性拒绝不缓存
- **成员簿记表清理**（SOFAJRaft 内存纪律）：`MembershipManager.processMemberChange` ADD/REMOVE 双向同步清理 lastResponseNanos/votesReceived/prevotesReceived（nextIndex/matchIndex/failureRecords 原已覆盖）
- **BoundedCache**（raft/util）：硬上界 + access-order LRU + 线程安全；用于锁 op 幂等缓存（后续所有"对不可控入参建簿记表"场景复用）
- **RaftNodeMicrometerListener**（Ratis TlsConf/metrics 分层式外挂）：speedboat.raft.term/commitIndex/lastApplied/lastLogIndex/leader/followerMatch(peer tag) 指标族；micrometer-core `<optional>` 依赖，不引即零接触（核心零 metrics 库依赖保持）

### Verified

- 单元：532 全绿（+4 BoundedCacheTest / +3 LockOpGatewayDedupTest / +4 快速回退语义）

## [Unreleased 2] - 2026-09-18

### Added（P4 三档持久化）

- **RaftStore 三档策略**：`NopRaftStore`（测试基线） / `InMemoryRaftStore`（**默认 mem**：进程内可恢复，CSV 快速） / `MmapRaftStore`（极稳定：128MB 单文件 mmap WAL——"丢了挂回"，默认目录 `./speedboat-data/{nodeId}/`、文件名/大小可配）
- **LockStateMachine 状态机检查点**：snapshot/restore（crc + temp·rename 原子替换），applied 每 1024 条触发或停机一次；epoch/holder/租约跨重启保真
- **读回链**：RaftNodeImpl.start() 接线 restoreTerm（既有） + `restoredLogEntries()` WAL 重建 + 检查点 lastApplied 跳位 + shutdown 快照兜底
- **配置**：`raft.persistence=mmap|mem|none`、`raft.persistence.dir`、`raft.mmap.size.mb`、`raft.mmap.file.name`

### Fixed

- **defect-20260918-01 消除**（重启副本 epoch 与长驻分叉）：mmap 挂回 WAL/检查点 + "列表即全量" TRUNC 标记收口；真网 loopback 复核——kill→同身份重启副本接管迁移 grant epoch=2 与长驻一致
- MmapRaftStore 帧.flip 缺陷（marker/normal frame bodyLen=0 空帧致扫描懵收口）+ MEMBER_CHANGE 帧体 Len 实校与 op 字节修正

### Verified

- 单元：全量 519 绿（+6 MmapRaftStoreTest 契约、+3 LockStateMachineCheckpointTest）
- 真网：loopback 三 JVM mmap 模式——重启副本 `restored term + rebuilt log entries=98`、跨重启迁移接管 epoch=2 双副本一致

## [Unreleased] - 2026-09-18

### Added（重要）

- **通用分布式命名锁**：主与锁概念分离——任意成员可申请任意名目锁（不再限 Leader），日志共识成功即持有；`tryLock/unlock/renew` 保持原 API，新增 `LockHandle.getEpoch()` fencing token
- **锁迁移语义**：持有者失联 → 租约到期 → 公平竞争接管；**非抢占**（不抢存量持有者）；判定只发生在状态机 apply（日志全序保证互斥）
- **LockOp 转发协议**：`LockOpRequest/Response` RPC（消息类型 9/10），follower 锁操作转发 Leader propose
- **判定结果表**：`LockOpResult`（GRANTED/DENIED + epoch），requestId 幂等去重
- **配置项**：`raft.max.log.size`（默认 4096）

### Fixed

- **defect-20260918-01**：内存日志上限 10 → 4096（重启副本重建判定状态的唯一依据，截断过深导致 epoch 跨副本分叉）；根治待快照/持久化（P4）
- 续期被拒（RENEW_LOST）立即停续期（宁可误停不可双持）

### Verified

- 单元：全量 510 绿（新增 NamedLockMultiHolderTest 6 条语义用例）
- 真实网络（三节点实网 + 本机三 JVM loopback）：三锁三持有者并存全网收敛、非 Leader 转发持锁、失联迁移 grant 双长驻副本一致 epoch=2

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
