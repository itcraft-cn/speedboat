# Speedboat 代码审查报告（2026-09-13）

> 范围：`src/main/java/cn/itcraft/speedboat` 全量 61 文件 / 6502 行。
> 规范：`spec.java.md` 1.2.0 + `review.java.md`（SpotBugs/PMD 规则映射）。
> 背景：本审查为 2026-07 全量审查后的复审，期间代码经历 P0/P1/P2 修复、权重选举改造、帧编解码修复、脑裂修复、锁竞态修复。

## 一、审查结论

| 维度 | 评价 |
|------|------|
| 修复落地情况 | 良好：上轮 P0~P2 均已修复并有针对性测试（NettyPipelineTest、三节点真实网络测试等） |
| 新发现问题 | 本轮新增发现：P0 × 1，P1 × 2，P2 × 若干（详见下文） |
| 总体评价 | 核心算法方向正确，但存在 **节点身份不稳定** 这一系统性缺陷，以及若干并发/资源细节问题 |

---

## 二、P0（必须立即修正）

### P0-1 锁续期线程池关闭后不可逆复用（RejectedExecutionException 风险）

- 位置：`lock/DistributedLockImpl.java:218-223`（stopRenewTask）+ `:111-113`（构造器创建单线程池）
- 问题：`stopRenewTask()` 调用 `renewExecutor.shutdown()`。`shutdown` 是**一次性不可逆**操作。
  若同一把锁先释放（触发 stopRenewTask → executor 已 shutdown），随后再次
  `tryLock` 成功再调 `startRenewTask()`，`renewExecutor.scheduleAtFixedRate` 将抛出
  `RejectedExecutionException`，租约续期中断，锁将因租约过期被他人抢占。
- 修正建议：续期任务用 `Scheduler` 单实例化成字段，stop 时 `cancel(true)` 对应
  ScheduledFuture，而不是 shutdown executor；或在 stopRenewTask 后重建 executor。

```java
// 现状（潜在崩溃路径）
private void stopRenewTask() {
    if (renewing.compareAndSet(true, false)) {
        renewExecutor.shutdown();          // 不可逆
        ...
    }
}
// 再次 startRenewTask → proposals on shutdown executor → RejectedExecutionException
```

---

## 三、P1（应当修正）

### P1-1 节点身份（nodeId）随机后缀 + "peer-" 前缀，跨进程不一致

- 位置：`util/NetworkUtils.java:75-89`（`generateNodeId(hostname, ip)` 使用 `RANDOM.nextInt(10000)`）、
  `Speedboat.java:154/202`（peer 节点用 `generateNodeId("peer", peerIp)`）
- 问题一：每次进程重启 nodeId 都会变（random 4 位），且不同节点对同一 peer
  生成出的 `peer-<ip>-<rand>` 后缀**互不相同**（各自进程独立随机）。
  节点 ID 是 Raft 的身份基石（votedFor / leaderId / matchIndex 的 key），
  身份漂移会导致：
  - A 节点投票给 B 的 `candidateId`（B 的真实 nodeId）与 A 的 `peerIds` 列表项
    （"`peer-ip-rand`"）**永远不相等**——成员表与实际身份脱节；
  - `failureRecords`、`nextIndex`、`matchIndex` 等按 ID 键控的状态失去稳定性；
  - 注册中心/成员变更按 ID 比对全部失效。
- 问题二：`Speedboat` 门面内 peerId 生成规则与自身 nodeId 规则不一致
  （"`peer-`" 前缀 vs hostname），制造了"同一节点两个身份"。
- 修正建议：nodeId 生成必须确定性（如 `hostname-ip`，或 IP 去掉随机后缀），peer 与自身使用同一生成函数，随机后缀仅用于检测冲突后分裂，不做常规成分。

### P1-2 RFT 日志未持久化，重启后安全性破坏

- 位置：`raft/RaftNode.java` 全部日志状态均为内存 `CopyOnWriteArrayList`；`LockStateMachine.snapshot/restore` 为空实现
- 问题：进程重启后 `term` 归零、`votedFor` 丢失、日志丢失。Raft 安全核心是
  "同一任期内同一张选票"，重启后可能在中断恢复期重复投票，导致脑裂双 Leader。
  对"master 选举"这一核心用途而言这是最高优先级的健壮性缺口。
- 修正建议：至少持久化 `currentTerm`、`votedFor`（单独小文件 / WAL pre-write），
  逐步补齐日志 WAL 与快照恢复（`StateMachine.restore` 已预留接口）。

### P1-3 raft 日志无 WAL/快照备份

- 说明：前提同 P1-2；即便不做完整 WAL，也必须优先处理 term/votedFor，否则成员变更（MemberChangeEntry）重启后丢失，集群成员表可能回退。

### P1-4 心跳路径日志洪水

- 位置：`raft/RaftNode.java:471-516`（`sendAppendEntries` 对每个 peer 打 3 条 INFO）
- 问题：心跳间隔默认 50ms、每 peer 3 条 info，单节点日志速率 > 60 条/秒，
  3 节点集群日志体量严重，且这些日志在生产中无排查价值。
- 修正建议：日志降级到 debug，仅在状态改变/错误时保留 info。

### P1-5 LogEntry 帧 maxFrame=1024 太小

- 位置：`transport/NettyTransport.java:90/161`（`LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4)`）
- 问题：`AppendEntriesRequest` 可携带多条日志（command 数据、成员变更），帧上限
  1KB 容易超限，触发解码失败 → 连接被关闭 → 反复重连。
- 修正建议：上限提升至可配置（如 1MB），并在发送侧做最大帧校验。

### P1-6 线程池命名/类型不达标（Executors 工厂方法）

- 位置：
  - `raft/RaftNode.java:182`：`newScheduledThreadPool(2)` 未使用 NamedThreadFactory
  - `membership/MembershipCoordinator.java:45`：`newSingleThreadScheduledExecutor()` 无名
  - `strategy/membership/impl/PassiveHealthCheckStrategy.java:23`：同上，且 `start()` 实际不启动任何任务，scheduler 永不使用却占用资源
- 规范引用：spec.java.md 线程管理章节"禁止 Executors 工厂 / 必须线程命名"
- 修正建议：统一改用 `NamedThreadFactory`；`PassiveHealthCheckStrategy` 删除无用 scheduler。

### P1-7 巨型方法 + 魔幻数值

- `raft/RaftNode.java` 1252 行（建议 ≤2000 但按照 spec 更优是拆分：成员变更 250+ 行应独立为 `MembershipProposer`）；`advanceCommitIndex` 内还有未使用的 `matchCount++` 死变量（:553-556）
- `Speedboat.java` 400 行，单机房/跨机房两个启动方法 90% 重复（见 dup 检测联动）

### P1-8 `System.out/err` 代码

- 位置：`cluster/ClusterTestRunner.java` 19 处（规范禁止 stdout/stderr）
- 说明：作为脚手架测试入口可放宽，但 commit 该类时建议统一为 SLF4J。

---

## 四、P2（建议改进）

| 编号 | 位置 | 问题 | 建议 |
|------|------|------|------|
| P2-1 | `raft/RaftNode.java:627-631` | `getGroup()` 返回 null 的占位实现 | 删除或实现，返回 null 属于 API 坑 |
| P2-2 | `raft/RaftNode.java:924-935` | `startMembershipChangeDetection()` 在构造器中调用时 `scheduler==null` 必然跳过——成员变更检测死代码路径 | 与 `MemoryCoordinator` 二选一，删除死路径 |
| P2-3 | `raft/RaftNode.java:333-351` | `handleAppendEntries` 对 COW 列表整体 clear+addAll，且会把**未冲突的已存在条目也替换**，O(n) 重建浪费 | 仅截断新 entries 中冲突段，prev 之后的直接追加 |
| P2-4 | `transport/NettyTransport.java:46` | `(CompletableFuture<RpcResponse>)(CompletableFuture<?>)` 三层强转 | 泛型接口重新设计（`CompletableFuture<R>` 传入 Function） |
| P2-5 | `lock/DistributedLockImpl.java:139/194` | 分布式锁热循环 `Thread.sleep(RETRY_INTERVAL_MS)`（100ms 粒度） | 可接受，但建议使用指数退避 |
| P2-6 | `raft/RaftNode.java:57` | `Executors.newScheduledThreadPool` 心跳与选举共用 scheduler，shutdown 时 `awaitTermination` 仅 1 秒 | 超时任务残留，建议 3-5 秒 |
| P2-7 | `lock/DistributedLockImpl.java:107` | 自建 `new ProtostuffSerializer()`，should inject/共享 | 与 `LockStateMachine` 里的 serializer 独立实例重复（见 dup 报告） |
| P2-8 | `strategy/membership/impl/PassiveHealthCheckStrategy.java:19` | `HashSet` + `synchronized` 混合 | 改用 `ConcurrentHashMap.newKeySet()` |
| P2-9 | `util/NetworkUtils.java` | `getNetworkInterfaces()` 耗时接口无缓存，`detectLocalIp` 每次 `Speedboat.start` 调用（可接受）但规范要求缓存 | 启动期缓存到 static 字段 |

---

## 五、上轮回访（2026-07 → 2026-09 修复核实）

| 上轮问题 | 状态 | 证据 |
|----------|------|------|
| startElection 脑裂 | ✅ 已修复 | `startElection` 加 `synchronized`（commit 1ac60fc） |
| waitForApply 锁竞态 | ✅ 已修复 | `isLockHeldBy && lastApplied >= targetIndex` 双条件 |
| broadcast NPE | ✅ 已修复 | `channels.isEmpty()` 检查 |
| 帧编解码 ByteBuf 错配 | ✅ 已修复 | `NettyPipelineTest` 回归保护 |
| advanceCommitIndex 权重语义 | ✅ 已修复 | matchedWeight vs requiredWeight |
| 逐pxepivot 心跳/peer 日志洪水 | ❌ 未修复（本轮 P1-4） | RaftNode:471-485 每次 3 info |
| 成员变更双实现 | ❌ 未修复（本轮 P2-2） | RaftNode.checkMembershipChanges 与 MembershipCoordinator 并存 |

## 六、安全审查

- 无硬编码密钥/密码 ✅
- 无 SQL（非 DB 项目）✅
- `length(4B)+crc32(4B)` 帧含 CRC 完整性校验 ✅
- 传输层**无认证/授权**（2026-07 结论仍成立，本轮回访）：任意可行网络可达主机伪造 AppendEntries 即可控制集群。唯一防线是 term 竞争。生产部署必须加 TLS + Channel 身份白名单（**P1**）。

## 七、修复优先级摘要

| 等级 | 问题数 | 代表问题 |
|------|--------|----------|
| P0 | 1 | 锁续期 executor 关闭后复用 |
| P1 | 7 | nodeId 身份漂移（最重）、无持久化、1KB 帧上限、日志洪水 |
| P2 | 9 | 死代码、COW 重建、强转、日志等 |
