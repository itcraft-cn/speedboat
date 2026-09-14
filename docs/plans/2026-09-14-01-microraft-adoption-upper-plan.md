# 实施计划：MicroRaft 经验吸收（上策）— Actor 单线程化 + 架构契约化

> 日期：2026-09-14
> 前置：docs/analysis/microraft-experience-adoption-20260914-001.md（对标分析）
> 决策：用户选定**上策**，核心诉求：Actor 单线程模型（MP + MPSC + SC）为一等公民
> 本文档不含代码，仅架构与验收标准；预计 10~15 天，分 5 个.phase 独立提交

---

## 〇、总原则（MicroRaft 三条铁律的本地化）

1. **单线程铁律**：Raft 状态机所有变更（角色/term/日志/成员/commitIndex）只允许在 **raft 单消费者线程** 上发生。其它线程（Netty IO、定时器、调用方）只允许做两件事：入队 / 读 volatile 快照。
2. **Transport 铁律**：`send` 非阻塞、不抛异常；建连/写失败全部折叠为"该请求无响应"（CompletableFuture 失败），由算法侧超时/重置兜底。传输层永不持有 raft 锁。
3. **契约铁律**：先写接口契约 Javadoc（谁在哪个线程调用、必须不做何事），再写实现。持久化类接口在本阶段即接口化（Nop 兜底），避免未来启用时零接口变动的承诺落空。

---

## Phase A：Actor 单线程化（MP + MPSC + SC）【本轮实施】

### 目标形态

```
MP（多生产者）                    MPSC                    SC（单消费者）
- Netty childIO 线程：入站消息  →  无锁队列  →   raft 单线程依次消费：
- transport 响应回调线程：ACK     (消/费)          * handleRequestVote / handleAppendEntries
- ScheduledExecutor 定时到期                       * handle...Response（matchIndex/commit 推进）
  （心跳/选举超时/成员检测）                        * transitionTo / startElection / propose
                                                   * 定时任务自续期（心跳 tick、选举超时重置）
```

### 设计要点

1. 新增 `raft/executor/` 包（对标 MicroRaft executor/）：
   - `RaftNodeExecutor` 接口：3 方法契约（execute / schedule(delay) / shutdown），Javadoc 写明"同一时刻最多执行 1 个任务，任务间自动 happens-before，因此实现内部不得提交阻塞性任务"。
   - `DefaultRaftNodeExecutor` 首选实现：**单线程 ScheduledExecutorService**（与 MicroRaft 一致，功能上等价 MPSC 语义，零新增依赖）。
   - 备选 `MpscRaftNodeExecutor`（JCTools MpscArrayQueue + drain 循环 + LockSupport 定时 park）： LockSupport park；确认后将 jctools-core 3.4.0 加入依赖。
2. RaftNode 接线改造：
   - 构造期注入 executor（Builder 提供，默认 Default 实现）。
   - 传输 handler 注册处做"线程跳转包装"：`setRequestVoteHandler(msg -> executor.execute(() -> handleRequestVote(msg)))`；完成块同理（including AppendEntriesResponse 回呼）。
   - 全部成员/定时任务改为 `executor.schedule(...)`，在 raft 线程上执行并自续期。
   - 拆除全部 `synchronized`（约 12 处）；ConcurrentHashMap 允许保留（多线程读快照无害），但职责收敛为"SC 写、MP 读"。
3. Transport 异步化（本 phase 的前置必要条件）：
   - `connectTo` 改为发起异步 `Bootstrap#connect`，成功回调再 `writeAndFlush`；CONNECT 超时通过 `ChannelFuture#await` 移除，改由 Netty option `CONNECT_TIMEOUT_MILLIS`。
   - send 三方法：Channel 未就绪 → 异步建连 + 回调写库；失败 → 立即 complete 失败占位响应（语义与当前"completedFailure"一致）。
   - 帧解码上限 `1024` → `1MB` 常量（fix 自评 P1-5），编解码共用一套 pipeline 常量。
4. 日志降噪（fix 自评 P1-4）：心跳/AppendEntries 轮询路径全部 `info → debug`；仅角色变更、term 变更、成员变更保留 info。

### 验收

- `mvn test` 496 项全绿（存量回归）。
- 新增 `RaftNodeExecutorContractTest`：并发 2000 条消息乱序投递 + 单线程断言（运行线程名必须等于 executor 线程名）+ 无 synchronized（spotbugs/手动核对）。
- 新增"心跳不阻塞"验证：模拟 connectTo 阻塞 1s 期间心跳 tick 正常出队（体现 SC 不被 MP 建连卡死）。

---

## Phase B：API / Impl 分层 + handler/task 拆包【次轮】

1. `RaftNode` 降为纯接口（javadoc 契约 + 线程模型说明），实现改名 `RaftNodeImpl`，对外 `RaftNode.builder()` 返回接口。
2. 拆出 `handler/`（VoteRequestHandler / AppendEntriesHandler / HeartbeatHandler）与 `task/`（HeartbeatTask 自续期、ElectionTimeoutTask、MemberCheckTask），消灭 1253 行上帝类；RaftNodeImpl 以 public 方法门面供 handler 调用（对标 MicroRaft 门面+协作者模式）。
3. `getGroup()` 落地或移除（当前返回 null 的半成品）。
4. 验收：`getEntryAt` O(n) → O(1)（下标直接命中 ArrayDeque/数组环形日志）；测试全绿；类圈复杂度下降（PMD 复核）。

---

## Phase C：选举正确性三件套【第三轮】

1. **check-quorum**：Leader 维护每 peer 最近心跳响应时间戳；周期任务检查"多数派最近响应超时"→ 主动降级 follower。（对标 MicroRaft quorumResponseTimestamp）
2. **PreVote + stickiness**：选举超时先发 PreVote（不涨 term）；得多数预票后进正式选举；合法 leader 心跳存续时拒绝预/正票。PreVoteRequest/Response 消息 + serializer typeId 登记。
3. **NotLeaderException**（带建议 leader 字段）落地于锁与 propose 路径。
4. 验收：新增分区/恢复 IT——老 leader 孤立后必须自降级；恢复后 term 不震荡、不干扰新任期。

---

## Phase D：状态与持久化接口前置【第四轮】

1. `persistence/RaftStore` 接口（9 方法契约，对标 MicroRaft 分级 flush 排布） + `NopRaftStore` 兜底；RaftNode 写路径全部经 store（当前为 Nop，行为不变）。
2. RaftState 字段标注 `[PERSISTENT]/[NOT PERSISTENT]`；`Term` 改造为不可变 `TermState` 对象原子替换（volatile）。
3. `maxLogSize` 截断改为"契约式收缩"（保证日志匹配属性与快照落差风险有说明），一批决定写入 MEMORY.md。
4. 验收：`InMemoryRaftStore`（测试专用）可完整存取/恢复 term、log；重启节点可据此恢复到一致视角（单元级 Mock 验证，不做真实磁盘）。

---

## Phase E：Transport 契约强化与可观测性【第五轮】

1. Transport 契约 Javadoc 落锁：send 非阻塞、不抛异常、丢包折叠为超时语义；`maxFrame`/背压/可选认证位常量化确认。
2. `RaftNodeReport` 最小版（role/status/term/members/follower matchIndex/最近响应时间）+ `ReportListener` 外挂注册；每 10s 周期发布 + 事件触发发布双通道。
3. 验收：端到端三进程 HostClusterNode 回归（V1/V2/V4/V5）+ 编写 metrics 输出示例（纯日志类 listener，不入 pom 额外依赖）。

---

## 里程碑与提交节奏

| Phase | 提交 | 风险 | 预计 |
|-----------|---|---|---|
| A Actor 单线程化 | refactor(actor) | 中（主线程模型切换） | 2~3 天 |
| B API/Impl 分层 | refactor(layering) | 中（大规模移动） | 3~4 天 |
| C 选举三件套 | feat(raft-election) | 中（新协议消息） | 2~3 天 |
| D Store 接口前置 | feat(persistence) | 低（Nop 兜底） | 2~3 天 |
| E 观测/传输契约 | feat(observability) | 低 | 1~2 天 |

### 回滚策略

每 phase 独立 commit；Phase A 若全量回归不过，保留 `synchronized` 暂行版分支 + 单线程化二分定位（优先怀疑：定时任务在两处 schedule、transport 响应回调漏包装两个点）。
