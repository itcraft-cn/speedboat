# SOFAJRaft/Ratis 吸收第一批（expectedNextIndex / 幂等去重 / 表清理 / Micrometer 外挂）

> 日期：2026-09-22 · commit c51b784 · 测试 532 全绿（基线 522）

| # | 吸收点 | 来源 | 改动 | 验证 |
|---|---|---|---|---|
| 1 | expectedNextIndex 快速回退 | SOFAJRaft/Ratis | AppendEntriesResponse +4th ctor/字段；RaftLogStore.conflictNextIndex/getFirstLogIndex；InboundAppendHandler 2 失败路径填充；ReplicationPump 采纳+钳制 | RaftNodeLogReplicationTest +4 |
| 2 | 表清理（内存纪律） | SOFAJRaft 反面教训 | processMemberChange ADD→lastResponseNanos 初始化；REMOVE→lastResponseNanos/votesReceived/prevotesReceived 清理 | 既有 522 回归 |
| 3 | requestId 幂等去重 | SOFAJRaft RequestClosure | LockOpGateway dedupCache（BoundedCache LRU 上界 4096），LockOpRequest 增 requestId ctor；RaftNodeImpl.handleLockOp 包内门面 | LockOpGatewayDedupTest +2 |
| 4 | BoundedCache | tiny-rules LruCache 对照 | raft/util 新类：access-order LRU+硬上界+synchronized | BoundedCacheTest +4 |
| 5 | Micrometer 外挂 | Ratis metrics-* 分层 | pom micrometer-core 1.9.17 <optional>；metrics/RaftNodeMicrometerListener（term/commitIndex/lastApplied/lastLogIndex/leader/followerMatch(peer tag) 指标族，动态增删） | 编译验证（运行时注册需应用侧引入 micrometer-core） |

注意点：
- expectedNextIndex 采纳强制 ≤ current-1（同索引不同 term 分割日志保护仍依赖逐条语义兜底）
- 幂等缓存只收"确定性结果"（受理成功/畸形包拒绝），非 Leader 条件性拒绝不缓存
- leader gauge nodeId tag 以首份报告为准（nodeId 启动期静态，一次性注册）
