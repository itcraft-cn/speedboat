# 日志复制设计文档

## 概述

实现 Raft 日志复制机制，用于同步主节点标识（Leader ID + Term）。支持多数确认语义，内存存储，保留最近 N 条历史记录。

## 需求

### 核心需求
- 同步主节点标识（Leader ID + Term）
- 内存存储，不持久化到磁盘
- 保留最近 10 条历史（可配置）
- 多数确认后才认为同步成功
- 集群间不同步

### 非需求
- 不支持业务数据复制
- 不支持跨集群同步
- 不支持持久化

## 数据结构

### LogEntry

```java
public class LogEntry {
    private final long index;      // 日志索引（全局递增，从1开始）
    private final long term;       // 产生该条目的任期
    private final String leaderId; // 该任期选出的 Leader
}
```

### RaftNode 新增字段

| 字段 | 类型 | 说明 |
|------|------|------|
| log | List\<LogEntry\> | 内存日志（最近 N 条） |
| commitIndex | volatile long | 已提交的最高索引 |
| lastApplied | volatile long | 已应用到状态机的最高索引 |
| nextIndex | Map\<String, Long\> | 每个 Follower 的下一个发送索引 |
| matchIndex | Map\<String, Long\> | 每个 Follower 已匹配的最高索引 |
| maxLogSize | int | 最大保留日志条数，默认 10 |

### 配置项

```java
SpeedboatConfig:
    - maxLogSize: int = 10  // 可通过 Builder 配置
```

## RPC 扩展

### AppendEntriesRequest

```java
public class AppendEntriesRequest {
    private final long term;              // Leader 任期
    private final String leaderId;        // Leader ID
    private final long prevLogIndex;      // 前一条日志的索引
    private final long prevLogTerm;       // 前一条日志的任期
    private final List<LogEntry> entries; // 日志条目（空列表表示心跳）
    private final long leaderCommit;      // Leader 的 commitIndex
}
```

### AppendEntriesResponse

```java
public class AppendEntriesResponse {
    private final long term;        // 响应者的当前任期
    private final boolean success;  // 是否匹配 prevLogIndex/prevLogTerm
    private final long matchIndex;  // Follower 已匹配的最高索引
}
```

### 向后兼容

- 保留 `HeartbeatRequest/Response`
- `handleHeartbeat()` 内部调用 `handleAppendEntries(entries=empty)`
- 现有心跳逻辑不受影响

## 核心流程

### Leader 日志复制流程

#### 发送日志（sendAppendEntries）

```
1. 遍历每个 Follower，获取 nextIndex[peerId]
2. 构造 AppendEntriesRequest:
   - prevLogIndex = nextIndex - 1
   - prevLogTerm = log[prevLogIndex].term（若 prevLogIndex=0 则 term=0）
   - entries = log.subList(nextIndex, log.size)
   - leaderCommit = commitIndex
3. 通过 transportLayer 发送 RPC
4. 处理响应：
   - 若 success=true: 更新 matchIndex[peerId], nextIndex[peerId] = matchIndex + 1
   - 若 success=false 且响应任期 > 当前任期: 转为 Follower
   - 若 success=false: nextIndex[peerId] = response.matchIndex + 1（快速回退）
```

#### 推进 commitIndex

```
1. 找到最大的 N，满足:
   - log[N].term == 当前任期
   - matchIndex 中超过半数 >= N
2. 若 N > commitIndex: commitIndex = N
```

#### 心跳触发

```
- 定时任务调用 sendAppendEntries(entries=empty)
- 空条目仍携带 prevLogIndex/prevLogTerm，用于日志匹配验证
```

### Follower 处理流程

#### handleAppendEntries(request)

```
1. 若 request.term < currentTerm: 拒绝，返回 (term=currentTerm, success=false, matchIndex=0)

2. 若 request.term > currentTerm: 
   - 更新 currentTerm = request.term
   - 转为 Follower，清空 votedFor

3. 重置选举超时计时器

4. 日志匹配检查:
   - 若 prevLogIndex > 0 且 log.size <= prevLogIndex: 
     返回 (term=currentTerm, success=false, matchIndex=log.size-1)
   - 若 prevLogIndex > 0 且 log[prevLogIndex].term != prevLogTerm:
     返回 (term=currentTerm, success=false, matchIndex=prevLogIndex-1)

5. 追加日志:
   - 若 entries 非空:
     - 清除 log 中 index >= prevLogIndex+1 的冲突条目
     - 追加新条目到 log
     - 截断日志: 若 log.size > maxLogSize，删除最早的条目（保留 commitIndex 以上）

6. 更新 commitIndex:
   - 若 leaderCommit > commitIndex:
     commitIndex = min(leaderCommit, log.lastIndex)

7. 应用到状态机:
   - 若 commitIndex > lastApplied:
     - lastApplied++
     - 更新本地 leaderId = log[lastApplied].leaderId

8. 返回 (term=currentTerm, success=true, matchIndex=log.lastIndex)
```

### Leader 选举后处理

#### 当选 Leader 时

```
1. 初始化 nextIndex[] = log.lastIndex + 1（每个 Follower）
2. 初始化 matchIndex[] = 0（每个 Follower）
3. 立即追加第一条日志:
   - 构造 LogEntry(index=nextIndex, term=currentTerm, leaderId=nodeId)
   - 追加到 log
   - 通过心跳发送给所有 Follower
   - 等待多数确认后提交
```

#### 日志回退策略

```
- Follower 返回 matchIndex 时，Leader 据此快速定位
- nextIndex = matchIndex + 1
- 不逐条回退，避免低效
```

## 状态查询接口

### RaftNode 新增方法

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| getCommitIndex() | long | 返回当前 commitIndex |
| getLastApplied() | long | 返回最后应用的索引 |
| getLogEntries() | List\<LogEntry\> | 返回日志快照（只读） |
| getLeaderHistory() | List\<LeaderRecord\> | 返回最近 N 个任期的 Leader |

### LeaderRecord

```java
public class LeaderRecord {
    private final long term;
    private final String leaderId;
}
```

### RaftGroup 新增方法

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| getCommittedLeader() | String | 返回已提交的 Leader ID |
| getLeaderHistory() | List\<LeaderRecord\> | 聚合所有节点的历史 |

## 错误处理

### 日志为空

- 新节点启动时 log 为空，index 从 1 开始
- prevLogIndex=0 表示"无前一条日志"，prevLogTerm=0
- Follower 收到 prevLogIndex=0 时跳过日志匹配检查

### 网络分区恢复

- 分区恢复的 Follower 日志可能落后或冲突
- Leader 通过 matchIndex 快速定位并发送缺失条目
- Follower 截断冲突日志后重新同步

### 日志截断策略

- maxLogSize=10 时，保留最近 10 条
- 截断规则: 若 log.size > maxLogSize，删除 index 最小的条目
- 已提交的条目不能截断（commitIndex 是截断下界）

### 并发安全

- log 使用 `CopyOnWriteArrayList` 或 `synchronized` 保护
- commitIndex/lastApplied 使用 `volatile`
- nextIndex/matchIndex 使用 `ConcurrentHashMap`

## 测试策略

### 单元测试

| 测试项 | 验证内容 |
|--------|----------|
| LogEntry 序列化 | Protostuff 序列化/反序列化正确 |
| 日志追加与截断 | maxLogSize 边界，已提交条目不截断 |
| Follower 日志匹配 | prevLogIndex/prevLogTerm 匹配成功/失败 |
| Leader matchIndex | matchIndex 正确更新 |
| commitIndex 推进 | 多数确认后 commitIndex 更新 |
| 日志冲突回退 | nextIndex 快速回退 |

### 集成测试

| 场景 | 验证内容 |
|------|----------|
| 正常流程 | Leader 选出 → 日志复制 → 多数确认 → 提交 |
| 网络分区 | 少数派无法确认，Leader 不推进 commitIndex |
| 分区恢复 | Follower 重新同步并追赶 |
| Leader 切换 | 新 Leader 初始化 nextIndex/matchIndex |
| 日志冲突 | 不同任期日志冲突，Follower 截断后同步 |

### 性能测试

- 测量日志复制延迟（Leader → 多数确认）
- 测量日志截断性能（maxLogSize=10）

## 实现计划

### Phase 1: 数据结构

| 序号 | 步骤 | 输出物 |
|------|------|--------|
| 1.1 | 创建 LogEntry 类 | LogEntry.java |
| 1.2 | 创建 AppendEntriesRequest/Response | AppendEntries*.java |
| 1.3 | 创建 LeaderRecord 类 | LeaderRecord.java |
| 1.4 | 添加 maxLogSize 到 SpeedboatConfig/SpeedboatConsts | 配置类更新 |

### Phase 2: RaftNode 日志字段

| 序号 | 步骤 | 输出物 |
|------|------|--------|
| 2.1 | 添加 log/commitIndex/lastApplied 字段 | RaftNode.java |
| 2.2 | 添加 nextIndex/matchIndex 字段 | RaftNode.java |
| 2.3 | 添加日志辅助方法（getLastLogIndex/getLastLogTerm） | RaftNode.java |

### Phase 3: Follower 处理

| 序号 | 步骤 | 输出物 |
|------|------|--------|
| 3.1 | 实现 handleAppendEntries | RaftNode.java |
| 3.2 | 实现日志匹配检查 | RaftNode.java |
| 3.3 | 实现日志追加与截断 | RaftNode.java |
| 3.4 | 实现 commitIndex 更新 | RaftNode.java |

### Phase 4: Leader 发送

| 序号 | 步骤 | 输出物 |
|------|------|--------|
| 4.1 | 实现 sendAppendEntries | RaftNode.java |
| 4.2 | 实现 commitIndex 推进逻辑 | RaftNode.java |
| 4.3 | 更新心跳发送逻辑 | RaftNode.java |
| 4.4 | 更新选举成功后的初始化 | RaftNode.java |

### Phase 5: RPC 集成

| 序号 | 步骤 | 输出物 |
|------|------|--------|
| 5.1 | 添加 AppendEntries 处理器到 TransportLayer | TransportLayer.java |
| 5.2 | 更新 RpcMessageHandler | RpcMessageHandler.java |
| 5.3 | 兼容 HeartbeatRequest | 向后兼容 |

### Phase 6: 查询接口

| 序号 | 步骤 | 输出物 |
|------|------|--------|
| 6.1 | 实现 getLeaderHistory | RaftNode.java |
| 6.2 | 实现 getCommittedLeader | RaftGroup.java |

### Phase 7: 测试

| 序号 | 步骤 | 输出物 |
|------|------|--------|
| 7.1 | LogEntry 单元测试 | LogEntryTest.java |
| 7.2 | 日志复制单元测试 | LogReplicationTest.java |
| 7.3 | 集成测试 | LogReplicationIntegrationTest.java |

## 风险与缓解

| 风险 | 缓解策略 |
|------|----------|
| 日志截断导致已提交数据丢失 | 截断时检查 commitIndex |
| 并发访问导致状态不一致 | 使用线程安全数据结构 |
| 网络分区导致日志不一致 | 通过 matchIndex 快速定位并重新同步 |

## 依赖

- 现有 RaftNode/RaftGroup 实现
- 现有 RPC 框架（NettyTransport）
- 现有序列化框架（ProtostuffSerializer）
