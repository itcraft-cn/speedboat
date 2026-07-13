# RPC 消息算法

## 概述

Speedboat RPC 模块定义了 **Raft 协议的三种核心消息**，用于节点间通信。

**消息类型**：
- RequestVote：投票请求/响应
- Heartbeat：心跳请求/响应
- AppendEntries：日志复制请求/响应

---

## 消息继承结构

```
RpcRequest (abstract)
    ├── RequestVoteRequest
    ├── HeartbeatRequest
    └── AppendEntriesRequest

RpcResponse (abstract)
    ├── RequestVoteResponse
    ├── HeartbeatResponse
    └── AppendEntriesResponse
```

---

## 基类定义

### RpcRequest

```
RpcRequest {
    String requestId  // UUID 唯一标识
}
```

**用途**：请求-响应匹配

### RpcResponse

```
RpcResponse {
    String requestId  // 对应的请求 ID
}
```

---

## RequestVote 消息

### RequestVoteRequest

```
RequestVoteRequest {
    String requestId
    long term           // 候选人任期
    String candidateId  // 候选人 ID
    int voteWeight      // 附加投票权重
}
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| requestId | String | 请求唯一标识 |
| term | long | 候选人当前任期 |
| candidateId | String | 候选人节点 ID |
| voteWeight | int | 附加投票权重（解决平票） |

---

### RequestVoteResponse

```
RequestVoteResponse {
    String requestId
    long term       // 响应者任期
    boolean voteGranted  // 是否投票
}
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| term | long | 响应者当前任期（用于更新候选人任期） |
| voteGranted | boolean | true 表示投票给候选人 |

---

### 投票请求流程

```
Candidate                          Follower
    |                                 |
    | RequestVoteRequest              |
    | term=2, candidateId=node1       |
    | voteWeight=1                    |
    |-------------------------------->|
    |                                 |
    |                                 | 检查投票条件
    |                                 | - term >= currentTerm
    |                                 | - 未投给其他候选人
    |                                 |
    | RequestVoteResponse             |
    | term=2, voteGranted=true        |
    |<--------------------------------|
    |                                 |
```

---

## Heartbeat 消息

### HeartbeatRequest

```
HeartbeatRequest {
    String requestId
    long term       // Leader 任期
    String leaderId // Leader ID
}
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| term | long | Leader 当前任期 |
| leaderId | String | Leader 节点 ID |

---

### HeartbeatResponse

```
HeartbeatResponse {
    String requestId
    long term           // Follower 任期
    boolean success     // 是否成功
}
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| term | long | Follower 当前任期 |
| success | boolean | true 表示接受心跳 |

---

### 心跳流程

```
Leader                              Follower
    |                                   |
    | HeartbeatRequest                  |
    | term=2, leaderId=node1            |
    |---------------------------------->|
    |                                   |
    |                                   | 检查心跳条件
    |                                   | - term >= currentTerm
    |                                   | - 重置选举超时
    |                                   |
    | HeartbeatResponse                 |
    | term=2, success=true              |
    |<----------------------------------|
    |                                   |
```

---

## AppendEntries 消息

### AppendEntriesRequest

```
AppendEntriesRequest {
    String requestId
    long term           // Leader 任期
    String leaderId     // Leader ID
    long prevLogIndex   // 前一条日志索引
    long prevLogTerm    // 前一条日志任期
    List<LogEntry> entries  // 日志条目列表
    long leaderCommit   // Leader 的 commitIndex
}
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| prevLogIndex | long | 日志一致性检查 |
| prevLogTerm | long | 日志一致性检查 |
| entries | List | 空列表表示心跳 |
| leaderCommit | long | 用于更新 Follower 的 commitIndex |

---

### AppendEntriesResponse

```
AppendEntriesResponse {
    String requestId
    long term       // Follower 任期
    boolean success // 是否成功
    long matchIndex // 已匹配的日志索引
}
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| matchIndex | long | 用于 Leader 更新 nextIndex |

---

### 日志复制流程

```
Leader                              Follower
    |                                   |
    | AppendEntriesRequest              |
    | term=2, prevLogIndex=5            |
    | prevLogTerm=1, entries=[...]      |
    | leaderCommit=6                    |
    |---------------------------------->|
    |                                   |
    |                                   | 一致性检查
    |                                   | log[prevLogIndex].term == prevLogTerm?
    |                                   |
    |                                   | 追加日志
    |                                   | 更新 commitIndex
    |                                   |
    | AppendEntriesResponse             |
    | term=2, success=true, matchIndex=6|
    |<----------------------------------|
    |                                   |
```

---

## 心跳优化

### heartbeat 静态工厂方法

```
AppendEntriesRequest.heartbeat(term, leaderId, prevLogIndex, prevLogTerm, leaderCommit):
    return AppendEntriesRequest(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        Collections.emptyList(),  // 空 entries
        leaderCommit
    )
```

**优化点**：
- entries 为空，减少序列化开销
- 复用 AppendEntries 协议

---

## 消息大小估算

### RequestVoteRequest

| 字段 | 大小 |
|------|------|
| requestId (UUID) | 36 bytes |
| term | 8 bytes |
| candidateId | ~10 bytes |
| voteWeight | 4 bytes |
| **总计** | ~60 bytes |

### HeartbeatRequest

| 字段 | 大小 |
|------|------|
| requestId | 36 bytes |
| term | 8 bytes |
| leaderId | ~10 bytes |
| **总计** | ~55 bytes |

### AppendEntriesRequest

| 字段 | 大小 |
|------|------|
| requestId | 36 bytes |
| term | 8 bytes |
| leaderId | ~10 bytes |
| prevLogIndex | 8 bytes |
| prevLogTerm | 8 bytes |
| entries | 可变 |
| leaderCommit | 8 bytes |
| **总计** | ~80 + entries |

---

## 请求 ID 生成

```
requestId = UUID.randomUUID().toString()
```

**格式**：`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`

**示例**：`550e8400-e29b-41d4-a716-446655440000`

---

## 线程安全

### 不可变性

| 消息类 | 可变性 |
|--------|--------|
| RequestVoteRequest | 不可变（final 字段） |
| RequestVoteResponse | 不可变 |
| HeartbeatRequest | 不可变 |
| HeartbeatResponse | 不可变 |
| AppendEntriesRequest | 部分可变（entries 列表） |

### entries 防御性拷贝

```
getEntries():
    return entries != null 
        ? Collections.unmodifiableList(entries) 
        : Collections.emptyList()
```

---

## 消息序列化顺序

### RequestVoteRequest

```
1. requestId (String)
2. term (long)
3. candidateId (String)
4. voteWeight (int)
```

### AppendEntriesRequest

```
1. requestId
2. term
3. leaderId
4. prevLogIndex
5. prevLogTerm
6. entries (List<LogEntry>)
7. leaderCommit
```

---

## 设计权衡

| 决策点 | 选择方案 | 优点 | 缺点 |
|--------|----------|------|------|
| 消息继承 | RpcRequest/RpcResponse 基类 | 复用 requestId | 耦合度增加 |
| 心跳实现 | AppendEntries 空 entries | 复用协议 | 语义稍混淆 |
| entries 拷贝 | 防御性拷贝 | 安全 | 性能开销 |

---

## 参考

- Raft 论文：Figure 2 (RPC descriptions)
- 相关源码：`src/main/java/cn/itcraft/speedboat/rpc/`
