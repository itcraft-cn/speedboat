# RPC 模块设计文档

## 模块概述

`cn.itcraft.speedboat.rpc` 是 Speedboat 的 RPC 消息定义层，定义 Raft 协议所需的所有请求和响应消息类型。采用抽象基类 + 具体消息类的继承体系，通过 UUID 实现请求-响应匹配。

## 核心类设计

### RpcRequest (抽象基类)

**职责**：所有 RPC 请求的抽象基类，自动生成唯一请求ID。

**核心设计**：`requestId` 字段在构造器中通过 `UUID.randomUUID().toString()` 自动生成，确保每次请求具有全局唯一标识，用于请求-响应匹配。

**构造器**：提供无参构造器和接受 `requestId` 参数的构造器，后者支持继承场景下由子类注入。

### RpcResponse (抽象基类)

**职责**：所有 RPC 响应的抽象基类，通过 `requestId` 匹配请求。

**设计**：`requestId` 为 final 字段，仅通过构造器注入。响应类可提供多个构造器（带 requestId 用于网络传输匹配，无 requestId 用于服务端内部创建）。

### AppendEntriesRequest

**职责**：标准 Raft 日志复制/心跳请求。

**字段**：

| 字段 | 类型 | Raft 含义 |
|------|------|-----------|
| `term` | `long` | Leader 当前任期 |
| `leaderId` | `String` | Leader 节点ID |
| `prevLogIndex` | `long` | 前一条日志的索引 |
| `prevLogTerm` | `long` | 前一条日志的任期 |
| `entries` | `List<LogEntry>` | 待复制的日志条目列表 |
| `leaderCommit` | `long` | Leader 已提交的日志索引 |

**静态工厂方法**：`heartbeat(term, leaderId, prevLogIndex, prevLogTerm, leaderCommit)` 创建空 entries 的 AppendEntriesRequest，用于心跳场景。

**线程安全**：entries 在构造时 `new ArrayList<>(entries)` 做防御性拷贝，getter 返回 `Collections.unmodifiableList`。

### AppendEntriesResponse

**职责**：日志复制/心跳响应。

**字段**：

| 字段 | 类型 | Raft 含义 |
|------|------|-----------|
| `term` | `long` | 响应者当前任期 |
| `success` | `boolean` | 日志复制是否成功 |
| `matchIndex` | `long` | 匹配的日志索引（失败时用于快速回退） |

**构造器**：提供 3 个构造器——无参（序列化用）、带 requestId（网络传输）、不带 requestId（服务端内部创建）。

### RequestVoteRequest

**职责**：标准 Raft 投票请求。

**字段**：

| 字段 | 类型 | Raft 含义 |
|------|------|-----------|
| `term` | `long` | 候选人任期 |
| `candidateId` | `String` | 候选人节点ID |
| `voteWeight` | `int` | 投票权重（Speedboat 扩展字段） |

**设计要点**：`voteWeight` 是 Speedboat 对标准 Raft 的扩展，允许候选人携带自身权重，投票者根据权重策略计算最终票数。

### RequestVoteResponse

**职责**：投票响应。

**字段**：

| 字段 | 类型 | Raft 含义 |
|------|------|-----------|
| `term` | `long` | 投票者当前任期 |
| `voteGranted` | `boolean` | 是否同意投票 |

**构造器**：提供 3 个构造器——无参、带 requestId、不带 requestId。

### HeartbeatRequest

**职责**：轻量级心跳请求。

**字段**：

| 字段 | 类型 | Raft 含义 |
|------|------|-----------|
| `term` | `long` | Leader 当前任期 |
| `leaderId` | `String` | Leader 节点ID |

**设计要点**：Heartbeat 本质上是空 entries 的 AppendEntries，但作为独立消息类型便于序列化/反序列化类型分发和服务端路由。

### HeartbeatResponse

**职责**：心跳响应。

**字段**：

| 字段 | 类型 | Raft 含义 |
|------|------|-----------|
| `term` | `long` | 响应者当前任期 |
| `success` | `boolean` | 心跳是否成功 |

**构造器**：提供 3 个构造器——无参、带 requestId、不带 requestId。

## 请求-响应匹配流程

```
1. RaftNode 创建 RequestVoteRequest (自动生成 UUID requestId)
2. NettyTransport 发送请求，以 requestId 为键存储 CompletableFuture
3. 对端 RpcMessageHandler 处理请求，生成响应时携带 requestId
4. 本端 RpcResponseHandler 收到响应，通过 requestId 匹配并完成 Future
5. RaftNode 的 thenAccept 回调被触发
```

## 消息类型注册

在 `CustomSerializer` 中注册的类型 ID 映射：

| 类型ID | 类 |
|--------|-----|
| 1 | `RequestVoteRequest` |
| 2 | `RequestVoteResponse` |
| 3 | `HeartbeatRequest` |
| 4 | `HeartbeatResponse` |
| 5 | `AppendEntriesRequest` |
| 6 | `AppendEntriesResponse` |

## 设计模式

| 模式 | 应用 |
|------|------|
| 模板方法模式 | `RpcRequest`/`RpcResponse` 抽象基类定义 requestId 模板 |
| 工厂方法模式 | `AppendEntriesRequest.heartbeat()` 静态工厂创建心跳 |
| 命令模式 | 每种 RPC 消息封装操作语义 |

## 线程安全

所有消息类均为不可变（字段 final 或无 setter），天然线程安全。

## 扩展点

1. 继承 `RpcRequest`/`RpcResponse` 添加新的 RPC 消息类型
2. 在 `CustomSerializer` 中注册新的类型 ID 映射
3. 在 `RpcMessageHandler` 中添加新的 `instanceof` 分支