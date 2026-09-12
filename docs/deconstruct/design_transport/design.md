# Transport 模块设计文档

## 模块概述

`cn.itcraft.speedboat.transport` 是 Speedboat 的网络传输层，基于 Netty 4.1.68 实现 NIO 异步通信。模块采用接口-实现分离设计，通过 `TransportLayer` 接口定义通信契约，`NettyTransport` 提供 Netty 实现。支持请求-响应模式、异步回调、广播通信和超时控制。

## 核心类设计

### TransportLayer (接口)

**职责**：定义 Raft 通信层的抽象契约，隔离上层协议逻辑与底层网络实现。

**3 个发送方法**：

| 方法 | 返回类型 | 用途 |
|------|----------|------|
| `sendRequestVote(String, RequestVoteRequest)` | `CompletableFuture<RequestVoteResponse>` | 发送投票请求 |
| `sendHeartbeat(String, HeartbeatRequest)` | `CompletableFuture<HeartbeatResponse>` | 发送心跳请求 |
| `sendAppendEntries(String, AppendEntriesRequest)` | `CompletableFuture<AppendEntriesResponse>` | 发送日志复制请求 |

**3 个 FunctionalInterface 处理器**：

| 处理器 | 类型 | 方法签名 |
|--------|------|----------|
| `RequestVoteHandler` | `@FunctionalInterface` | `RequestVoteResponse handle(RequestVoteRequest)` |
| `HeartbeatHandler` | `@FunctionalInterface` | `HeartbeatResponse handle(HeartbeatRequest)` |
| `AppendEntriesHandler` | `@FunctionalInterface` | `AppendEntriesResponse handle(AppendEntriesRequest)` |

**设计模式**：策略模式（接口隔离）+ 观察者模式（FunctionalInterface 回调）。三个 FunctionalInterface 均使用 Lambda 设置，使 RaftNode 无需感知 Netty 细节。

### NettyTransport

**职责**：TransportLayer 的 Netty NIO 实现，管理服务端监听、客户端连接池、请求-响应匹配和超时控制。

**核心组件**：

| 组件 | 类型 | 说明 |
|------|------|------|
| `localEndpoint` | `NodeEndpoint` | 本节点端点信息 |
| `peers` | `List<NodeEndpoint>` | 对等节点列表 |
| `serializer` | `CustomSerializer` | 自定义序列化器 |
| `bossGroup` | `NioEventLoopGroup(1)` | 服务端接收连接线程组 |
| `workerGroup` | `NioEventLoopGroup()` | 读写处理线程组（默认 2*cpu） |
| `channels` | `ConcurrentHashMap<String, Channel>` | 节点ID到Channel映射 |
| `channelGroup` | `DefaultChannelGroup` | 全局Channel组管理 |
| `pendingRequests` | `ConcurrentHashMap<String, CompletableFuture<RpcResponse>>` | 请求ID到Future映射 |

**服务端启动** (`start()`)：

1. 创建 `ServerBootstrap`，配置 bossGroup/workerGroup
2. 使用 `NioServerSocketChannel`，绑定 `localEndpoint.getPort()`
3. Pipeline：`LengthFieldBasedFrameDecoder(max=1024, lenField=4, strip=4)` → `ByteArrayEncoder` → `RpcMessageHandler`
4. 同步等待绑定完成（`bind().sync()`）
5. **不做启动期 connectToPeers，改为懒连接**：首次发送时才建立连接（借鉴 ABChecker 模式）

**客户端连接**（懒连接 `getChannel()` → `connectTo()`）：

1. `getChannel()`：查 `channels` 缓存，命中且 `isActive()` 直接复用；失效则移除后重建
2. `connectTo()`：`synchronized` 双重检查，避免并发重复建连
3. 创建 `Bootstrap`，Pipeline：`LengthFieldBasedFrameDecoder` → `ByteArrayEncoder` → `RpcResponseHandler`
4. 连接等待上限 `CONNECT_TIMEOUT_MS = 1000ms`；失败返回 null，下次发送时自动重试
5. 连接成功后将 Channel 存入 `channels` 和 `channelGroup`

**发送失败处理**：`invalidateChannel()` 将失效 Channel 从 `channels` 移除，下次发送自动重建（自愈式连接管理）。

**请求-响应流程**：

所有三个发送方法（`sendRequestVote`、`sendHeartbeat`、`sendAppendEntries`）遵循相同模式：

1. 通过懒连接获取目标 Channel，若获取失败，返回已完成 Future（失败结果）
2. 创建 `CompletableFuture`，以 `requestId` 为键存入 `pendingRequests`
3. 通过 `serializer.wrap()` 序列化请求（帧格式：length(4B)+crc32(4B)+serType(1B)+msgType(1B)+payload），通过 `channel.writeAndFlush()` 发送
4. 设置 `DEFAULT_TIMEOUT_MS`（5000ms）超时任务：超时后从 `pendingRequests` 移除并完成 Future（失败结果）
5. 异常时移除 pending request 并以失败完成 Future

**广播方法**：

- `broadcastRequestVote(RequestVoteRequest)`：遍历 peers（排除自身）发送投票请求
- `broadcastHeartbeat(HeartbeatRequest)`：遍历 peers（排除自身）发送心跳，含 `channels.isEmpty()` NPE 防护

**关闭** (`shutdown()`)：关闭所有 Channel，优雅关闭 workerGroup 和 bossGroup。

**超时处理**：统一使用 `DEFAULT_TIMEOUT_MS = 5000` 毫秒。超时后以失败结果（voteGranted=false, success=false）完成 Future，而非抛出异常，保证上层不会因超时阻塞。

**线程安全**：
- `channels` 和 `pendingRequests` 使用 `ConcurrentHashMap`
- `channelGroup` 使用 Netty 的 `DefaultChannelGroup`（内部线程安全）
- 超时任务使用 `workerGroup.schedule()` 执行

### NodeEndpoint

**职责**：网络节点端点的简单 POJO，封装节点ID、主机地址和端口。

**字段**：`nodeId`（String）、`host`（String）、`port`（int），均为 final。

**静态工厂**：`parse(String nodeUrl)` 将 "host:port" 格式解析为 NodeEndpoint。注意：当前实现中 nodeId 和 host 均设为解析出的 host 部分，适用于简单场景。

**线程安全**：不可变对象，天然线程安全。

### RpcMessageHandler

**职责**：服务端 Netty ChannelInboundHandlerAdapter，处理入站 RPC 请求并写回响应。

**类型分发**：`channelRead()` 中通过 `instanceof` 判断反序列化后的对象类型：

1. `RequestVoteRequest`：调用 `transport.getRequestVoteHandler().handle(request)`，将响应包装为带 requestId 的 `RequestVoteResponse`，序列化后写回
2. `HeartbeatRequest`：同理处理，返回 `HeartbeatResponse`
3. `AppendEntriesRequest`：同理处理，返回 `AppendEntriesResponse`

**错误处理**：`exceptionCaught()` 直接关闭连接。反序列化失败时关闭连接。

### RpcResponseHandler

**职责**：客户端 Netty ChannelInboundHandlerAdapter，处理入站 RPC 响应并完成对应的 CompletableFuture。

**响应匹配**：`channelRead()` 中将反序列化后的对象按 `instanceof RpcResponse` 判断，提取 `response.getRequestId()`，从 `transport.getPendingRequests()` 中查找并移除对应的 Future，然后 `future.complete(response)`。

**错误处理**：同 RpcMessageHandler，异常时关闭连接。

## 数据流

```
[发送端] RaftNode
    |-- sendRequestVote(peerId, request)
    |     |
    |     |-- NettyTransport.serialize(wrap) → byte[]
    |     |-- channel.writeAndFlush(byte[])
    |     |-- pendingRequests.put(requestId, future)
    |     |-- workerGroup.schedule(timeout task, 5000ms)
    |     |-- return CompletableFuture
    |
[接收端] NettyTransport
    |-- ByteArrayDecoder → RpcMessageHandler
    |     |-- serializer.unwrap(byte[])
    |     |-- handler.handle(request) → response
    |     |-- serializer.wrap(response) → byte[]
    |     |-- ctx.writeAndFlush(byte[])
    |
[发送端] RpcResponseHandler
    |-- ByteArrayDecoder → RpcResponseHandler
    |     |-- serializer.unwrap(byte[])
    |     |-- pendingRequests.remove(requestId)
    |     |-- future.complete(response)
```

## 设计模式

| 模式 | 应用 |
|------|------|
| 策略模式 | TransportLayer 接口 + NettyTransport 实现 |
| 观察者模式 | FunctionalInterface 回调（RequestVoteHandler 等） |
| 外观模式 | TransportLayer 屏蔽 Netty 复杂性 |
| 异步回调 | CompletableFuture 实现请求-响应异步匹配 |

## 配置参数

| 参数 | 值 | 说明 |
|------|------|------|
| 连接超时 | 3000ms | 每个 peer 连接等待时间 |
| 请求超时 | 5000ms | 单个 RPC 请求超时时间 |
| bossGroup 线程数 | 1 | 服务端接收连接线程 |
| workerGroup 线程数 | 默认（2*cpu） | 读写处理线程 |

## 扩展点

1. 实现 `TransportLayer` 接口可替换底层通信框架（如 gRPC、RSocket）
2. 通过 `CustomSerializer` 参数可替换序列化方案
3. 通过派生 `ChannelInboundHandlerAdapter` 可扩展消息处理逻辑