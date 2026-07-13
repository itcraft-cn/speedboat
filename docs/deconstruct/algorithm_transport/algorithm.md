# 网络传输算法

## 概述

Speedboat 网络传输层基于 **Netty 4.1** 实现，提供高性能、异步非阻塞的 RPC 通信能力。

**核心特性**：
- 异步请求-响应模式（CompletableFuture）
- 自动重连与超时处理
- 多连接管理（Channel 池）
- 双向 RPC 支持

---

## 架构设计

### 组件关系

```
+------------------+
|   RaftNode       |
+------------------+
         |
         v
+------------------+      +------------------+
|  TransportLayer  |<---->|  CustomSerializer|
|    (interface)   |      +------------------+
+------------------+
         |
         v
+------------------+
|  NettyTransport  |
+------------------+
    |         |
    v         v
+-------+  +-------+
|Server |  |Client |
|Handler|  |Handler|
+-------+  +-------+
    |         |
    v         v
+------------------+
|   Channel Pool   |
+------------------+
```

### 核心类职责

| 类名 | 职责 |
|------|------|
| TransportLayer | 传输层接口，定义 RPC 方法 |
| NettyTransport | Netty 实现，管理连接池 |
| RpcMessageHandler | 服务端消息处理器 |
| RpcResponseHandler | 客户端响应处理器 |
| NodeEndpoint | 节点地址封装 |

---

## 连接管理算法

### 启动流程

```
NettyTransport.start():
    1. 创建 ServerBootstrap:
       - bossGroup: 1 线程
       - workerGroup: N 线程（默认 CPU 核数）
       - channel: NioServerSocketChannel
       - handler: ByteArrayDecoder + ByteArrayEncoder + RpcMessageHandler
    
    2. 绑定本地端口:
       serverBootstrap.bind(localEndpoint.getPort()).sync()
    
    3. 连接到所有 peers:
       connectToPeers()
```

### 连接 Peers 算法

```
connectToPeers():
    1. 创建 Bootstrap:
       - group: workerGroup（复用）
       - channel: NioSocketChannel
       - handler: ByteArrayDecoder + ByteArrayEncoder + RpcResponseHandler
    
    2. 遍历 peers:
       for peer in peers:
           if peer.nodeId != localEndpoint.nodeId:
               future = bootstrap.connect(peer.host, peer.port)
               future.addListener { f ->
                   if f.isSuccess:
                       channel = f.channel()
                       channels.put(peer.nodeId, channel)
                       channelGroup.add(channel)
               }
    
    3. 等待所有连接完成（最多 3 秒）:
       for future in connectFutures:
           future.await(3000, TimeUnit.MILLISECONDS)
```

---

## RPC 调用算法

### 发送请求算法（以 sendRequestVote 为例）

```
sendRequestVote(targetNodeId, request):
    1. 获取 Channel:
       channel = channels.get(targetNodeId)
       if channel == null 或 !channel.isActive():
           return CompletableFuture.completedFuture(
               RequestVoteResponse(request.requestId, 0, false)
           )
    
    2. 创建 Future:
       future = new CompletableFuture<RequestVoteResponse>()
       pendingRequests.put(request.requestId, future)
    
    3. 序列化并发送:
       data = serializer.wrap(request)
       channel.writeAndFlush(data)
    
    4. 设置超时任务:
       workerGroup.schedule(() -> {
           pending = pendingRequests.remove(request.requestId)
           if pending != null 且 !pending.isDone():
               pending.complete(RequestVoteResponse(request.requestId, 0, false))
       }, 5000, TimeUnit.MILLISECONDS)
    
    5. 返回 Future:
       return future
```

### 广播算法

```
broadcastHeartbeat(request):
    futures = new ArrayList()
    
    for nodeId in channels.keySet():
        future = sendHeartbeat(nodeId, request)
        futures.add(future)
    
    return futures
```

---

## 消息处理算法

### 服务端处理（RpcMessageHandler）

```
channelRead0(ctx, msg):
    1. 反序列化:
       request = serializer.unwrap(msg, xxxRequest.class)
    
    2. 根据 request 类型分发:
       if request is RequestVoteRequest:
           response = transport.getRequestVoteHandler().handle(request)
       else if request is HeartbeatRequest:
           response = transport.getHeartbeatHandler().handle(request)
       else if request is AppendEntriesRequest:
           response = transport.getAppendEntriesHandler().handle(request)
    
    3. 序列化响应:
       responseData = serializer.wrap(response)
    
    4. 写回:
       ctx.writeAndFlush(responseData)
```

### 客户端响应处理（RpcResponseHandler）

```
channelRead0(ctx, msg):
    1. 反序列化:
       response = serializer.unwrap(msg, RpcResponse.class)
    
    2. 查找对应的 pending Future:
       future = pendingRequests.get(response.getRequestId())
    
    3. 完成 Future:
       if future != null:
           future.complete(response)
           pendingRequests.remove(response.getRequestId())
```

---

## 超时与重试

### 超时配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| DEFAULT_TIMEOUT_MS | 5000 ms | RPC 调用超时 |
| 连接等待超时 | 3000 ms | 连接 peers 最大等待 |
| shutdown 超时 | 1 s | 优雅关闭等待 |

### 超时处理流程

```
                    请求发送
                        |
                        v
              +-------------------+
              | 设置超时任务      |
              | (5秒后执行)       |
              +-------------------+
                        |
        +---------------+---------------+
        |                               |
        v                               v
  收到响应                          超时触发
        |                               |
        v                               v
  完成 Future                    完成 Future
  (成功响应)                     (失败响应)
        |                               |
        +---------------+---------------+
                        |
                        v
              从 pendingRequests 移除
```

---

## 性能优化

### 连接复用

| 优化项 | 实现方式 |
|--------|----------|
| Channel 池 | Map<String, Channel> 复用连接 |
| EventLoopGroup 共享 | Server/Client 复用 workerGroup |
| ByteBuf 池化 | Netty 默认开启池化 |

### 零拷贝

```
Netty 零拷贝优化：
- ByteBuf: 堆外内存，避免 JVM 堆拷贝
- FileRegion: 文件传输零拷贝
- CompositeByteBuf: 多 Buffer 组合零拷贝
```

### 线程模型

```
+-------------+     +-------------+     +-------------+
| bossGroup   | --> | workerGroup | --> | Handler     |
| (1 thread)  |     | (N threads) |     | (Netty IO)  |
+-------------+     +-------------+     +-------------+
                            |
                            v
                    +-------------+
                    | RaftNode    |
                    | (业务处理)  |
                    +-------------+
```

---

## 可靠性保证

### 连接断开处理

| 场景 | 处理方式 |
|------|----------|
| Channel 非活跃 | 返回默认失败响应 |
| 发送异常 | completeExceptionally |
| 连接失败 | 等待下次心跳重连 |

### 消息完整性

```
完整性保证链路：
1. CustomSerializer CRC32 校验
2. Netty TCP 保证有序可靠
3. requestId 请求-响应匹配
```

---

## 关闭流程

```
shutdown():
    1. 关闭所有 Channel:
       for channel in channels.values():
           channel.close()
    
    2. 优雅关闭 EventLoopGroup:
       workerGroup.shutdownGracefully()
       bossGroup.shutdownGracefully()
```

---

## 配置参数

| 参数 | 默认值 | 配置方式 |
|------|--------|----------|
| boss 线程数 | 1 | 构造参数 |
| worker 线程数 | CPU 核数 | 构造参数 |
| 连接超时 | 3 s | connectToPeers |
| RPC 超时 | 5 s | DEFAULT_TIMEOUT_MS |

---

## 监控指标

| 指标 | 获取方式 |
|------|----------|
| 活跃连接数 | channels.size() |
| 待处理请求数 | pendingRequests.size() |
| Channel 状态 | channel.isActive() |

---

## 设计权衡

| 决策点 | 选择方案 | 优点 | 缺点 |
|--------|----------|------|------|
| IO 模型 | NIO | 高并发，非阻塞 | 编程复杂度高 |
| 连接管理 | 长连接池 | 减少连接开销 | 需维护连接状态 |
| 超时处理 | 定时任务 | 简单可靠 | 轻微定时器开销 |
| 序列化 | ByteArray | 简单通用 | 需额外编解码器 |

---

## 参考

- Netty 官方文档：https://netty.io/wiki/
- Java NIO 原理：Java IO 模型
- 相关源码：`src/main/java/cn/itcraft/speedboat/transport/`
