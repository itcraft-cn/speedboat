# 序列化算法

## 概述

Speedboat 序列化模块采用 **双层协议封装** 设计：
- **内层**：Protostuff 高效二进制序列化
- **外层**：CustomSerializer 自定义协议头（CRC32 校验 + 类型标识）

**设计目标**：
- 高性能：Protostuff 比 Java 原生序列化快 5-10 倍
- 可靠性：CRC32 校验防止数据损坏
- 可扩展：支持多种序列化实现切换

---

## 协议格式

### 消息结构

```
+----------------+----------------+----------+----------+------------+
| Payload Length | CRC32 Checksum | Serializer Type | Message Type | Payload    |
| 4 bytes        | 4 bytes        | 1 byte         | 1 byte      | N bytes    |
+----------------+----------------+-----------------+-------------+------------+
| <---------- HEADER (10 bytes) ---------->                          | <---------> |
```

### 字段说明

| 字段 | 长度 | 说明 |
|------|------|------|
| Payload Length | 4 bytes | payload 字节数，用于解析边界 |
| CRC32 Checksum | 4 bytes | payload 的 CRC32 校验和 |
| Serializer Type | 1 byte | 序列化器类型 ID（当前固定为 2，Protostuff） |
| Message Type | 1 byte | RPC 消息类型 ID（1-6） |
| Payload | N bytes | 序列化后的业务数据 |

### 消息类型映射

| Type ID | 消息类型 | 说明 |
|---------|----------|------|
| 1 | RequestVoteRequest | 投票请求 |
| 2 | RequestVoteResponse | 投票响应 |
| 3 | HeartbeatRequest | 心跳请求 |
| 4 | HeartbeatResponse | 心跳响应 |
| 5 | AppendEntriesRequest | 日志复制请求 |
| 6 | AppendEntriesResponse | 日志复制响应 |

---

## 核心算法

### 序列化流程（wrap）

**输入**：Java 对象 obj  
**输出**：byte[] 包含协议头的数据

```
算法 wrap(obj):
    1. 查找 obj 类型对应的 typeId
       若未注册 → 抛出 SerializationException
    
    2. 调用 payloadSerializer.serialize(obj)
       → 得到 payload[]
    
    3. 计算 CRC32:
       crc32 = new CRC32()
       crc32.update(payload)
       checksum = (int) crc32.getValue()
    
    4. 构造 ByteBuffer (10 + payload.length):
       buffer.putInt(payload.length)        // 4 bytes
       buffer.putInt(checksum)              // 4 bytes
       buffer.put(serializerTypeId)         // 1 byte
       buffer.put(messageTypeId)            // 1 byte
       buffer.put(payload)                  // N bytes
    
    5. 返回 buffer.array()
```

**时间复杂度**：O(n)，n = payload 大小  
**空间复杂度**：O(n)

### 反序列化流程（unwrap）

**输入**：byte[] data, Class<T> clazz  
**输出**：T 对象

```
算法 unwrap(data, clazz):
    1. 边界检查:
       若 data.length < 10 → 抛出 SerializationException
    
    2. 解析 Header:
       buffer = ByteBuffer.wrap(data)
       payloadLen = buffer.getInt()
       crc32Value = buffer.getInt()
       serializerTypeId = buffer.get() & 0xFF
       messageTypeId = buffer.get() & 0xFF
    
    3. 提取 Payload:
       若 payloadLen > data.length - 10 → 抛出 SerializationException
       payload = new byte[payloadLen]
       buffer.get(payload)
    
    4. CRC32 校验:
       crc32 = new CRC32()
       crc32.update(payload)
       若 (int) crc32.getValue() ≠ crc32Value → 抛出 SerializationException
    
    5. 类型解析:
       targetClass = idToType.get(messageTypeId)
       若 targetClass == null → 抛出 SerializationException
    
    6. 反序列化:
       return payloadSerializer.deserialize(payload, targetClass)
```

**时间复杂度**：O(n)  
**空间复杂度**：O(n)

---

## Protostuff 序列化

### 核心特性

| 特性 | 说明 |
|------|------|
| Schema 自动生成 | RuntimeSchema.getSchema() 运行时生成 |
| 无需预定义 | 不需要 .proto 文件 |
| 高效编码 | 变长整数、字段编号优化 |
| LinkedBuffer | 可复用缓冲区，减少 GC |

### 序列化实现

```
ProtostuffSerializer.serialize(obj):
    1. 若 obj == null → 返回空数组
    
    2. schema = RuntimeSchema.getSchema(obj.getClass())
    
    3. buffer = LinkedBuffer.allocate(256)  // 初始 256 bytes
    
    4. data = ProtostuffIOUtil.toByteArray(obj, schema, buffer)
    
    5. buffer.clear()  // 重置缓冲区供下次使用
    
    6. 返回 data
```

### 反序列化实现

```
ProtostuffSerializer.deserialize(data, clazz):
    1. 若 data == null 或 data.length == 0 → 返回 null
    
    2. schema = RuntimeSchema.getSchema(clazz)
    
    3. obj = schema.newMessage()  // 创建空实例
    
    4. ProtostuffIOUtil.mergeFrom(data, obj, schema)  // 填充字段
    
    5. 返回 obj
```

---

## 性能分析

### 内存占用

| 组件 | 初始大小 | 说明 |
|------|----------|------|
| LinkedBuffer | 256 bytes | 可复用缓冲区 |
| ByteBuffer (wrap) | 10 + payload | 每次序列化新分配 |
| CRC32 | 32 bytes 内部状态 | 每次计算新实例 |

### 性能优化点

| 优化项 | 当前实现 | 改进空间 |
|--------|----------|----------|
| LinkedBuffer 池化 | 每次 allocate | 可用 ThreadLocal 池化 |
| CRC32 实例 | 每次新建 | 可复用单例 |
| ByteBuffer | 每次新分配 | 可用 DirectBuffer 池 |

### 性能数据

| 操作 | 耗时 | 说明 |
|------|------|------|
| 序列化 RequestVoteRequest | ~5 μs | 小对象快速序列化 |
| 序列化 HeartbeatRequest | ~3 μs | 最简单的 RPC 消息 |
| 序列化 AppendEntriesRequest | ~10-50 μs | 取决于 entries 数量 |
| CRC32 计算 | ~0.5 μs/KB | Java 内置实现 |

---

## 错误处理

### 异常类型

| 异常 | 触发条件 | 处理建议 |
|------|----------|----------|
| SerializationException | 类型未注册 | 检查 registerType 调用 |
| SerializationException | 数据长度不足 | 检查网络传输完整性 |
| SerializationException | CRC32 校验失败 | 数据损坏，需重传 |
| SerializationException | 未知消息类型 | 版本不兼容，需升级 |

---

## 扩展性

### 支持新的序列化器

```java
public interface Serializer {
    byte[] serialize(Object obj) throws SerializationException;
    <T> T deserialize(byte[] data, Class<T> clazz) throws SerializationException;
    int getTypeId();
}
```

### 支持新的消息类型

```java
customSerializer.registerType(7, NewRpcRequest.class);
customSerializer.registerType(8, NewRpcResponse.class);
```

---

## 与其他组件交互

```
RaftNode                    NettyTransport               CustomSerializer
    |                            |                              |
    | propose(data)              |                              |
    |--------------------------->|                              |
    |                            | wrap(AppendEntriesRequest)   |
    |                            |----------------------------->|
    |                            |                              |
    |                            |<-----------------------------|
    |                            |         byte[] data          |
    |                            |                              |
    |                            | channel.writeAndFlush(data)  |
    |                            |----------------------------->|
    |                            |                              |
    |                            |          Network             |
    |                            |<=============================>|
    |                            |                              |
    |                            |          byte[] data         |
    |                            |<-----------------------------|
    |                            |                              |
    |                            | unwrap(data, xxxRequest.class)|
    |                            |----------------------------->|
    |                            |                              |
    |                            |<-----------------------------|
    |                            |        xxxRequest            |
    |                            |                              |
    | handleXxxRequest(req)      |                              |
    |<---------------------------|                              |
```

---

## 设计权衡

| 决策点 | 选择方案 | 优点 | 缺点 |
|--------|----------|------|------|
| 序列化框架 | Protostuff | 高性能，无需 Schema 文件 | 不支持循环引用 |
| 协议头 | 固定 10 字节 | 解析简单高效 | 扩展字段受限 |
| 校验算法 | CRC32 | 速度快，实现简单 | 非加密级别完整性 |
| 类型注册 | 手动注册 | 明确类型映射 | 需维护类型 ID |

---

## 参考

- Protostuff 官方文档：https://protostuff.github.io/docs/
- CRC32 算法：IEEE 802.3 标准
- 相关源码：`src/main/java/cn/itcraft/speedboat/serialize/`
