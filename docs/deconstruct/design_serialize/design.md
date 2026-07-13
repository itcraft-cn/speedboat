# Serialize 模块设计文档

## 模块概述

`cn.itcraft.speedboat.serialize` 是 Speedboat 的序列化层，负责 RPC 消息的序列化与反序列化。采用分层架构：`Serializer` 接口定义 Payload 序列化契约，`ProtostuffSerializer` 提供基于 Protostuff 的高效实现，`CustomSerializer` 作为包装器在 Payload 基础上添加协议头部（长度、CRC32 校验、类型标识），实现带类型路由和校验的二进制协议。

## Serializer (接口)

**职责**：定义 Payload 序列化契约，支持序列化器可插拔。

| 方法 | 说明 |
|------|------|
| `byte[] serialize(Object obj)` | 序列化对象为字节数组 |
| `<T> T deserialize(byte[] data, Class<T> clazz)` | 反序列化字节数组为对象 |
| `int getTypeId()` | 获取序列化器类型标识 |

## ProtostuffSerializer

**职责**：基于 Protostuff 的 Payload 序列化器，使用 Protostuff 的 `RuntimeSchema` 动态获取 Schema，无需预编译。

**配置参数**：

| 参数 | 值 | 说明 |
|------|------|------|
| `TYPE_ID` | 2 | 序列化器类型标识 |
| `LinkedBuffer.allocate()` | 256 | 序列化缓冲区大小 |

**序列化流程**：
1. 空对象返回 `new byte[0]`
2. 通过 `RuntimeSchema.getSchema(obj.getClass())` 动态获取 Schema
3. 分配 `LinkedBuffer.allocate(256)`
4. 调用 `ProtostuffIOUtil.toByteArray()` 序列化
5. finally 块中 `buffer.clear()` 释放缓冲区

**反序列化流程**：
1. 空数组返回 null
2. 通过 `RuntimeSchema.getSchema(clazz)` 获取 Schema
3. `schema.newMessage()` 创建实例
4. `ProtostuffIOUtil.mergeFrom()` 填充数据

**错误处理**：所有序列化/反序列化异常包装为 `SerializationException` 抛出。

## CustomSerializer

**职责**：在 Payload 序列化之上添加协议头部，实现类型路由、CRC32 校验和完整性验证。

**设计模式**：装饰器模式——包装 `Serializer`，在原始 Payload 前后添加协议头部。

**10 字节协议头部结构**：

| 偏移 | 长度 | 字段 | 说明 |
|------|------|------|------|
| 0 | 4 | `payloadLen` | Payload 长度（int） |
| 4 | 4 | `crc32Value` | Payload 的 CRC32 校验值（int） |
| 8 | 1 | `serializerTypeId` | 序列化器类型标识（byte） |
| 9 | 1 | `messageTypeId` | 消息类型标识（byte） |

**类型注册表**：使用两个 `HashMap` 维护双向映射：`typeToId`（Class -> Integer）和 `idToType`（Integer -> Class）。

**内置类型映射**：

| 消息类型ID | 类 |
|-----------|-----|
| 1 | `RequestVoteRequest` |
| 2 | `RequestVoteResponse` |
| 3 | `HeartbeatRequest` |
| 4 | `HeartbeatResponse` |
| 5 | `AppendEntriesRequest` |
| 6 | `AppendEntriesResponse` |

**wrap(Object) 序列化流程**：
1. 查找对象的类型ID，未注册时抛出 `SerializationException`
2. 调用 `payloadSerializer.serialize(obj)` 获取 Payload
3. 分配 `ByteBuffer(HEADER_LEN + payload.length)`
4. 写入 Payload 长度
5. 计算 Payload 的 CRC32 校验值并写入
6. 写入 serializerTypeId 和 messageTypeId
7. 写入 Payload 数据

**unwrap(byte[]) 反序列化流程**：
1. 验证数据长度 >= HEADER_LEN
2. 读取 payloadLen、crc32Value、serializerTypeId、messageTypeId
3. 验证 payloadLen 合法性
4. 提取 Payload 并验证 CRC32 校验值
5. 通过 messageTypeId 查找目标类型
6. 调用 `payloadSerializer.deserialize(payload, targetClass)` 反序列化

**错误处理**：
- 未注册类型 → `SerializationException("Unregistered type: ...")`
- 数据长度不足 → `SerializationException("Invalid data length")`
- Payload 长度非法 → `SerializationException("Invalid payload length")`
- CRC32 校验失败 → `SerializationException("CRC32 check failed")`
- 未知消息类型 → `SerializationException("Unknown message type id: ...")`

## SerializationException

**职责**：序列化模块的统一异常类型，继承 `RuntimeException`，避免强制 try-catch。

**构造器**：提供 `SerializationException(String)` 和 `SerializationException(String, Throwable)` 两个构造器。

## 线程安全

- `ProtostuffSerializer`：无状态，`LinkedBuffer` 在方法内分配和释放，线程安全
- `CustomSerializer`：`typeToId` 和 `idToType` 在构造后不再修改，不可变，线程安全

## 扩展点

1. 实现 `Serializer` 接口可替换底层序列化引擎（如 Kryo、Hessian）
2. 在 `CustomSerializer` 构造器或 `registerType()` 中注册新的消息类型
3. 修改 `HEADER_LEN` 可定制协议头结构