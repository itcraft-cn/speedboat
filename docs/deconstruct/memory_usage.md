# Speedboat 内存使用分析

## 1. 内存模型概览

Speedboat 采用 Java 堆内存管理，核心组件内存占用如下：

---

## 2. 核心组件内存占用

### 2.1 RaftNode

| 属性 | 类型 | 大小 (估算) | 说明 |
|------|------|-------------|------|
| nodeId | String | ~50 bytes | 节点标识 |
| state | NodeState (enum) | ~32 bytes | 枚举引用 |
| currentTerm | Term | ~24 bytes | 任期对象 |
| votedFor | String | ~50 bytes | 投票对象 |
| lastHeartbeat | long | 8 bytes | 时间戳 |
| scheduler | ScheduledExecutorService | ~1KB | 线程池引用 |
| group | RaftGroup | ~16 bytes | 引用 |
| **总计** | | **~1.2KB** | 单节点 |

### 2.2 RaftGroup

| 属性 | 类型 | 大小 (估算) | 说明 |
|------|------|-------------|------|
| nodes | ConcurrentHashMap | ~1KB + N * 50 bytes | 节点映射 |
| groupId | String | ~50 bytes | 组ID |
| groupStrategy | GroupStrategy | ~50 bytes | 策略引用 |
| **总计** | | **~1.1KB + N * 50 bytes** | N=节点数 |

### 2.3 NettyTransport

| 属性 | 类型 | 大小 (估算) | 说明 |
|------|------|-------------|------|
| endpoints | ConcurrentHashMap | ~1KB + N * 100 bytes | 端点映射 |
| bossGroup | EventLoopGroup | ~1MB | Boss线程组 |
| workerGroup | EventLoopGroup | ~2-4MB | Worker线程组 |
| channel | Channel | ~500KB | Netty Channel |
| listeners | CopyOnWriteArrayList | ~100 bytes | 监听器列表 |
| serializer | Serializer | ~1KB | 序列化器 |
| **总计** | | **~5-7MB** | 单传输实例 |

### 2.4 VoteContext

| 属性 | 类型 | 大小 (估算) | 说明 |
|------|------|-------------|------|
| candidate | RaftNode | ~16 bytes | 引用 |
| group | RaftGroup | ~16 bytes | 引用 |
| term | long | 8 bytes | 任期 |
| baseWeight | int | 4 bytes | 基础权重 |
| **总计** | | **~50 bytes** | 单上下文 |

---

## 3. 消息内存占用

### 3.1 序列化前后对比

| 消息类型 | 序列化前 | 序列化后 | 压缩比 |
|----------|----------|----------|--------|
| RequestVoteRequest | ~200 bytes | ~50 bytes | 75% |
| RequestVoteResponse | ~100 bytes | ~30 bytes | 70% |
| HeartbeatRequest | ~150 bytes | ~40 bytes | 73% |
| HeartbeatResponse | ~100 bytes | ~30 bytes | 70% |

### 3.2 CustomSerializer 协议头

```
Header: 9 bytes
  ├─ Length: 4 bytes
  ├─ CRC32: 4 bytes
  └─ Type: 1 byte
Payload: n bytes
```

**示例**：
- RequestVoteRequest: 9 + 50 = 59 bytes
- HeartbeatRequest: 9 + 40 = 49 bytes

---

## 4. 内存分布

### 4.1 单节点内存占用

```
┌─────────────────────────────────┐
│  Speedboat (Facade)             │ ~500 bytes
├─────────────────────────────────┤
│  RaftGroup                      │ ~1.1KB
│    └─ RaftNode (self)           │ ~1.2KB
├─────────────────────────────────┤
│  NettyTransport                 │ ~5-7MB
│    ├─ BossGroup                 │ ~1MB
│    ├─ WorkerGroup               │ ~2-4MB
│    └─ Channel + Buffer          │ ~2MB
├─────────────────────────────────┤
│  Strategies                     │ ~1KB
├─────────────────────────────────┤
│  Others (GC, JVM)               │ ~10MB
└─────────────────────────────────┘
Total: ~18-20MB
```

### 4.2 集群内存占用 (N 节点)

```
单节点: ~20MB
集群 (N=3): ~60MB
集群 (N=5): ~100MB
集群 (N=7): ~140MB
```

---

## 5. 内存分配模式

### 5.1 长期持有对象

| 对象 | 生命周期 | 内存来源 |
|------|----------|----------|
| RaftNode | 进程生命周期 | Heap |
| RaftGroup | 进程生命周期 | Heap |
| NettyTransport | 进程生命周期 | Heap + Direct |
| ScheduledExecutorService | 进程生命周期 | Heap |

### 5.2 短期对象

| 对象 | 生命周期 | GC 回收 |
|------|----------|---------|
| RaftMessage | 消息处理期间 | Young GC |
| VoteContext | 选举期间 | Young GC |
| byte[] (序列化) | 网络传输期间 | Young GC |
| ByteBuf (Netty) | Channel 处理期间 | Pool |

### 5.3 对象池化

**Protostuff LinkedBuffer**：
```java
private static final ThreadLocal<LinkedBuffer> BUFFER_CACHE = 
    ThreadLocal.withInitial(() -> LinkedBuffer.allocate(512));

// 使用
LinkedBuffer buffer = BUFFER_CACHE.get();
try {
    byte[] data = ProtostuffIOUtil.toByteArray(obj, schema, buffer);
} finally {
    buffer.clear();
}
```

**Netty ByteBuf**：
```java
// Netty 默认使用池化 ByteBuf
ByteBuf buf = Unpooled.buffer(1024);
// 使用后自动释放
buf.release();
```

---

## 6. GC 影响

### 6.1 GC 频率

| 场景 | Young GC | Full GC |
|------|----------|---------|
| 空闲状态 | 1次/分钟 | 几乎无 |
| 选举期间 | 2-3次/秒 | 无 |
| 高负载 | 5-10次/秒 | 偶尔 |

### 6.2 GC 暂停时间

| GC 类型 | 暂停时间 | 影响 |
|---------|----------|------|
| Young GC | 1-5ms | 可忽略 |
| Mixed GC | 10-50ms | 轻微影响 |
| Full GC | 100-500ms | 可能导致超时 |

### 6.3 GC 优化建议

1. **堆大小**：建议 1-2GB（根据节点数量）
2. **新生代比例**：提高至 40-50%（减少 Young GC 频率）
3. **使用 G1GC**：适合低延迟场景
4. **禁用 System.gc()**：`-XX:+DisableExplicitGC`

---

## 7. 内存泄漏风险

### 7.1 潜在泄漏点

| 风险点 | 原因 | 解决方案 |
|--------|------|----------|
| ScheduledExecutorService | 未正确关闭 | shutdown() + awaitTermination() |
| ByteBuf | 未释放 | 使用 try-finally 或 ReferenceCountUtil |
| ThreadLocal | 线程池线程复用 | 定期清理或使用 remove() |
| Listener 列表 | 未移除 | 提供移除方法 |

### 7.2 检测方法

```java
// 使用 JVM 工具
jmap -histo <pid> | head -20
jstat -gcutil <pid> 1000

// 使用 JProfiler/VisualVM 分析内存快照
```

---

## 8. 内存优化策略

### 8.1 对象复用

**消息对象池**：
```java
private static final ObjectPool<RequestVoteRequest> REQUEST_POOL = 
    new ObjectPool<>(RequestVoteRequest::new, 100);

// 使用
RequestVoteRequest request = REQUEST_POOL.borrow();
try {
    request.setTerm(term);
    // ...
    transport.send(nodeId, serializer.wrap(request));
} finally {
    REQUEST_POOL.release(request);
}
```

### 8.2 减少对象创建

**避免临时字符串**：
```java
// 不推荐
String key = "node:" + nodeId + ":term";

// 推荐
StringBuilder sb = new StringBuilder(32);
sb.append("node:").append(nodeId).append(":term");
String key = sb.toString();
```

### 8.3 使用原始类型

**避免装箱**：
```java
// 不推荐
Map<String, Integer> voteCount;

// 推荐
Long2IntOpenHashMap voteCount; // fastutil
```

---

## 9. 直接内存使用

### 9.1 Netty Direct Buffer

```
Direct Buffer 优势：
  ├─ 减少内存拷贝 (零拷贝)
  ├─ 降低 GC 压力
  └─ 提升网络传输性能

Direct Buffer 大小：
  ├─ 默认无限制
  ├─ 建议：-XX:MaxDirectMemorySize=512M
  └─ 监控：ByteBuffer.allocateDirect() 使用量
```

### 9.2 监控直接内存

```bash
# JMX 查看直接内存使用
jcmd <pid> VM.native_memory summary

# 或使用 NMT (Native Memory Tracking)
-XX:NativeMemoryTracking=summary
```

---

## 10. 内存配置建议

### 10.1 JVM 参数

```bash
# 堆内存
-Xms1g -Xmx1g

# 新生代
-XX:NewRatio=2 -XX:SurvivorRatio=8

# G1GC (推荐)
-XX:+UseG1GC
-XX:MaxGCPauseMillis=50
-XX:G1HeapRegionSize=16m

# 直接内存
-XX:MaxDirectMemorySize=512m

# GC 日志
-Xlog:gc*:file=gc.log:time,tags
```

### 10.2 配置调优

| 场景 | 堆大小 | 新生代比例 | G1 Region |
|------|--------|------------|-----------|
| 3节点 | 512MB | 40% | 4MB |
| 5节点 | 1GB | 45% | 8MB |
| 7节点+ | 2GB | 50% | 16MB |

---

## 11. 内存监控指标

### 11.1 关键指标

| 指标 | 说明 | 告警阈值 |
|------|------|----------|
| heap_used | 堆内存使用 | > 80% |
| heap_max | 最大堆内存 | - |
| non_heap_used | 非堆内存 | > 100MB |
| direct_memory | 直接内存 | > 400MB |
| gc_pause_ms | GC 暂停 | > 100ms |
| gc_rate | GC 频率 | > 10次/秒 |

### 11.2 监控集成

```java
public class MemoryMonitor {
    private MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    public void monitor() {
        MemoryUsage heap = memoryMXBean.getHeapMemoryUsage();
        double usedRatio = (double) heap.getUsed() / heap.getMax();
        
        if (usedRatio > 0.8) {
            logger.warn("Heap usage high: {}%", usedRatio * 100);
        }
    }
}
```

---

## 12. 总结

### 12.1 内存占用评估

- **单节点**：~20MB（包含 Netty）
- **集群 (5节点)**：~100MB
- **峰值**：选举期间增加 ~5-10MB

### 12.2 优化重点

1. **对象池化**：复用高频对象
2. **减少拷贝**：使用 Netty 零拷贝
3. **GC 调优**：使用 G1GC，降低暂停
4. **监控告警**：实时监控内存使用

### 12.3 最佳实践

- 堆大小：根据节点数配置 512MB - 2GB
- 使用 G1GC：低延迟场景首选
- 监控直接内存：避免 OutOfDirectMemoryError
- 定期分析内存快照：检测潜在泄漏