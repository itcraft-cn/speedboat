# Speedboat 分布式锁设计

## 1. 需求概述

基于 Speedboat 现有 Raft 实现，添加分布式锁功能：

- **强一致性**：锁操作通过 Raft 日志复制
- **可重入**：同 nodeId 可多次 lock
- **快速失败**：跨 nodeId 快速失败，不阻塞
- **租约机制**：TTL + 自动续约

## 2. API 设计

### 2.1 极简 API

```java
// 获取锁实例（传入锁名）
DistributedLock lock = Speedboat.getLock("myLock");

// 尝试获取锁（快速失败）
boolean success = lock.tryLock(10, TimeUnit.SECONDS);  // TTL 10秒

// 释放锁
lock.unlock();

// try-with-resources 自动释放
try (LockHandle handle = lock.acquire(10, TimeUnit.SECONDS)) {
    if (handle != null) {
        // 持有锁，执行业务
    }
}
```

### 2.2 DistributedLock 接口

```java
public interface DistributedLock {
    
    String getLockName();
    
    boolean tryLock(long ttl, TimeUnit unit);
    
    boolean tryLock();
    
    void unlock();
    
    LockHandle acquire(long ttl, TimeUnit unit);
    
    boolean isLocked();
    
    boolean isHeldByCurrentNode();
    
    int getHoldCount();
}
```

### 2.3 LockHandle 接口

```java
public interface LockHandle extends AutoCloseable {
    
    String getLockName();
    
    boolean isValid();
    
    @Override
    void close();  // 自动 unlock
}
```

## 3. 核心架构

### 3.1 数据流

```
┌──────────────────────────────────────────────────────────────────┐
│                        Lock 操作流程                              │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Client                                                          │
│    ↓                                                             │
│  DistributedLock.tryLock(name, ttl)                             │
│    ↓                                                             │
│  构建 LockCommand(LOCK, name, nodeId, ttl)                      │
│    ↓                                                             │
│  RaftNode.propose(command)                                       │
│    ↓                                                             │
│  LogEntry(COMMAND, LockCommand)                                  │
│    ↓                                                             │
│  AppendEntries RPC → Follower                                    │
│    ↓                                                             │
│  多数派确认 → commitIndex 推进                                    │
│    ↓                                                             │
│  applyCommittedEntries()                                         │
│    ↓                                                             │
│  LockStateMachine.apply(LockCommand)                             │
│    ↓                                                             │
│  更新内存锁状态                                                   │
│    ↓                                                             │
│  返回结果（成功/失败）                                            │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 3.2 组件关系

```
┌───────────────┐
│   Speedboat   │ ← 门面类
└───────┬───────┘
        │
        ├─────────────────────────┐
        │                         │
        ▼                         ▼
┌───────────────┐         ┌───────────────────┐
│   RaftNode    │         │ DistributedLock   │
│   (现有)      │         │ (新增)            │
└───────┬───────┘         └─────────┬─────────┘
        │                           │
        │ propose(Command)          │
        ▼                           ▼
┌───────────────┐         ┌───────────────────┐
│   LogEntry    │         │ LockStateMachine  │
│ (扩展COMMAND) │         │ (新增)            │
└───────┬───────┘         └─────────┬─────────┘
        │                           │
        │ data = LockCommand        │ ConcurrentHashMap
        ▼                           ▼
┌─────────────────────────────────────────────┐
│               LockEntry                      │
│  - nodeId: String                            │
│  - locked: AtomicBoolean                     │
│  - holdCount: int                            │
│  - leaseExpireTime: long                     │
└─────────────────────────────────────────────┘
```

## 4. 数据结构

### 4.1 LockCommand

```java
public class LockCommand {
    
    public enum Type {
        LOCK,       // 获取锁
        UNLOCK,     // 释放锁
        RENEW       // 续约
    }
    
    private Type type;
    private String lockName;
    private String nodeId;
    private long ttlMs;         // TTL（毫秒）
    private long timestamp;     // 命令时间戳
    
    // 构造器、getter
}
```

### 4.2 LockEntry（内存锁状态）

```java
public class LockEntry {
    
    private final String lockName;
    private volatile String nodeId;           // 持有者 nodeId
    private final AtomicInteger holdCount;    // 重入计数
    private volatile long leaseExpireTime;    // 租约过期时间
    
    // 方法
    boolean isLocked();
    boolean isHeldBy(String nodeId);
    boolean isExpired();
    void lock(String nodeId, long ttlMs);
    void unlock();
    void renew(long ttlMs);
}
```

### 4.3 LockStateMachine

```java
public class LockStateMachine {
    
    private final ConcurrentHashMap<String, LockEntry> locks;
    private final ScheduledExecutorService leaseRenewer;
    
    // 应用命令
    boolean apply(LockCommand command);
    
    // 查询
    LockEntry getLock(String lockName);
    boolean isLocked(String lockName);
    boolean isHeldBy(String lockName, String nodeId);
    
    // 租约续约（后台线程）
    void startLeaseRenewer();
    void renewLeases();
}
```

## 5. 扩展 LogEntry

### 5.1 新增 EntryType

```java
public enum EntryType {
    LEADER_INFO,      // Leader 标识（现有）
    MEMBER_CHANGE,    // 成员变更（现有）
    COMMAND           // 命令（新增）
}
```

### 5.2 新增 data 字段

```java
public class LogEntry {
    
    private long index;
    private long term;
    private String leaderId;
    private EntryType entryType;
    private byte[] data;        // 新增：命令数据（序列化）
    
    // COMMAND 类型构造器
    public static LogEntry command(long index, long term, String leaderId, byte[] data);
}
```

## 6. 扩展 RaftNode

### 6.1 新增 propose(Command) 方法

```java
public class RaftNode {
    
    private StateMachine stateMachine;  // 新增
    
    // 新增方法
    public boolean propose(LockCommand command) {
        // 1. 检查是否是 Leader
        if (!isLeader()) {
            return false;
        }
        
        // 2. 序列化命令
        byte[] data = serializer.serialize(command);
        
        // 3. 构建日志条目
        long newIndex = getLastLogIndex() + 1;
        LogEntry entry = LogEntry.command(newIndex, term.getCurrent(), nodeId, data);
        
        // 4. 添加到本地日志
        log.add(entry);
        
        // 5. 复制到 Follower
        sendAppendEntries();
        
        // 6. 等待提交（异步）
        // 通过 applyCommittedEntries() 触发 StateMachine.apply()
        
        return true;
    }
    
    // 修改 applyCommittedEntries()
    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = getEntryAt(lastApplied);
            
            if (entry.getEntryType() == EntryType.LEADER_INFO) {
                leaderId = entry.getLeaderId();
            } else if (entry.getEntryType() == EntryType.COMMAND) {
                // 新增：应用命令
                LockCommand command = serializer.deserialize(entry.getData());
                stateMachine.apply(command);
            } else if (entry.getEntryType() == EntryType.MEMBER_CHANGE) {
                processMemberChange((MemberChangeEntry) entry);
            }
        }
    }
}
```

## 7. 状态机接口

### 7.1 StateMachine 接口

```java
public interface StateMachine {
    
    void apply(byte[] data);
}
```

### 7.2 LockStateMachine 实现

```java
public class LockStateMachine implements StateMachine {
    
    private final ConcurrentHashMap<String, LockEntry> locks;
    
    @Override
    public void apply(byte[] data) {
        LockCommand command = serializer.deserialize(data);
        apply(command);
    }
    
    boolean apply(LockCommand command) {
        String lockName = command.getLockName();
        String nodeId = command.getNodeId();
        
        LockEntry entry = locks.computeIfAbsent(lockName, LockEntry::new);
        
        switch (command.getType()) {
            case LOCK:
                // 检查是否可获取
                if (entry.isLocked() && !entry.isHeldBy(nodeId)) {
                    return false;  // 快速失败
                }
                
                // 可重入或首次获取
                if (entry.isHeldBy(nodeId)) {
                    entry.incrementHoldCount();
                } else {
                    entry.lock(nodeId, command.getTtlMs());
                }
                return true;
                
            case UNLOCK:
                // 只有持有者才能释放
                if (!entry.isHeldBy(nodeId)) {
                    return false;
                }
                
                entry.decrementHoldCount();
                if (entry.getHoldCount() == 0) {
                    entry.unlock();
                }
                return true;
                
            case RENEW:
                if (!entry.isHeldBy(nodeId)) {
                    return false;
                }
                entry.renew(command.getTtlMs());
                return true;
        }
        
        return false;
    }
}
```

## 8. 租约续约

### 8.1 自动续约机制

```java
public class DistributedLockImpl implements DistributedLock {
    
    private final RaftNode raftNode;
    private final String lockName;
    private final String nodeId;
    private volatile long ttlMs;
    private volatile ScheduledFuture<?> renewTask;
    
    @Override
    public boolean tryLock(long ttl, TimeUnit unit) {
        this.ttlMs = unit.toMillis(ttl);
        
        LockCommand command = new LockCommand(
            LockCommand.Type.LOCK, lockName, nodeId, ttlMs
        );
        
        boolean success = raftNode.proposeAndWait(command);
        
        if (success) {
            // 启动自动续约
            startAutoRenew();
        }
        
        return success;
    }
    
    private void startAutoRenew() {
        // 每 TTL/2 时间续约一次
        long renewInterval = ttlMs / 2;
        
        renewTask = scheduler.scheduleAtFixedRate(() -> {
            LockCommand renew = new LockCommand(
                LockCommand.Type.RENEW, lockName, nodeId, ttlMs
            );
            raftNode.propose(renew);
        }, renewInterval, renewInterval, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public void unlock() {
        // 停止续约
        if (renewTask != null) {
            renewTask.cancel(false);
        }
        
        LockCommand command = new LockCommand(
            LockCommand.Type.UNLOCK, lockName, nodeId, 0
        );
        
        raftNode.propose(command);
    }
}
```

## 9. 集成到 Speedboat

### 9.1 Speedboat 门面扩展

```java
public class Speedboat {
    
    private static volatile Speedboat instance;
    private final LockStateMachine lockStateMachine;
    
    // 新增：获取锁实例
    public static DistributedLock getLock(String lockName) {
        checkSingletonRunning();
        return instance.doGetLock(lockName);
    }
    
    private DistributedLock doGetLock(String lockName) {
        return new DistributedLockImpl(lockName, nodeId, raftNode, lockStateMachine);
    }
    
    // 新增：查询锁状态
    public static boolean isLockHeld(String lockName) {
        checkSingletonRunning();
        return instance.lockStateMachine.isLocked(lockName);
    }
    
    public static String getLockOwner(String lockName) {
        checkSingletonRunning();
        LockEntry entry = instance.lockStateMachine.getLock(lockName);
        return entry != null ? entry.getNodeId() : null;
    }
}
```

## 10. 测试设计

### 10.1 单元测试

```java
// LockCommandTest - 命令序列化
@Test
void testLockCommandSerialization() {
    LockCommand cmd = new LockCommand(LOCK, "test", "node1", 10000);
    byte[] data = serializer.serialize(cmd);
    LockCommand decoded = serializer.deserialize(data);
    assertEquals(cmd, decoded);
}

// LockStateMachineTest - 状态机逻辑
@Test
void testLockAndUnlock() {
    LockStateMachine sm = new LockStateMachine();
    
    // 获取锁
    LockCommand lock = new LockCommand(LOCK, "test", "node1", 10000);
    assertTrue(sm.apply(lock));
    
    // 同 nodeId 可重入
    assertTrue(sm.apply(lock));
    assertEquals(2, sm.getLock("test").getHoldCount());
    
    // 不同 nodeId 快速失败
    LockCommand lock2 = new LockCommand(LOCK, "test", "node2", 10000);
    assertFalse(sm.apply(lock2));
    
    // 释放锁
    LockCommand unlock1 = new LockCommand(UNLOCK, "test", "node1", 0);
    assertTrue(sm.apply(unlock1));
    assertEquals(1, sm.getLock("test").getHoldCount());
    
    assertTrue(sm.apply(unlock1));
    assertFalse(sm.getLock("test").isLocked());
}
```

### 10.2 集成测试

```java
// DistributedLockIntegrationTest - 集群测试
@Test
void testDistributedLockAcrossNodes() throws Exception {
    // 启动 3 个节点
    TestCluster cluster = new TestCluster(3);
    cluster.start();
    
    // 等待 Leader 选举
    cluster.waitForLeader();
    
    // node1 获取锁
    DistributedLock lock1 = cluster.getNode(0).getLock("test");
    assertTrue(lock1.tryLock(10, TimeUnit.SECONDS));
    
    // node2 快速失败
    DistributedLock lock2 = cluster.getNode(1).getLock("test");
    assertFalse(lock2.tryLock(10, TimeUnit.SECONDS));
    
    // node1 释放锁
    lock1.unlock();
    
    // node2 现在可以获取
    assertTrue(lock2.tryLock(10, TimeUnit.SECONDS));
    
    cluster.shutdown();
}
```

## 11. 线程命名规范

### 11.1 ThreadFactory 设计

所有线程必须使用 `ThreadFactory` 命名，格式：

```
Speedboat-{component}-{nodeId}
```

示例：
- `Speedboat-LeaseRenewer-node1` - 租约续约线程
- `Speedboat-Election-node2` - 选举超时线程
- `Speedboat-Heartbeat-node1` - 心跳线程

### 11.2 NamedThreadFactory 实现

```java
public class NamedThreadFactory implements ThreadFactory {
    
    private final String namePrefix;
    private final AtomicInteger counter = new AtomicInteger(1);
    private final boolean daemon;
    
    public NamedThreadFactory(String component, String nodeId, boolean daemon) {
        this.namePrefix = "Speedboat-" + component + "-" + nodeId;
        this.daemon = daemon;
    }
    
    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, namePrefix + "-" + counter.getAndIncrement());
        thread.setDaemon(daemon);
        return thread;
    }
}
```

### 11.3 使用示例

```java
// 租约续约线程池
ThreadFactory leaseFactory = new NamedThreadFactory("LeaseRenewer", nodeId, true);
ScheduledExecutorService leaseRenewer = Executors.newSingleThreadScheduledExecutor(leaseFactory);

// RaftNode 选举线程池（改造现有代码）
ThreadFactory raftFactory = new NamedThreadFactory("Raft", nodeId, true);
scheduler = Executors.newScheduledThreadPool(2, raftFactory);
```

## 12. 性能考量

### 11.1 延迟

- **lock 操作**：需要 Raft 日志复制（多数派确认），延迟 = RTT × 2
- **本地网络**：~1-5ms
- **跨机房**：~50-200ms（取决于网络）

### 11.2 吞吐

- **单 Leader**：所有锁操作经过 Leader，吞吐受限于 Leader 处理能力
- **优化方向**：批量提交（batch propose）、异步 apply

### 11.3 内存

- **锁状态**：`ConcurrentHashMap` 内存存储，无持久化
- **Leader 故障恢复**：新 Leader 从 Raft 日志重建锁状态

## 13. 边界情况

### 12.1 Leader 故障

- Leader 故障时，锁状态丢失（内存）
- 新 Leader 从 Raft 日志重建锁状态
- 未提交的锁请求可能丢失（客户端需重试）

### 12.2 租约过期

- 客户端故障时，锁自动释放
- TTL 到期后，锁变为可获取

### 12.3 网络分区

- 分区期间，少数派无法获取锁（无法达成多数派）
- 分区恢复后，锁状态恢复

## 14. 实现优先级

1. **高优先级**：
   - LockCommand 序列化
   - LockStateMachine 核心逻辑
   - RaftNode.propose() 扩展
   - DistributedLock 实现

2. **中优先级**：
   - 自动续约机制
   - try-with-resources 支持

3. **低优先级**：
   - 性能优化（批量提交）
   - 监控指标

## 15. 文件清单

新增文件：
- `src/main/java/cn/itcraft/speedboat/lock/DistributedLock.java`
- `src/main/java/cn/itcraft/speedboat/lock/DistributedLockImpl.java`
- `src/main/java/cn/itcraft/speedboat/lock/LockHandle.java`
- `src/main/java/cn/itcraft/speedboat/lock/LockCommand.java`
- `src/main/java/cn/itcraft/speedboat/lock/LockEntry.java`
- `src/main/java/cn/itcraft/speedboat/lock/LockStateMachine.java`
- `src/main/java/cn/itcraft/speedboat/statemachine/StateMachine.java`
- `src/main/java/cn/itcraft/speedboat/util/NamedThreadFactory.java`

修改文件：
- `src/main/java/cn/itcraft/speedboat/raft/LogEntry.java` - 添加 COMMAND 类型、data 字段
- `src/main/java/cn/itcraft/speedboat/raft/RaftNode.java` - 添加 propose()、StateMachine、使用 NamedThreadFactory
- `src/main/java/cn/itcraft/speedboat/Speedboat.java` - 添加 getLock() 方法
- `src/main/java/cn/itcraft/speedboat/strategy/membership/impl/PassiveHealthCheckStrategy.java` - 使用 NamedThreadFactory

测试文件：
- `src/test/java/cn/itcraft/speedboat/lock/LockCommandTest.java`
- `src/test/java/cn/itcraft/speedboat/lock/LockStateMachineTest.java`
- `src/test/java/cn/itcraft/speedboat/lock/DistributedLockTest.java`
- `src/test/java/cn/itcraft/speedboat/lock/DistributedLockIntegrationTest.java`