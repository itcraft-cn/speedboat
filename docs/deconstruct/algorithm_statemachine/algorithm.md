# 状态机算法

## 概述

Speedboat 状态机模块定义了 **日志应用抽象接口**，是 Raft 日志与业务状态之间的桥梁。

**核心职责**：
- 接收已提交的日志条目
- 按顺序应用到业务状态
- 支持快照与恢复（预留接口）

---

## 接口定义

```java
public interface StateMachine {
    
    void apply(LogEntry entry);
    
    void snapshot(String snapshotPath);
    
    void restore(String snapshotPath);
    
    long getLastAppliedIndex();
}
```

### 方法说明

| 方法 | 说明 |
|------|------|
| apply | 应用日志条目到状态机 |
| snapshot | 生成快照到指定路径 |
| restore | 从快照恢复状态 |
| getLastAppliedIndex | 获取最后应用的日志索引 |

---

## 应用算法

### apply 流程

```
apply(entry):
    1. 参数校验:
       if entry == null:
           return
    
    2. 类型过滤:
       if entry.type != COMMAND:
           return  // 忽略配置变更等非命令条目
    
    3. 数据校验:
       if entry.data == null 或 entry.data.length == 0:
           return
    
    4. 业务应用:
       // 子类实现具体逻辑
       applyInternal(entry)
    
    5. 更新索引:
       lastAppliedIndex = entry.index
```

### 调用时机

```
RaftNode                              StateMachine
    |                                      |
    | 日志被提交 (commitIndex 更新)        |
    |------------------------------------->|
    |                                      |
    |                                      | apply(entry)
    |                                      | 更新 lastAppliedIndex
    |                                      |
```

---

## 快照机制（预留）

### snapshot 流程

```
snapshot(snapshotPath):
    1. 获取当前状态快照:
       state = captureCurrentState()
    
    2. 序列化:
       data = serialize(state)
    
    3. 写入文件:
       writeFile(snapshotPath, data)
    
    4. 记录元数据:
       metadata = {
           lastAppliedIndex: lastAppliedIndex,
           timestamp: currentTimeMillis()
       }
       writeMetadata(snapshotPath + ".meta", metadata)
```

### restore 流程

```
restore(snapshotPath):
    1. 读取快照文件:
       data = readFile(snapshotPath)
    
    2. 反序列化:
       state = deserialize(data)
    
    3. 恢复状态:
       restoreState(state)
    
    4. 恢复元数据:
       metadata = readMetadata(snapshotPath + ".meta")
       lastAppliedIndex = metadata.lastAppliedIndex
```

---

## LockStateMachine 实现

### 数据结构

```
LockStateMachine {
    ConcurrentHashMap<String, LockEntry> lockTable
    ProtostuffSerializer serializer
    volatile long lastAppliedIndex
}
```

### apply 实现

```
apply(entry):
    // 1. 基础校验
    if entry == null 或 entry.type != COMMAND:
        return
    
    // 2. 反序列化命令
    command = serializer.deserialize(entry.data, LockCommand.class)
    if command == null:
        return
    
    // 3. 应用命令
    lockEntry = lockTable.computeIfAbsent(lockName, LockEntry::new)
    
    switch command.type:
        case LOCK:
            lockEntry.tryAcquire(nodeId, LEASE_TIMEOUT)
        
        case UNLOCK:
            lockEntry.release(nodeId)
        
        case RENEW:
            lockEntry.renew(nodeId, LEASE_TIMEOUT)
    
    // 4. 更新索引
    lastAppliedIndex = entry.index
```

---

## 状态查询接口

### isLockHeldBy

```
isLockHeldBy(lockName, nodeId):
    entry = lockTable.get(lockName)
    
    if entry == null:
        return false
    
    if entry.isLeaseExpired():
        return false
    
    return entry.isHeldBy(nodeId)
```

### isLockAvailable

```
isLockAvailable(lockName, nodeId):
    entry = lockTable.get(lockName)
    
    if entry == null:
        return true
    
    if entry.isLeaseExpired():
        return true
    
    return entry.isHeldBy(nodeId)
```

---

## 线程安全

### 并发访问保证

| 组件 | 安全机制 |
|------|----------|
| lockTable | ConcurrentHashMap |
| lastAppliedIndex | volatile |
| LockEntry 操作 | synchronized 方法 |

### 调用上下文

```
状态机调用模型：

RaftNode 应用线程
    |
    v
StateMachine.apply()  // 单线程顺序调用
    |
    v
更新 lockTable        // 无需额外同步
```

---

## 性能分析

### 时间复杂度

| 操作 | 复杂度 | 说明 |
|------|--------|------|
| apply | O(1) | ConcurrentHashMap 查找 |
| isLockHeldBy | O(1) | 内存查询 |
| snapshot | O(n) | n = 状态大小 |
| restore | O(n) | n = 快照大小 |

### 内存占用

```
LockStateMachine 内存占用：
- lockTable: 每个锁约 100-200 bytes
- serializer: 单例，约 1 KB
- lastAppliedIndex: 8 bytes
```

---

## 扩展点

### 自定义状态机

```java
public class MyStateMachine implements StateMachine {
    
    private final MyState state = new MyState();
    private volatile long lastAppliedIndex = 0;
    
    @Override
    public void apply(LogEntry entry) {
        MyCommand cmd = deserialize(entry.getData());
        state.apply(cmd);
        lastAppliedIndex = entry.getIndex();
    }
    
    @Override
    public void snapshot(String path) {
        // 自定义快照逻辑
    }
    
    @Override
    public void restore(String path) {
        // 自定义恢复逻辑
    }
    
    @Override
    public long getLastAppliedIndex() {
        return lastAppliedIndex;
    }
}
```

---

## 与 Raft 交互

```
+------------------+     apply()      +------------------+
|    RaftNode      |---------------->|  StateMachine    |
+------------------+                  +------------------+
        |                                     |
        | commitIndex 更新                    |
        |                                     |
        | lastApplied < commitIndex           |
        |                                     |
        | 读取 log[lastApplied + 1]           |
        |                                     |
        | apply(entry)                        |
        |------------------------------------>|
        |                                     |
        |                                     | 更新内部状态
        |                                     | 更新 lastAppliedIndex
        |                                     |
```

---

## 设计权衡

| 决策点 | 选择方案 | 优点 | 缺点 |
|--------|----------|------|------|
| 接口抽象 | 接口定义 | 可扩展多种状态机 | 需子类实现 |
| 快照机制 | 预留接口 | 未来可扩展 | 当前未实现 |
| 应用顺序 | 单线程顺序 | 简化并发控制 | 吞吐受限 |

---

## 参考

- Raft 论文：状态机复制
- 相关源码：`src/main/java/cn/itcraft/speedboat/statemachine/`
- 实现示例：`src/main/java/cn/itcraft/speedboat/lock/LockStateMachine.java`
