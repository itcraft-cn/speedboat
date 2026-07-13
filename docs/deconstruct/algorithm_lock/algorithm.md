# 分布式锁算法

## 概述

Speedboat 分布式锁基于 **Raft 日志一致性** 实现，确保在分布式环境下锁的互斥性和安全性。

**核心特性**：
- 基于 Raft 日志复制的强一致性
- 租约机制防止死锁
- 自动续约后台任务
- 可重入锁支持

---

## 锁状态模型

### 状态定义

```
LockEntry 状态：
+------------------+
| lockName         | 锁名称
| nodeId           | 持有者节点 ID
| held             | 是否被持有
| holdCount        | 重入次数
| leaseExpireTime  | 租约过期时间
+------------------+
```

### 状态转换

```
        +----------+
        |  UNLOCKED |
        +----------+
             |
             | tryLock (成功)
             v
        +----------+
        |  LOCKED   |
        +----------+
             |
        +----+----+
        |         |
        | renew   | unlock
        v         v
   +----------+  +----------+
   |  LOCKED   |  | UNLOCKED |
   | (续约)    |  +----------+
   +----------+
        |
        | 租约过期
        v
   +----------+
   | UNLOCKED |
   +----------+
```

---

## 加锁算法

### tryLock 流程

```
tryLock(timeoutMs):
    startTime = currentTimeMillis()
    deadline = startTime + timeoutMs
    
    while currentTimeMillis() < deadline:
        if tryLockInternal():
            startRenewTask()
            return LockHandle(success=true)
        
        sleep(RETRY_INTERVAL_MS)  // 100ms
    
    return LockHandle(success=false)
```

### tryLockInternal 流程

```
tryLockInternal():
    1. 检查是否为 Leader:
       if !raftNode.isLeader():
           return false  // 非 Leader 不能直接加锁
    
    2. 检查锁可用性:
       if !stateMachine.isLockAvailable(lockName, nodeId):
           return false  // 锁被其他节点持有
    
    3. 构造锁命令:
       command = LockCommand.lock(lockName, nodeId)
       data = serialize(command)
    
    4. 提交到 Raft:
       if !raftNode.propose(data):
           return false  // 提交失败
    
    5. 等待应用:
       return waitForApply(1000)
```

### waitForApply 流程

```
waitForApply(timeoutMs):
    startTime = currentTimeMillis()
    
    while currentTimeMillis() - startTime < timeoutMs:
        if stateMachine.isLockHeldBy(lockName, nodeId):
            return true  // 锁已应用到状态机
        
        sleep(10)
    
    return false  // 超时
```

---

## 解锁算法

### unlock 流程

```
unlock():
    1. 检查锁持有状态:
       if !stateMachine.isLockHeldBy(lockName, nodeId):
           stopRenewTask()
           return
    
    2. 构造解锁命令:
       command = LockCommand.unlock(lockName, nodeId)
       data = serialize(command)
    
    3. 提交到 Raft:
       raftNode.propose(data)
    
    4. 停止续约任务:
       entry = stateMachine.getLockEntry(lockName)
       if entry == null 或 entry.holdCount <= 0:
           stopRenewTask()
```

---

## 租约续约算法

### 自动续约任务

```
startRenewTask():
    if renewing.compareAndSet(false, true):
        renewInterval = LEASE_TIMEOUT_MS / 2  // 15秒
        scheduler.scheduleAtFixedRate(
            { renewLease() },
            renewInterval,
            renewInterval
        )
```

### renewLease 流程

```
renewLease():
    1. 检查 Leader 状态:
       if !raftNode.isLeader():
           return  // 非 Leader 不续约
    
    2. 检查锁持有状态:
       if !stateMachine.isLockHeldBy(lockName, nodeId):
           stopRenewTask()  // 锁丢失
           return
    
    3. 构造续约命令:
       command = LockCommand.renew(lockName, nodeId)
       data = serialize(command)
    
    4. 提交续约:
       raftNode.propose(data)
```

---

## 状态机应用算法

### apply 流程（LockStateMachine）

```
apply(entry):
    1. 类型检查:
       if entry.type != COMMAND:
           return
    
    2. 反序列化命令:
       command = deserialize(entry.data, LockCommand.class)
    
    3. 应用命令:
       applyCommand(command)
    
    4. 更新应用索引:
       lastAppliedIndex = entry.index
```

### applyCommand 流程

```
applyCommand(command):
    lockEntry = lockTable.computeIfAbsent(lockName, LockEntry::new)
    
    switch command.type:
        case LOCK:
            lockEntry.tryAcquire(nodeId, LEASE_TIMEOUT_MS)
        
        case UNLOCK:
            lockEntry.release(nodeId)
        
        case RENEW:
            lockEntry.renew(nodeId, LEASE_TIMEOUT_MS)
```

---

## 可重入锁算法

### tryAcquire 实现

```
LockEntry.tryAcquire(nodeId, leaseTimeoutMs):
    1. 检查租约过期:
       if isLeaseExpired():
           // 租约过期，重置锁
           this.nodeId = nodeId
           this.held = true
           this.holdCount = 1
           this.leaseExpireTime = now + leaseTimeoutMs
           return true
    
    2. 检查是否为当前持有者:
       if this.nodeId.equals(nodeId):
           // 重入
           this.holdCount++
           this.leaseExpireTime = now + leaseTimeoutMs
           return true
    
    3. 其他情况:
       return false  // 锁被其他节点持有
```

### release 实现

```
LockEntry.release(nodeId):
    1. 检查持有者:
       if !this.nodeId.equals(nodeId):
           return  // 非持有者无法释放
    
    2. 递减计数:
       this.holdCount--
    
    3. 检查是否完全释放:
       if this.holdCount <= 0:
           this.held = false
           this.nodeId = null
           this.holdCount = 0
```

---

## 租约过期检测

### isLeaseExpired 实现

```
LockEntry.isLeaseExpired():
    if leaseExpireTime == 0:
        return false  // 未设置租约
    
    return currentTimeMillis() > leaseExpireTime
```

### 租约过期检查时机

| 检查时机 | 说明 |
|----------|------|
| tryLock | 检查锁可用性 |
| isLockHeldBy | 检查持有状态 |
| renewLease | 检查续约条件 |

---

## 时间复杂度

| 操作 | 时间复杂度 | 说明 |
|------|------------|------|
| tryLock | O(1) + 网络延迟 | ConcurrentHashMap 查找 |
| unlock | O(1) + 网络延迟 | 无需等待响应 |
| renewLease | O(1) | 后台异步任务 |
| isLockHeldBy | O(1) | 内存查询 |

---

## 并发安全

### 线程安全保证

| 组件 | 安全机制 |
|------|----------|
| lockTable | ConcurrentHashMap |
| LockEntry | synchronized 方法 |
| renewing | AtomicBoolean |
| holdCount | volatile + synchronized |

### 锁竞争处理

```
加锁失败处理策略：
1. 重试等待（RETRY_INTERVAL_MS = 100ms）
2. 超时返回失败（由调用方决定是否重试）
3. 非阻塞检查（isLockAvailable）
```

---

## 异常场景处理

### Leader 切换

```
场景：加锁后 Leader 切换
处理：
1. 新 Leader 无法续约（raftNode.isLeader() == false）
2. 租约到期后自动释放
3. 原持有者感知锁丢失（isLockHeldBy == false）
```

### 网络分区

```
场景：持有锁的节点被分区
处理：
1. 无法续约（无法提交到 Raft）
2. 租约到期后自动释放
3. 分区恢复后重新竞争
```

### 节点宕机

```
场景：持有锁的节点宕机
处理：
1. 续约任务停止
2. 租约到期后自动释放
3. 其他节点可获取锁
```

---

## 性能指标

### 关键参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| LEASE_TIMEOUT_MS | 30000 ms | 租约超时 |
| WAIT_TIMEOUT_MS | 5000 ms | 加锁等待超时 |
| RETRY_INTERVAL_MS | 100 ms | 重试间隔 |

### 性能数据

| 操作 | 延迟 | 说明 |
|------|------|------|
| tryLock 成功 | 10-50 ms | Raft 日志复制延迟 |
| tryLock 失败 | 100-5000 ms | 取决于等待时间 |
| unlock | 10-50 ms | 异步提交 |
| renewLease | 10-50 ms | 后台异步 |

---

## 设计权衡

| 决策点 | 选择方案 | 优点 | 缺点 |
|--------|----------|------|------|
| 锁实现 | 基于 Raft 日志 | 强一致性 | 性能受限于 Raft |
| 租约机制 | 定时续约 | 防死锁 | 需维护续约任务 |
| 重入性 | 支持 | 便于编程 | 状态复杂度增加 |
| 等待策略 | 自旋重试 | 简单 | CPU 开销 |

---

## 参考

- Raft 论文：In Search of an Understandable Consensus Algorithm
- 分布式锁设计：Redlock 算法
- 相关源码：`src/main/java/cn/itcraft/speedboat/lock/`
