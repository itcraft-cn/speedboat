# Lock 模块设计文档

## 模块概述

`cn.itcraft.speedboat.lock` 是 Speedboat 的分布式锁模块，基于 Raft 共识算法实现可重入分布式锁。通过 `StateMachine` 接口将锁操作集成到 Raft 日志复制流程中，所有锁操作（LOCK/UNLOCK/RENEW）作为 Raft 日志 COMMAND 条目，经共识后应用到状态机，确保分布式一致性。

## 核心类设计

### DistributedLock (接口)

**职责**：定义分布式锁的使用契约，提供简洁的 API。

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `tryLock()` | `LockHandle` | 非阻塞尝试获取锁，使用默认超时（5000ms） |
| `tryLock(long timeoutMs)` | `LockHandle` | 带超时的尝试获取锁 |
| `unlock()` | `void` | 释放锁 |
| `isLocked()` | `boolean` | 检查锁是否被持有 |
| `isHeldByCurrentNode()` | `boolean` | 检查锁是否被当前节点持有 |
| `getLockName()` | `String` | 获取锁名称 |
| `getHolderNodeId()` | `String` | 获取持有者节点ID |

### LockHandle (接口)

**职责**：锁持有句柄，实现 `AutoCloseable`，支持 try-with-resources 语法。

| 方法 | 说明 |
|------|------|
| `isSuccess()` | 获取锁是否成功 |
| `getLockName()` | 获取锁名称 |
| `getNodeId()` | 获取持锁节点ID |
| `close()` | 释放锁（AutoCloseable） |

**设计要点**：`close()` 内部使用 `AtomicBoolean` 保证幂等性，多次调用仅首次执行释放操作。

### DistributedLockImpl

**职责**：分布式锁的核心实现，基于 RaftNode 的 propose 机制实现锁操作。

**配置参数**：

| 常量 | 值 | 说明 |
|------|------|------|
| `DEFAULT_LEASE_TIMEOUT_MS` | 30000 | 默认租约超时（30s） |
| `DEFAULT_WAIT_TIMEOUT_MS` | 5000 | 默认等待超时（5s） |
| `RETRY_INTERVAL_MS` | 100 | 重试间隔（100ms） |

**核心字段**：

| 字段 | 说明 |
|------|------|
| `lockName` | 锁名称（唯一标识） |
| `nodeId` | 当前节点ID |
| `raftNode` | 关联的 RaftNode |
| `stateMachine` | 锁状态机 |
| `serializer` | ProtostuffSerializer 实例 |
| `renewExecutor` | 租约续期调度器（单线程） |
| `renewing` | AtomicBoolean 续期状态标记 |

**tryLock 算法**：
1. 计算 deadline = `startTime + timeoutMs`
2. 循环：检查是否超时
3. 调用 `tryLockInternal()` 尝试获取锁
4. 成功则 `startRenewTask()` 启动租约续期，返回成功 LockHandle
5. 失败则 `Thread.sleep(RETRY_INTERVAL_MS)` 后重试
6. 超时返回失败 LockHandle

**tryLockInternal 算法**：
1. 检查 `raftNode.isLeader()`，非 Leader 返回 false
2. 检查 `stateMachine.isLockAvailable(lockName, nodeId)`，不可用返回 false
3. 构造 `LockCommand.lock(lockName, nodeId)` 并序列化
4. 调用 `raftNode.propose(data)` 提交日志
5. 调用 `waitForApply(1000, entryIndex)` 等待状态机应用（最多 1 秒）

**waitForApply 判定条件（2026-09 修复后）**：`stateMachine.isLockHeldBy(lockName, nodeId)` **且** `raftNode.getLastApplied() >= targetIndex` 同时满足才算获取成功。仅凭"锁被持有"可能读到其他节点的旧锁状态，必须配合日志应用位置确认是自己的 LOCK 命令已生效，消除锁转移竞态。

**租约续期**：
- 续期间隔：`DEFAULT_LEASE_TIMEOUT_MS / 2`（即 15 秒）
- 续期任务：构造 `LockCommand.renew(lockName, nodeId)` 并提交
- 停止条件：节点不再是 Leader，或锁不再被当前节点持有
- 使用 `AtomicBoolean` 防止重复启动

**unlock 算法**：
1. 检查锁是否被当前节点持有，未持有则仅停止续期
2. 构造 `LockCommand.unlock(lockName, nodeId)` 并提交
3. 检查 LockEntry 的 holdCount，为 0 时停止续期任务

### LockCommand

**职责**：锁操作命令，实现 `Serializable`，作为 Raft 日志的 COMMAND 数据载体验证。

**CommandType 枚举**：`LOCK`、`UNLOCK`、`RENEW`

**字段**：`lockName`、`nodeId`、`commandType`、`timestamp`（自动设置 `System.currentTimeMillis()`）

**静态工厂方法**：`lock(lockName, nodeId)`、`unlock(lockName, nodeId)`、`renew(lockName, nodeId)`

### LockEntry

**职责**：单个锁的运行时状态，实现可重入和租约管理。

**线程安全**：`tryAcquire()`、`release()`、`renew()` 使用 `synchronized` 方法级锁，`nodeId`、`holdCount`、`leaseExpireTime` 使用 `volatile` 保证可见性。

**核心字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `lockName` | `String`（final） | 锁名称 |
| `nodeId` | `volatile String` | 当前持有者 |
| `holdCount` | `volatile int` | 重入计数 |
| `leaseExpireTime` | `volatile long` | 租约过期时间（毫秒） |

**关键方法**：

| 方法 | 逻辑 |
|------|------|
| `isHeld()` | nodeId != null && holdCount > 0 |
| `isHeldBy(nodeId)` | 持有者匹配检查 |
| `isLeaseExpired()` | `System.currentTimeMillis() > leaseExpireTime` |
| `tryAcquire(nodeId, leaseTimeoutMs)` | 未持有或租约过期时获取；重入时 holdCount++；持有者不同时拒绝 |
| `release(nodeId)` | 持有者匹配时 holdCount--；holdCount <= 0 时清空状态 |
| `renew(nodeId, leaseTimeoutMs)` | 持有者匹配时更新 leaseExpireTime |

**可重入机制**：`tryAcquire` 中当 `requestNodeId.equals(nodeId)` 时，仅递增 holdCount 并更新租约时间，不更改持有者。

### LockStateMachine

**职责**：实现 `StateMachine` 接口，将锁操作整合到 Raft 状态机中。

**字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `lockTable` | `ConcurrentHashMap<String, LockEntry>` | 锁表 |
| `serializer` | `ProtostuffSerializer` | 反序列化 LockCommand |
| `lastAppliedIndex` | `volatile long` | 最后应用的日志索引 |
| `DEFAULT_LEASE_TIMEOUT_MS` | 30000 | 默认租约超时 |

**apply(LogEntry) 算法**：
1. 过滤：仅处理 `EntryType.COMMAND` 类型条目
2. 使用 `ProtostuffSerializer` 反序列化 `data` 为 `LockCommand`
3. 调用 `applyCommand(command)` 分发
4. 更新 `lastAppliedIndex`

**applyCommand(LockCommand) 算法**：
1. 使用 `lockTable.computeIfAbsent(lockName, LockEntry::new)` 获取或创建 LockEntry
2. 按 CommandType 分发：
   - `LOCK`：调用 `lockEntry.tryAcquire(nodeId, DEFAULT_LEASE_TIMEOUT_MS)`
   - `UNLOCK`：调用 `lockEntry.release(nodeId)`
   - `RENEW`：调用 `lockEntry.renew(nodeId, DEFAULT_LEASE_TIMEOUT_MS)`

**查询方法**：`isLockHeldBy(lockName, nodeId)`、`isLockAvailable(lockName, nodeId)`、`getLockEntry(lockName)`、`getLockCount()`

**快照/恢复**：`snapshot()` 和 `restore()` 当前为空实现（预留扩展）。

## 线程安全

| 组件 | 机制 |
|------|------|
| `LockEntry` | `synchronized` 方法 + `volatile` 字段 |
| `LockStateMachine.lockTable` | `ConcurrentHashMap` |
| `DistributedLockImpl.renewing` | `AtomicBoolean` |
| `LockHandleImpl.closed` | `AtomicBoolean` |
| `LockStateMachine.lastAppliedIndex` | `volatile` |

## 设计模式

| 模式 | 应用 |
|------|------|
| 策略模式 | DistributedLock 接口 + DistributedLockImpl 实现 |
| 状态模式 | LockEntry 管理锁状态（空闲/持有/重入/过期） |
| 模板方法 | StateMachine.apply() 定义处理框架 |
| AutoCloseable | LockHandle 支持 try-with-resources |

## 扩展点

1. 实现 `DistributedLock` 接口可替换锁实现（如基于 Redis 的 RedLock）
2. 实现 `StateMachine` 可扩展更多命令类型
3. 修改 `LockStateMachine` 中 `DEFAULT_LEASE_TIMEOUT_MS` 可调整租约时长