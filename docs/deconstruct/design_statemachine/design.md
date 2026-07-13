# StateMachine 模块设计文档

## 模块概述

`cn.itcraft.speedboat.statemachine` 是 Speedboat 的状态机抽象层，定义 Raft 日志应用、快照和恢复的统一接口。通过状态机模式将 Raft 共识日志与业务逻辑解耦，使分布式锁、配置管理、计数服务等业务无感知地集成到 Raft 复制流程中。

## StateMachine (接口)

### 职责

定义 Raft 状态机的核心契约，是 Raft 日志与业务逻辑之间的桥梁。

### 方法

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `apply(LogEntry entry)` | `void` | 应用一条已提交的 Raft 日志条目 |
| `snapshot(String snapshotPath)` | `void` | 创建状态机快照到指定路径 |
| `restore(String snapshotPath)` | `void` | 从指定路径恢复状态机快照 |
| `getLastAppliedIndex()` | `long` | 获取最后应用的日志索引 |

### 设计考量

**apply(LogEntry) 设计**：
- 仅处理 `EntryType.COMMAND` 类型的日志条目
- `data` 字段承载业务命令的序列化数据
- 实现类负责反序列化 `data` 并分发到具体业务逻辑
- 方法无返回值，状态变更直接体现在状态机内部状态

**snapshot/restore 设计**：
- 提供快照机制用于日志压缩和故障恢复
- 当前版本（1.0.0）为空实现预留
- 快照路径由调用方指定，支持本地文件、远程存储等

**getLastAppliedIndex() 设计**：
- 返回最后成功应用的日志索引
- 用于 Raft 节点重启时确定从何处开始重放日志
- 实现类需保证与 `apply()` 同步更新

### 与 RaftNode 的集成

在 `RaftNode.applyCommittedEntries()` 中调用：

```java
while (lastApplied < commitIndex) {
    lastApplied++;
    LogEntry entry = getEntryAt(lastApplied);
    if (entry != null) {
        if (entry.getEntryType() == LogEntry.EntryType.COMMAND) {
            if (stateMachine != null) {
                stateMachine.apply(entry);
            }
        }
        // ... 处理其他类型
    }
}
```

**关键设计**：`stateMachine` 可为 null，此时 COMMAND 条目被跳过。这允许 RaftNode 在不使用状态机的场景（如纯选举模式）下运行。

### 实现类示例：LockStateMachine

详见 Lock 模块设计文档。`LockStateMachine` 是 StateMachine 的唯一内置实现，将锁操作（LOCK/UNLOCK/RENEW）映射到 `ConcurrentHashMap` 中的 `LockEntry` 状态。

## 设计模式

| 模式 | 应用 |
|------|------|
| 状态模式 | StateMachine 管理业务状态 |
| 命令模式 | LogEntry 封装命令，StateMachine 执行命令 |
| 策略模式 | StateMachine 接口允许不同业务逻辑实现 |

## 线程安全

- `apply()` 方法由 `RaftNode.applyCommittedEntries()` 在 `synchronized` 上下文中调用（间接保证）
- `getLastAppliedIndex()` 返回值需用 `volatile` 保证可见性
- 实现类内部状态（如 `LockStateMachine.lockTable`）需自行保证线程安全

## 扩展点

1. 实现 `StateMachine` 接口创建新的业务状态机（如配置管理、计数器、队列）
2. 实现 `snapshot()` / `restore()` 实现快照持久化
3. 可扩展 `apply()` 处理 `EntryType` 之外的更多条目类型