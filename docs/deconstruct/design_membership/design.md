# Membership 模块设计文档

## 模块概述

`cn.itcraft.speedboat.membership` 和 `cn.itcraft.speedboat.strategy.membership` 是 Speedboat 的成员管理模块，负责集群节点的健康检测、注册中心集成、成员变更验证和自动故障移除。采用策略模式将三种职责分离为独立接口，通过 `MembershipCoordinator` 协调调度。

## MembershipCoordinator

### 职责

协调健康检测、注册中心、变更验证三个策略组件，管理变更检测任务的生命周期，提供统一的成员变更接口。

### 核心字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `raftNode` | `RaftNode` | 关联的 RaftNode |
| `config` | `MembershipConfig` | 成员管理配置 |
| `healthCheckStrategy` | `HealthCheckStrategy` | 健康检测策略 |
| `registryStrategy` | `RegistryStrategy` | 注册中心策略 |
| `changeValidationStrategy` | `ChangeValidationStrategy` | 变更验证策略 |
| `scheduler` | `ScheduledExecutorService` | 单线程调度器 |
| `running` | `AtomicBoolean` | 运行状态 |
| `detectionFuture` | `volatile ScheduledFuture<?>` | 检测任务句柄 |

### 生命周期

**start()**：启动所有策略组件，若 `config.isMembershipChangeEnabled()` 则启动定时检测任务（周期 = `config.getHealthCheckInterval()`，默认 1000ms）。

**stop()**：取消检测任务，停止所有策略组件，关闭调度器（等待 5 秒）。

### 检测流程

`detectAndProposeChanges()` 仅 Leader 执行：

1. **健康检测**：调用 `healthCheckStrategy.checkUnhealthyPeers(raftNode.getPeerIds())`，对不健康节点调用 `processUnhealthyPeer()`
2. **注册中心差异**：调用 `registryStrategy.getRegisteredPeers()`，与当前成员列表比较，发现新增/移除节点

**processUnhealthyPeer()**：
1. 从 `raftNode.getFailureRecords()` 获取或创建 FailureRecord
2. 递增故障计数
3. 若 `shouldProposeRemoval(failureThreshold, confirmationNanos)` 返回 true，且 `changeValidationStrategy.validateRemove()` 通过，则调用 `raftNode.proposeRemoveMember()`
4. 成功提议后清除故障记录

**detectRegistryChanges()**：
1. 比较注册中心节点列表与当前 peerIds
2. 新节点：验证通过后调用 `raftNode.proposeAddMember()`，并标记为健康
3. 消失节点：若 `registryStrategy.isRegistered()` 返回 true，则按故障处理

### 手动操作

- `proposeAddMember(peerId, address)`：验证后调用 `raftNode.proposeAddMember()`
- `proposeRemoveMember(peerId)`：验证后调用 `raftNode.proposeRemoveMember()`

## HealthCheckStrategy (接口)

### 职责

定义节点健康检测策略。

### 方法

| 方法 | 说明 |
|------|------|
| `Set<String> checkHealth(Set<String> nodeIds)` | 检查节点健康状态，返回健康节点集合 |
| `List<String> checkUnhealthyPeers(List<String> nodeIds)` | default 方法，返回不健康节点列表 |
| `boolean isHealthy(String nodeId)` | default 方法，检查单节点健康状态 |
| `void start()` | 启动检测 |
| `void stop()` | 停止检测 |
| `boolean isRunning()` | 是否运行中 |

### PassiveHealthCheckStrategy (实现)

**职责**：被动健康检测，不主动发起探测，依赖外部手动标记。

**核心数据结构**：`knownHealthyNodes`（`HashSet<String>`），通过 `synchronized` 保证线程安全。

**方法**：
- `checkHealth()`：返回 `nodeIds ∩ knownHealthyNodes` 的交集
- `markHealthy(nodeId)`：添加节点到已知健康集合
- `markUnhealthy(nodeId)`：从已知健康集合移除节点
- `clear()`：清空已知健康集合

**生命周期**：start/stop 管理 `running` 状态，实际不启动定时任务（被动模式）。

## RegistryStrategy (接口)

### 职责

定义注册中心集成策略，用于动态发现集群节点。

### 方法

| 方法 | 说明 |
|------|------|
| `Map<String, String> getAllRegisteredNodes()` | 获取所有注册节点（nodeId -> address） |
| `List<String> getRegisteredPeers()` | default 方法，返回注册节点ID列表 |
| `boolean isRegistered(String nodeId)` | 检查节点是否已注册 |
| `String getAddress(String nodeId)` | 获取节点地址 |
| `boolean isValid(String nodeId, String address)` | 验证节点地址有效性 |
| `void start()` | 启动 |
| `void stop()` | 停止 |

### NoOpRegistryStrategy (实现)

**职责**：空操作注册中心策略，所有方法返回空/null/false。

**使用场景**：不使用注册中心的场景。

## ChangeValidationStrategy (接口)

### 职责

定义成员变更验证策略，控制添加/移除节点的安全条件。

### 方法

| 方法 | 说明 |
|------|------|
| `boolean canProposeChange(RaftNode, MemberChangeEntry)` | Leader 是否可以提议变更 |
| `boolean shouldAcceptChange(RaftNode, MemberChangeEntry)` | Follower 是否应该接受变更 |
| `boolean isQuorumAvailable(RaftNode)` | default 方法，检查法定人数是否可用（默认 true） |
| `boolean validateAdd(String newPeerId, List<String> currentPeerIds)` | default 方法，验证添加：节点不能已存在 |
| `boolean validateRemove(String peerId, List<String> currentPeerIds)` | default 方法，验证移除：节点必须存在 |

### DefaultChangeValidationStrategy (实现)

**canProposeChange()**：仅 Leader 可提议，需法定人数可用

| ChangeType | 条件 |
|------------|------|
| ADD | 总是允许 |
| REMOVE | 不允许移除自己 |

**shouldAcceptChange()**：需法定人数可用

| ChangeType | 条件 |
|------------|------|
| ADD | 总是接受 |
| REMOVE | 不允许移除自己 |

## 线程安全

| 组件 | 机制 |
|------|------|
| `MembershipCoordinator` | `AtomicBoolean` running + `ScheduledExecutorService` 单线程调度 |
| `PassiveHealthCheckStrategy` | `synchronized` 保护 knownHealthyNodes |
| `NoOpRegistryStrategy` | 只读操作，无状态 |
| `DefaultChangeValidationStrategy` | 无状态，方法无副作用 |

## 设计模式

| 模式 | 应用 |
|------|------|
| 策略模式 | HealthCheckStrategy / RegistryStrategy / ChangeValidationStrategy |
| 模板方法 | 接口中 default 方法定义模板算法 |
| 中介者模式 | MembershipCoordinator 协调三个策略组件 |
| 空对象模式 | NoOpRegistryStrategy |

## 扩展点

1. 实现 `HealthCheckStrategy` 接口定制健康检测（如主动 Ping、TCP 连接检测、HTTP 健康端点）
2. 实现 `RegistryStrategy` 接口对接注册中心（Nacos、Redis、Consul、Etcd）
3. 实现 `ChangeValidationStrategy` 接口增加变更安全策略（如集群规模限制、变更频率限制）