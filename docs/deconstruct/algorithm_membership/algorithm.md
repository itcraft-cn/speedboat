# 成员管理算法

## 概述

Speedboat 成员管理模块提供 **动态成员变更** 能力，支持运行时添加/移除节点，并结合健康检测和注册中心策略。

**核心特性**：
- 自动健康检测
- 注册中心集成
- 变更验证策略
- 故障记录与阈值触发

---

## 架构设计

### 组件关系

```
+------------------------+
| MembershipCoordinator  |
+------------------------+
         |
         +--> HealthCheckStrategy
         |
         +--> RegistryStrategy
         |
         +--> ChangeValidationStrategy
         |
         v
+------------------------+
|       RaftNode         |
+------------------------+
```

### 策略接口

| 策略 | 职责 | 实现类 |
|------|------|--------|
| HealthCheckStrategy | 健康检测 | PassiveHealthCheckStrategy |
| RegistryStrategy | 注册中心 | NoOpRegistryStrategy |
| ChangeValidationStrategy | 变更验证 | DefaultChangeValidationStrategy |

---

## 协调器生命周期

### 启动流程

```
start():
    if running.compareAndSet(false, true):
        // 1. 启动策略组件
        healthCheckStrategy.start()
        registryStrategy.start()
        
        // 2. 启动检测任务
        if config.isMembershipChangeEnabled():
            detectionFuture = scheduler.scheduleAtFixedRate(
                { detectAndProposeChanges() },
                config.healthCheckInterval,
                config.healthCheckInterval
            )
```

### 停止流程

```
stop():
    if running.compareAndSet(true, false):
        // 1. 取消检测任务
        if detectionFuture != null:
            detectionFuture.cancel(false)
        
        // 2. 停止策略组件
        healthCheckStrategy.stop()
        registryStrategy.stop()
        
        // 3. 关闭调度器
        scheduler.shutdown()
```

---

## 变更检测算法

### detectAndProposeChanges

```
detectAndProposeChanges():
    // 只有 Leader 可以提议变更
    if !raftNode.isLeader():
        return
    
    // 1. 执行健康检测
    unhealthyPeers = healthCheckStrategy.checkUnhealthyPeers(raftNode.getPeerIds())
    for peerId in unhealthyPeers:
        processUnhealthyPeer(peerId)
    
    // 2. 检查注册中心差异
    registeredPeers = registryStrategy.getRegisteredPeers()
    if registeredPeers != null:
        detectRegistryChanges(registeredPeers)
```

---

## 不健康节点处理

### processUnhealthyPeer

```
processUnhealthyPeer(peerId):
    // 1. 获取或创建故障记录
    record = failureRecords.computeIfAbsent(peerId, FailureRecord::new)
    
    // 2. 增加故障计数
    record.incrementFailures()
    
    // 3. 检查是否应该提议移除
    if record.shouldProposeRemoval(config.failureThreshold, config.confirmationNanos):
        
        // 4. 验证是否允许移除
        if changeValidationStrategy.validateRemove(peerId, raftNode.getPeerIds()):
            success = raftNode.proposeRemoveMember(peerId)
            if success:
                failureRecords.remove(peerId)
```

### 故障记录结构

```
FailureRecord {
    String peerId
    int failureCount
    long lastFailureTime
    long firstFailureTime
}
```

### shouldProposeRemoval 算法

```
shouldProposeRemoval(threshold, confirmationNanos):
    // 1. 故障次数达到阈值
    if failureCount < threshold:
        return false
    
    // 2. 确认时间窗口内持续故障
    elapsedNanos = now - firstFailureTime
    if elapsedNanos < confirmationNanos:
        return false
    
    return true
```

---

## 注册中心变更检测

### detectRegistryChanges

```
detectRegistryChanges(registeredPeers):
    currentPeers = raftNode.getPeerIds()
    
    // 1. 发现新节点
    for peerId in registeredPeers:
        if !currentPeers.contains(peerId) && !peerId.equals(raftNode.getNodeId()):
            if changeValidationStrategy.validateAdd(peerId, currentPeers):
                success = raftNode.proposeAddMember(peerId)
                if success:
                    healthCheckStrategy.markHealthy(peerId)
    
    // 2. 发现已移除节点
    for peerId in currentPeers:
        if !registeredPeers.contains(peerId):
            if registryStrategy.isRegistered(peerId):
                processUnhealthyPeer(peerId)
```

---

## 手动成员变更

### proposeAddMember

```
proposeAddMember(peerId, address):
    // 1. Leader 检查
    if !raftNode.isLeader():
        return false
    
    // 2. 变更验证
    if !changeValidationStrategy.validateAdd(peerId, raftNode.getPeerIds()):
        return false
    
    // 3. 提交变更
    success = raftNode.proposeAddMember(peerId)
    
    // 4. 标记健康
    if success:
        healthCheckStrategy.markHealthy(peerId)
    
    return success
```

### proposeRemoveMember

```
proposRemoveMember(peerId):
    // 1. Leader 检查
    if !raftNode.isLeader():
        return false
    
    // 2. 变更验证
    if !changeValidationStrategy.validateRemove(peerId, raftNode.getPeerIds()):
        return false
    
    // 3. 提交变更
    return raftNode.proposeRemoveMember(peerId)
```

---

## 被动健康检测策略

### PassiveHealthCheckStrategy

```
PassiveHealthCheckStrategy {
    Set<String> healthyNodes  // 健康节点集合
    Set<String> unhealthyNodes  // 不健康节点集合
}
```

### checkUnhealthyPeers

```
checkUnhealthyPeers(peerIds):
    return peerIds.stream()
        .filter(id -> unhealthyNodes.contains(id))
        .collect(toList())
```

### markHealthy / markUnhealthy

```
markHealthy(nodeId):
    unhealthyNodes.remove(nodeId)
    healthyNodes.add(nodeId)

markUnhealthy(nodeId):
    healthyNodes.remove(nodeId)
    unhealthyNodes.add(nodeId)
```

**调用时机**：
- 心跳成功 → markHealthy
- 心跳超时 → markUnhealthy

---

## 变更验证策略

### DefaultChangeValidationStrategy

```
validateAdd(peerId, currentPeers):
    // 1. 不重复添加
    if currentPeers.contains(peerId):
        return false
    
    // 2. 不超过最大节点数（可选）
    if currentPeers.size() >= MAX_MEMBERS:
        return false
    
    return true

validateRemove(peerId, currentPeers):
    // 1. 节点存在
    if !currentPeers.contains(peerId):
        return false
    
    // 2. 移除后仍满足多数派
    remainingCount = currentPeers.size() - 1
    majorityCount = (remainingCount / 2) + 1
    if remainingCount < majorityCount:
        return false
    
    return true
```

---

## 配置参数

### MembershipConfig

| 参数 | 默认值 | 说明 |
|------|--------|------|
| membershipChangeEnabled | true | 是否启用成员变更 |
| healthCheckInterval | 5000 ms | 健康检测间隔 |
| failureThreshold | 3 | 故障次数阈值 |
| confirmationNanos | 10s | 确认时间窗口 |

---

## 时序图

### 自动移除不健康节点

```
MembershipCoordinator       HealthCheckStrategy       RaftNode
        |                          |                     |
        | detectAndProposeChanges  |                     |
        |------------------------->|                     |
        |                          |                     |
        | checkUnhealthyPeers      |                     |
        |------------------------->|                     |
        |                          |                     |
        |<-------------------------|  unhealthyPeers     |
        |                          |                     |
        | processUnhealthyPeer     |                     |
        |---------------------------------------------->|
        |                          |                     |
        |                          |   proposeRemoveMember
        |                          |<--------------------|
        |                          |                     |
```

---

## 设计权衡

| 决策点 | 选择方案 | 优点 | 缺点 |
|--------|----------|------|------|
| 健康检测 | 被动标记 | 无额外探测流量 | 依赖外部调用 |
| 变更提议 | Leader 独占 | 避免冲突 | Leader 单点 |
| 故障判定 | 阈值+时间窗口 | 避免误判 | 需调参 |

---

## 参考

- Raft 成员变更论文
- 相关源码：`src/main/java/cn/itcraft/speedboat/membership/`
- 策略实现：`src/main/java/cn/itcraft/speedboat/strategy/membership/`
