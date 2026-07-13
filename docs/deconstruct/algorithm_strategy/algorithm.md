# 策略算法

## 概述

Speedboat 策略模块采用 **策略模式** 抽象可变行为，支持运行时切换不同的算法实现。

**策略分类**：
- VoteWeightStrategy：投票权重计算
- GroupStrategy：分组配置策略
- HealthCheckStrategy：健康检测策略
- RegistryStrategy：注册中心策略
- ChangeValidationStrategy：变更验证策略

---

## 投票权重策略

### 接口定义

```java
public interface VoteWeightStrategy {
    int calculateAdditionalWeight(VoteContext context);
}
```

### VoteContext 结构

```
VoteContext {
    String candidateId
    boolean startupFirst
    boolean electionInitiator
    String candidateDatacenter
    int peerCount
}
```

---

### EvenNodeVoteWeightStrategy

**用途**：解决偶数节点平票问题

**算法**：

```
calculateAdditionalWeight(context):
    weight = 0
    
    // 1. 启动优先权
    if context.isStartupFirst():
        weight += 1
    
    // 2. 选举发起者优先权
    if context.isElectionInitiator():
        weight += 1
    
    return weight
```

**应用场景**：
- 2 节点集群：启动优先节点获得 +1 权重
- 4 节点集群：选举发起者获得 +1 权重

---

### DatacenterVoteWeightStrategy

**用途**：跨机房选举优先本地机房

**算法**：

```
calculateAdditionalWeight(context):
    candidateDc = context.getCandidateDatacenter()
    
    if candidateDc == null:
        return 0
    
    // 同机房优先
    if candidateDc.equals(localDatacenter):
        return 2
    
    // 异机房降低权重
    return -1
```

**应用场景**：
- 同城双活：优先本地机房
- 异地多活：避免跨机房 Leader

---

### CompositeVoteWeightStrategy

**用途**：组合多个权重策略

**算法**：

```
calculateAdditionalWeight(context):
    return strategies.stream()
        .mapToInt(s -> s.calculateAdditionalWeight(context))
        .sum()
```

**组合示例**：

```
// 偶数节点 + 跨机房组合
strategy = CompositeVoteWeightStrategy(
    EvenNodeVoteWeightStrategy(),
    DatacenterVoteWeightStrategy("dc1")
)

// 计算结果 = EvenNode权重 + Datacenter权重
```

---

## 分组策略

### 接口定义

```java
public interface GroupStrategy {
    long getElectionTimeout();
    long getHeartbeatInterval();
    boolean allowCrossGroupCommunication();
    long getMinElectionTimeout();
    long getMaxElectionTimeout();
}
```

---

### DefaultGroupStrategy

**用途**：单机房默认配置

**配置**：

| 参数 | 值 | 说明 |
|------|-----|------|
| electionTimeout | 150-300 ms | 快速选举 |
| heartbeatInterval | 50 ms | 高频心跳 |
| crossGroupComm | false | 无跨组通信 |

---

### DatacenterGroupStrategy

**用途**：机房级分组

**配置**：

| 参数 | 值 | 说明 |
|------|-----|------|
| electionTimeout | 150-300 ms | 子组内快速选举 |
| heartbeatInterval | 50 ms | 子组内心跳 |
| crossGroupComm | true | 支持父组选举 |

**层级架构**：

```
+------------------+
|  Global Group    |  (electionTimeout: 500-1000ms)
+------------------+
         |
    +----+----+
    |         |
+-------+ +-------+
| DC-1  | | DC-2  |  (electionTimeout: 150-300ms)
+-------+ +-------+
```

---

### GlobalGroupStrategy

**用途**：全局组配置

**配置**：

| 参数 | 值 | 说明 |
|------|-----|------|
| electionTimeout | 500-1000 ms | 慢速选举避免冲突 |
| heartbeatInterval | 200 ms | 低频心跳 |
| crossGroupComm | false | 无需跨组 |

---

## 健康检测策略

### 接口定义

```java
public interface HealthCheckStrategy {
    void start();
    void stop();
    List<String> checkUnhealthyPeers(List<String> peerIds);
    void markHealthy(String nodeId);
    void markUnhealthy(String nodeId);
}
```

---

### PassiveHealthCheckStrategy

**算法**：

```
checkUnhealthyPeers(peerIds):
    return peerIds.stream()
        .filter(id -> unhealthyNodes.contains(id))
        .collect(toList())

markHealthy(nodeId):
    unhealthyNodes.remove(nodeId)
    healthyNodes.add(nodeId)

markUnhealthy(nodeId):
    healthyNodes.remove(nodeId)
    unhealthyNodes.add(nodeId)
```

**调用时机**：
- 心跳成功 → `markHealthy(nodeId)`
- 心跳超时 → `markUnhealthy(nodeId)`

---

## 注册中心策略

### 接口定义

```java
public interface RegistryStrategy {
    void start();
    void stop();
    List<String> getRegisteredPeers();
    boolean isRegistered(String nodeId);
}
```

---

### NoOpRegistryStrategy

**用途**：无注册中心的默认实现

**算法**：

```
getRegisteredPeers():
    return null  // 不提供注册中心信息

isRegistered(nodeId):
    return false
```

---

## 变更验证策略

### 接口定义

```java
public interface ChangeValidationStrategy {
    boolean validateAdd(String nodeId, List<String> currentPeers);
    boolean validateRemove(String nodeId, List<String> currentPeers);
}
```

---

### DefaultChangeValidationStrategy

**算法**：

```
validateAdd(nodeId, currentPeers):
    // 1. 不重复添加
    if currentPeers.contains(nodeId):
        return false
    
    // 2. 不超过最大节点数
    if currentPeers.size() >= MAX_MEMBERS:
        return false
    
    return true

validateRemove(nodeId, currentPeers):
    // 1. 节点存在
    if !currentPeers.contains(nodeId):
        return false
    
    // 2. 移除后仍满足多数派
    remainingCount = currentPeers.size() - 1
    if remainingCount < 1:
        return false
    
    return true
```

---

## 策略注入

### Builder 模式

```java
RaftNode node = new RaftNode.Builder()
    .nodeId("node-1")
    .peerIds(Arrays.asList("node-2", "node-3"))
    .voteWeightStrategy(
        new CompositeVoteWeightStrategy(
            new EvenNodeVoteWeightStrategy(),
            new DatacenterVoteWeightStrategy("dc1")
        )
    )
    .groupStrategy(new DatacenterGroupStrategy())
    .build();
```

---

## 策略选择矩阵

| 场景 | VoteWeightStrategy | GroupStrategy |
|------|-------------------|---------------|
| 单机房奇数节点 | (无需) | DefaultGroupStrategy |
| 单机房偶数节点 | EvenNodeVoteWeightStrategy | DefaultGroupStrategy |
| 同城双活 | DatacenterVoteWeightStrategy | DatacenterGroupStrategy |
| 跨机房级联 | CompositeVoteWeightStrategy | DatacenterGroupStrategy + GlobalGroupStrategy |

---

## 性能影响

| 策略 | 计算开销 | 调用频率 |
|------|----------|----------|
| VoteWeightStrategy | O(1) | 每次选举 |
| GroupStrategy | O(1) | 配置读取 |
| HealthCheckStrategy | O(n) | 定时检测 |

---

## 设计权衡

| 决策点 | 选择方案 | 优点 | 缺点 |
|--------|----------|------|------|
| 策略数量 | 5 个接口 | 职责单一 | 类数量多 |
| 默认实现 | NoOp/Default | 开箱即用 | 需显式配置 |
| 组合策略 | Composite | 灵活组合 | 调试复杂 |

---

## 参考

- 策略模式：GoF 设计模式
- 相关源码：`src/main/java/cn/itcraft/speedboat/strategy/`
