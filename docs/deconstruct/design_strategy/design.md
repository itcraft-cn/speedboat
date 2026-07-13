# Strategy 模块设计文档

## 模块概述

`cn.itcraft.speedboat.strategy` 是 Speedboat 的策略层，通过策略模式将选举超时、心跳间隔、投票权重、健康检测、注册中心、变更验证等行为从核心逻辑中解耦。分为三个子包：`group`（分组策略）、`voteweight`（投票权重策略）、`membership`（成员管理策略）。

## 分组策略 (strategy/group)

### GroupStrategy (接口)

定义 Raft 组的选举超时和心跳策略。

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `getElectionTimeout()` | `long` | 随机选举超时（ms） |
| `getHeartbeatInterval()` | `long` | 心跳间隔（ms） |
| `allowCrossGroupCommunication()` | `boolean` | 是否允许跨组通信 |
| `getMinElectionTimeout()` | `long` | 选举超时下限（ms） |
| `getMaxElectionTimeout()` | `long` | 选举超时上限（ms） |

### DefaultGroupStrategy

单机房默认策略，适用于局域网环境。

| 参数 | 值 | 来源常量 |
|------|------|----------|
| minElectionTimeout | 150ms | `MIN_ELECTION_TIMEOUT_MS` |
| maxElectionTimeout | 300ms | `MAX_ELECTION_TIMEOUT_MS` |
| heartbeatInterval | 50ms | `HEARTBEAT_INTERVAL_MS` |
| crossGroupCommunication | false | 硬编码 |

`getElectionTimeout()` 使用 `ThreadLocalRandom` 在 [150, 300)ms 区间随机生成。

### DatacenterGroupStrategy

机房内分组策略，**当前与 DefaultGroupStrategy 完全相同**（相同参数值，相同实现逻辑）。为未来差异化优化预留。

### GlobalGroupStrategy

跨机房（全局）分组策略，适用于跨机房网络延迟较大的场景。

| 参数 | 值 | 来源常量 |
|------|------|----------|
| minElectionTimeout | 500ms | `GLOBAL_MIN_ELECTION_TIMEOUT_MS` |
| maxElectionTimeout | 1000ms | `GLOBAL_MAX_ELECTION_TIMEOUT_MS` |
| heartbeatInterval | 200ms | `GLOBAL_HEARTBEAT_INTERVAL_MS` |
| crossGroupCommunication | true | 硬编码 |

与 DefaultGroupStrategy 的关键差异：更长的超时和心跳间隔（适应跨机房延迟），`allowCrossGroupCommunication()` 返回 true。

## 投票权重策略 (strategy/voteweight)

### VoteWeightStrategy (接口)

定义投票附加权重计算策略。

单一方法：`int calculateAdditionalWeight(VoteContext context)`。

权重计算时机：在 `RaftNode.startElection()` 中计算 `totalVoteWeight = 1 + strategy.calculateAdditionalWeight(context)`。

### DefaultVoteWeightStrategy

返回 0，无附加权重。所有节点一票一权。

### EvenNodeVoteWeightStrategy

均衡加权策略，倾向于特定节点：

| 条件 | 附加权重 |
|------|----------|
| `context.isStartupFirst()` | +1 |
| `context.isElectionInitiator()` | +1 |

两者可叠加，最多 +2。

### DatacenterVoteWeightStrategy

机房亲和加权策略，构造时注入 `localDatacenter`：

| 条件 | 附加权重 |
|------|----------|
| candidateDatacenter == localDatacenter | +2 |
| candidateDatacenter != localDatacenter | -1 |
| candidateDatacenter 为空 | 0 |

### CompositeVoteWeightStrategy

组合模式，聚合多个策略的权重结果求和。

构造时接收可变参数或数组 `VoteWeightStrategy...`，`calculateAdditionalWeight()` 使用 `stream().mapToInt().sum()` 求和。

## 成员管理策略 (strategy/membership)

### HealthCheckStrategy (接口)

定义节点健康检测策略。详见 membership 模块设计文档。

### RegistryStrategy (接口)

定义注册中心策略。详见 membership 模块设计文档。

### ChangeValidationStrategy (接口)

定义成员变更验证策略。详见 membership 模块设计文档。

## 设计模式

| 模式 | 应用 |
|------|------|
| 策略模式 | GroupStrategy / VoteWeightStrategy / HealthCheckStrategy 等接口 |
| 组合模式 | CompositeVoteWeightStrategy 聚合多个策略 |
| 模板方法 | GroupStrategy 接口定义策略模板 |

## 线程安全

所有策略类均为无状态或构造后不可变对象，天然线程安全。`DatacenterVoteWeightStrategy` 的 `localDatacenter` 为 final 字段。

## 扩展点

1. 实现 `GroupStrategy` 接口自定义选举超时和心跳行为
2. 实现 `VoteWeightStrategy` 接口自定义投票权重逻辑
3. 使用 `CompositeVoteWeightStrategy` 组合多个权重策略
4. 实现 `HealthCheckStrategy`/`RegistryStrategy`/`ChangeValidationStrategy` 定制成员管理