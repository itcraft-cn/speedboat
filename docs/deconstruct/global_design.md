# Speedboat 全局设计文档

## 1. 系统概述

**Speedboat** 是一个基于 Raft 协议的分布式锁控系统，专为主从控制场景设计，目标实现亚秒级/亚毫秒级的主从切换，支持偶数节点和跨机房容错。

---

## 2. 核心目标

### 2.1 性能目标

| 场景 | 切换时间目标 |
|------|-------------|
| 同机房 (万兆网卡) | 150-300ms |
| 同城 (100km) | 200-500ms |
| 异地 (1000km) | 500-1000ms |

### 2.2 功能目标

- **主从选举**：基于 Raft 协议的分布式选举
- **心跳维护**：Leader 定期向 Follower 发送心跳
- **状态管理**：节点状态转换和任期管理
- **锁控操作**：acquireLock/releaseLock
- **权重投票**：支持偶数节点和跨机房权重策略

### 2.3 质量目标

- **可用性**：99.9%，支持节点故障和网络分区
- **可扩展性**：策略可插拔，传输层可替换
- **可观测性**：JMX 暴露指标，日志记录
- **测试覆盖率**：80%+ 分支覆盖率

---

## 3. 系统架构

### 3.1 分层架构

```
┌─────────────────────────────────────────────────────────────┐
│  应用层 (Facade Layer)                                       │
│  └─ Speedboat: 门面类，提供统一API                           │
├─────────────────────────────────────────────────────────────┤
│  协议层 (Protocol Layer)                                     │
│  ├─ RaftGroup: Raft 组管理                                   │
│  ├─ RaftNode: Raft 节点状态机                                │
│  ├─ NodeState: 状态枚举 (LEADER/FOLLOWER/CANDIDATE)         │
│  ├─ Term: 任期管理                                           │
│  └─ RaftMessage: 消息系列                                    │
├─────────────────────────────────────────────────────────────┤
│  策略层 (Strategy Layer)                                     │
│  ├─ VoteWeightStrategy: 投票权重策略接口                     │
│  │  ├─ FirstStartPriorityStrategy: 先启动者优先权            │
│  │  ├─ DatacenterPriorityStrategy: 机房优先权                │
│  │  └─ CascadePriorityStrategy: 级联组合                    │
│  └─ GroupStrategy: 组策略接口                                │
│     ├─ DefaultGroupStrategy: 默认配置                        │
│     └─ CascadeGroupStrategy: 级联配置                        │
├─────────────────────────────────────────────────────────────┤
│  传输层 (Transport Layer)                                    │
│  ├─ Transport: 传输接口                                      │
│  ├─ NettyTransport: Netty 4.1 实现                           │
│  ├─ NodeEndpoint: 节点地址封装                               │
│  └─ TransportListener: 监听器接口                            │
├─────────────────────────────────────────────────────────────┤
│  序列化层 (Serialize Layer)                                  │
│  ├─ Serializer: 序列化接口                                   │
│  ├─ ProtostuffSerializer: Protostuff 实现                    │
│  ├─ CustomSerializer: 自定义协议封装                         │
│  └─ SerializationException: 异常类                           │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 组件关系图

```
┌──────────────┐
│  Speedboat   │ (Facade)
└──────┬───────┘
       │
       ├──────────────────┬──────────────────┐
       │                  │                  │
┌──────▼───────┐  ┌───────▼──────┐  ┌───────▼──────┐
│  RaftGroup   │  │  Transport   │  │ VoteWeight   │
│              │  │              │  │  Strategy    │
└──────┬───────┘  └───────┬──────┘  └──────────────┘
       │                  │
┌──────▼───────┐  ┌───────▼──────┐
│  RaftNode    │  │  Serializer  │
│              │  │              │
└──────┬───────┘  └───────┬──────┘
       │                  │
┌──────▼───────┐  ┌───────▼──────┐
│ NodeState    │  │ Protostuff   │
│ Term         │  │ Serializer   │
└──────────────┘  └──────────────┘
```

---

## 4. 核心类设计

### 4.1 Speedboat (门面类)

**职责**：统一入口，协调各模块

**关键方法**：
```java
public class Speedboat {
    private RaftGroup raftGroup;
    private Transport transport;
    private VoteWeightStrategy voteWeightStrategy;

    public void init();                    // 启动系统
    public void shutdown();                // 关闭系统
    public boolean acquireLock(String);    // 获取锁
    public void releaseLock(String);       // 释放锁
    public boolean isLeader();             // 是否为 Leader
    public Optional<NodeInfo> getLeaderInfo(); // 获取 Leader 信息
}
```

### 4.2 RaftGroup (组管理)

**职责**：管理一组 Raft 节点

**关键方法**：
```java
public class RaftGroup {
    private Map<String, RaftNode> nodes;
    private String groupId;
    private GroupStrategy groupStrategy;

    public void addNode(RaftNode);         // 添加节点
    public void removeNode(String);        // 移除节点
    public void startElection();           // 触发选举
    public void handleMessage(RaftMessage);// 处理消息
}
```

### 4.3 RaftNode (节点状态机)

**职责**：单个节点的 Raft 状态机

**关键方法**：
```java
public class RaftNode {
    private String nodeId;
    private NodeState state;
    private Term currentTerm;
    private String votedFor;

    public void start();                   // 启动节点
    public void stop();                    // 停止节点
    public void startElection();           // 发起选举
    public void sendHeartbeat();           // 发送心跳
    public void transitionTo(NodeState);   // 状态转换
}
```

### 4.4 NodeState (状态枚举)

**职责**：定义节点状态和转换规则

```java
public enum NodeState {
    LEADER,      // 主节点
    FOLLOWER,    // 从节点
    CANDIDATE;   // 候选人

    public boolean canTransitionTo(NodeState); // 转换验证
}
```

### 4.5 Term (任期管理)

**职责**：保证任期单调递增

```java
public class Term {
    private long current;

    public void increment();                   // 任期自增
    public boolean updateIfHigher(long);       // 更新任期
    public boolean isMonotonicViolation(long); // 检查违规
}
```

---

## 5. 核心流程

### 5.1 选举流程

```
Follower 心跳超时
  ↓
transitionTo(CANDIDATE)
  ↓
currentTerm.increment()
  ↓
votedFor = self.nodeId
  ↓
calculateAdditionalWeight()
  ↓
broadcast(RequestVote)
  ↓
收集投票响应
  ↓
if (votes >= majority) {
    transitionTo(LEADER)
    sendHeartbeat()
} else {
    transitionTo(FOLLOWER)
}
```

### 5.2 心跳流程

```
Leader (定时器)：
  ↓
创建 HeartbeatRequest
  ↓
broadcast(HeartbeatRequest)

Follower (接收)：
  ↓
校验 term
  ↓
resetHeartbeatTimer()
  ↓
返回 HeartbeatResponse
```

### 5.3 状态转换流程

```
transitionTo(NodeState nextState)：
  ↓
canTransitionTo(nextState) ?
  ├─ Yes → 清理当前状态 → 更新 state → 初始化新状态
  └─ No → throw IllegalStateTransitionException
```

---

## 6. 策略设计

### 6.1 投票权重策略

| 策略 | 用途 | 权重计算 |
|------|------|----------|
| FirstStartPriorityStrategy | 偶数节点平票 | 先启动者 +1 |
| DatacenterPriorityStrategy | 跨机房优先权 | 同机房 +2，同城 +1 |
| CascadePriorityStrategy | 级联组合 | 组合多个策略 |

### 6.2 组策略

| 策略 | 用途 | 配置项 |
|------|------|--------|
| DefaultGroupStrategy | 默认配置 | electionTimeout, heartbeatInterval |
| CascadeGroupStrategy | 级联架构 | innerGroupStrategy, outerGroupStrategy |

---

## 7. 传输层设计

### 7.1 协议格式

```
+--------+--------+------+----------+
| Length | CRC32  | Type | Payload  |
| 4 bytes| 4 bytes|1 byte| n bytes  |
+--------+--------+------+----------+

总长度: 9 + payload.length
```

### 7.2 Netty 配置

| 配置项 | 值 | 说明 |
|--------|-----|------|
| BossGroup | 1 thread | 接收连接 |
| WorkerGroup | N threads | 处理 IO |
| SO_BACKLOG | 128 | 连接队列 |
| TCP_NODELAY | true | 禁用 Nagle |

---

## 8. 内存设计

### 8.1 内存占用

| 组件 | 单节点内存 | 集群 (5节点) |
|------|-----------|-------------|
| RaftNode | ~1.2KB | ~6KB |
| RaftGroup | ~1.1KB | ~1.1KB |
| NettyTransport | ~5-7MB | ~25-35MB |
| 总计 | ~20MB | ~100MB |

### 8.2 GC 优化

- **堆大小**：512MB - 2GB（根据节点数）
- **新生代**：40-50%（减少 Young GC）
- **GC 算法**：G1GC（低延迟场景）

---

## 9. 性能优化

### 9.1 网络优化

- **Netty NIO**：非阻塞 IO，零拷贝
- **对象池**：复用 LinkedBuffer
- **批量发送**：合并心跳和投票

### 9.2 序列化优化

- **Protostuff**：高效序列化，5-10倍提升
- **紧凑协议**：CustomSerializer 封装
- **CRC32 校验**：保证完整性

### 9.3 选举优化

- **权重预计算**：减少选举延迟
- **随机超时**：避免同时选举
- **自适应心跳**：根据网络延迟调整

---

## 10. 容错设计

### 10.1 节点故障

```
单节点故障：
  ├─ Follower 故障 → 不影响集群
  └─ Leader 故障 → 新选举，快速切换

多节点故障：
  ├─ < majority 故障 → 继续工作
  ├─ >= majority 故障 → 等待恢复
```

### 10.2 网络故障

```
网络分区：
  ├─ Majority 分区 → 继续选举和工作
  └─ Minority 分区 → 等待恢复

网络延迟：
  ├─ 自适应超时 → 延长心跳间隔
  └─ 重试机制 → 超时重试
```

---

## 11. 部署方案

### 11.1 单机房 (3节点)

```
性能：150-300ms
容错：1节点故障可继续工作
配置：DefaultGroupStrategy
```

### 11.2 同城双机房 (6节点)

```
性能：200-500ms
容错：单机房故障可继续工作
配置：DatacenterPriorityStrategy
```

### 11.3 异地三机房 (9节点)

```
性能：500-1000ms
容错：任意单机房故障可继续工作
配置：CascadeGroupStrategy
```

---

## 12. 监控指标

### 12.1 关键指标

| 指标 | 说明 | 告警阈值 |
|------|------|----------|
| node_state | 节点状态 | - |
| current_term | 当前任期 | - |
| heartbeat_latency | 心跳延迟 | > 100ms |
| election_count | 选举次数 | > 10/min |
| message_sent | 发送消息数 | - |
| error_count | 错误数 | > 10/min |

### 12.2 监控集成

- **JMX**：暴露 MBean
- **日志**：记录关键事件
- **Prometheus**：Exporter 集成（可选）

---

## 13. 测试设计

### 13.1 单元测试

| 模块 | 测试重点 |
|------|----------|
| RaftNode | 状态转换、选举、心跳 |
| RaftGroup | 节点管理、消息路由 |
| Term | 任期单调性 |
| Transport | 发送、接收、序列化 |
| Strategy | 权重计算 |

### 13.2 集成测试

| 场景 | 测试重点 |
|------|----------|
| 单节点启动 | 初始化流程 |
| 多节点选举 | 选举唯一性 |
| 网络分区 | Majority/Minority |
| 权重投票 | 偶数节点平票 |

### 13.3 测试覆盖率

**目标**：80%+ 分支覆盖率
**当前**：Instruction 92.9%，Branch 80.7%

---

## 14. 扩展方向

### 14.1 短期扩展

- **日志复制**：Leader 日志同步到 Follower
- **快照机制**：压缩日志，减少存储
- **成员变更**：动态添加/移除节点

### 14.2 中期扩展

- **Rust JNI**：重构传输层，提升性能
- **多 Raft 组**：支持多个 Raft 组
- **监控平台**：Prometheus + Grafana

### 14.3 长期扩展

- **Multi-Raft**：多 Raft 组架构
- **跨云容灾**：多云部署
- **AI 运维**：智能故障预测

---

## 15. 技术栈

### 15.1 核心技术

| 技术 | 版本 | 用途 |
|------|------|------|
| Java | 11+ | 编程语言 |
| Netty | 4.1 | 网络框架 |
| Protostuff | 1.7 | 序列化 |
| JUnit | 5 | 单元测试 |
| Mockito | 4 | Mock 测试 |
| Maven | 3.8 | 构建 |

### 15.2 工具链

- **IDE**：IntelliJ IDEA
- **版本控制**：Git
- **CI/CD**：Jenkins (可选)
- **监控**：JMX, Prometheus (可选)

---

## 16. 约定式提交规范

```
提交格式：
  <type>(<scope>): <subject>

类型：
  ├─ feat: 新功能
  ├─ fix: 修复
  ├─ docs: 文档
  ├─ style: 格式
  ├─ refactor: 重构
  ├─ test: 测试
  └─ chore: 构建/辅助工具

示例：
  feat(raft): 实现权重投票策略
  fix(transport): 修复 Netty 连接泄漏
  test(core): 补充分支覆盖测试达到80%目标
```

---

## 17. 编码规范

### 17.1 Java 规范

- **命名**：类名 PascalCase，方法名 camelCase
- **注释**：仅必要的注释，避免冗余
- **OOP**：组合优于继承
- **接口**：核心类不拆分，通过策略接口扩展

### 17.2 禁止事项

- **禁止**：使用 Java 原生序列化
- **禁止**：在核心类中使用继承
- **禁止**：暴露内部状态（字段私有化）
- **禁止**：在权重计算中执行 IO 操作

---

## 18. 版本规划

### 18.1 v1.0 (当前)

- ✅ 主从选举
- ✅ 心跳维护
- ✅ 权重投票
- ✅ 网络传输
- ✅ 测试覆盖率 80.7%

### 18.2 v1.1 (计划)

- 日志复制
- 快照机制
- 成员变更

### 18.3 v2.0 (计划)

- Rust JNI 传输层
- 多 Raft 组
- 监控平台

---

## 19. 总结

Speedboat 是一个高性能分布式锁控系统，核心特点：

1. **高性能**：亚秒级切换，Netty + Protostuff
2. **高可用**：majority 容错，权重解决平票
3. **高扩展**：策略可插拔，传输层可替换
4. **高测试**：80.7% 分支覆盖率

架构清晰，设计合理，性能达标，测试完整，后续演进方向明确。