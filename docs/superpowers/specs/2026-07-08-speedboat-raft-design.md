# Speedboat Raft 设计文档

## 概述

Speedboat 是一个基于 Raft 共识算法实现的分布式锁控系统，专注于主从控制场景。核心诉求是在内网万兆网卡环境下，实现亚秒级甚至亚毫秒级的主节点切换，支持动态节点规模和跨机房容错。

## 需求分析

### 核心诉求

1. **分布式锁控**：实现主从控制，确保同一时刻只有一个主节点
2. **高性能切换**：主节点故障后快速切换，目标亚秒级（150-300ms）甚至亚毫秒级
3. **动态节点规模**：支持运行时配置节点数量，包括偶数节点
4. **跨机房容错**：支持同城机房和异地机房部署，具备机房感知能力

### 性能指标

| 场景 | 目标切换时间 | 网络环境 |
|------|-------------|-----------|
| 同机房 | 150-300ms | 万兆网卡，延迟 < 1ms |
| 同城机房 | 200-500ms | 光纤直连，延迟 1-5ms |
| 异地机房 | 500-1000ms | 跨地域，延迟 50-200ms |

### 部署场景

1. **单机房**：所有节点在同一机房
2. **同城双机房**：节点分布在不同机房，光纤直连
3. **异地多机房**：节点分布在不同地域，需要容灾能力

## 架构设计

### 分层架构

```
┌──────────────────────────────────────────────────────┐
│                  Speedboat (Facade API)               │
│  - init(nodes) / isMain() / destroy()                │
│  - 对外暴露的统一接口                                  │
└──────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────┐
│              RaftNode (State Machine Core)            │
│  - NodeState: Follower/Candidate/Leader              │
│  - Term 管理                                         │
│  - 投票权重计算（策略接口注入）                         │
│  - 超时管理（随机 Election Timeout）                  │
└──────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────┐
│              RaftGroup (Cluster Management)           │
│  - 节点组管理                                        │
│  - 级联支持（parentGroup / childGroup）              │
│  - 选主协调                                          │
└──────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────┐
│         TransportLayer (抽象网络层接口)                │
│  - sendRequestVote(target, request)                  │
│  - sendHeartbeat(target, heartbeat)                  │
│  - broadcastRequestVote(request)                     │
│  - broadcastHeartbeat(heartbeat)                     │
└──────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────┐
│      NettyTransport (Netty实现，可替换为Rust JNI)      │
│  - NIO EventLoop                                    │
│  - 连接池管理                                        │
└──────────────────────────────────────────────────────┘
```

### 包结构

```
cn.itcraft.speedboat
├── Speedboat.java                     // 门面类，对外API
├── config/
│   ├── SpeedboatConfig.java           // 配置类
│   └── SpeedboatConsts.java           // 常量类
├── raft/
│   ├── RaftNode.java                  // Raft节点核心类（完整不拆分）
│   ├── RaftGroup.java                 // Raft组管理（支持级联）
│   ├── NodeState.java                 // 状态枚举（含状态转换校验）
│   ├── Term.java                      // Term管理类
│   ├── VoteContext.java               // 投票上下文
│   └── ElectionTimeout.java           // 超时管理
├── strategy/
│   ├── voteweight/
│   │   ├── VoteWeightStrategy.java        // 权重策略接口
│   │   ├── DefaultVoteWeightStrategy.java // 默认实现（无特殊权重）
│   │   ├── EvenNodeVoteWeightStrategy.java // 偶数节点策略
│   │   ├── DatacenterVoteWeightStrategy.java // 机房感知策略
│   │   └── CompositeVoteWeightStrategy.java // 组合策略
│   └── group/
│       ├── GroupStrategy.java             // 组策略接口
│       ├── DefaultGroupStrategy.java      // 默认实现（单层Raft）
│       ├── DatacenterGroupStrategy.java   // 机房内层策略
│       └── GlobalGroupStrategy.java       // 跨机房层策略
├── rpc/
│   ├── RequestVoteRequest.java        // 请求投票RPC
│   ├── RequestVoteResponse.java       // 投票响应RPC
│   ├── HeartbeatRequest.java          // 心跳请求RPC
│   └── HeartbeatResponse.java         // 心跳响应RPC
├── transport/
│   ├── TransportLayer.java            // 网络层抽象接口
│   └── NettyTransport.java            // Netty实现
└── serialize/
    ├── Serializer.java                // 序列化抽象接口
    └── CustomSerializer.java          // 自定义序列化实现
```

## 核心组件设计

### 1. Speedboat 门面类

**职责**：
- 对外暴露统一API
- 初始化 Raft 节点
- 管理生命周期
- 提供主节点状态查询

**核心接口**：

```java
public class Speedboat {
    
    // 初始化节点，传入集群节点列表
    public void init(String nodes);
    public void init(String[] nodes);
    
    // 查询当前是否为主节点
    public boolean isMain();
    
    // 销毁节点，释放资源
    public void destroy();
    
    // 获取当前节点信息
    public NodeInfo getCurrentNode();
    
    // 获取当前Term
    public long getCurrentTerm();
}
```

### 2. RaftNode 核心类

**职责**：
- 状态转换管理
- Term 管理
- 超时管理
- 投票逻辑
- 心跳发送/接收

**设计原则**：
- 保持完整，不拆分为多个类
- 扩展逻辑通过策略接口注入
- 核心逻辑与特化逻辑分离

**核心字段**：

```java
public class RaftNode {
    
    // 基础属性
    private String nodeId;
    private NodeState currentState;
    private long currentTerm;
    private String votedFor;
    
    // 超时管理
    private long lastHeartbeatTime;
    private ElectionTimeout electionTimeout;
    
    // 扩展接口（组合）
    private VoteWeightStrategy voteWeightStrategy;
    
    // 网络层
    private TransportLayer transportLayer;
}
```

### 3. NodeState 状态枚举

**设计要点**：
- 状态转换固化，便于校验
- 非法转换自动恢复为 CANDIDATE

```java
public enum NodeState {
    LEADER(new NodeState[]{FOLLOWER}),
    FOLLOWER(new NodeState[]{CANDIDATE, FOLLOWER}),
    CANDIDATE(new NodeState[]{LEADER, FOLLOWER, CANDIDATE});
    
    private final NodeState[] validNextStates;
    
    NodeState(NodeState[] validNextStates) {
        this.validNextStates = validNextStates;
    }
    
    public boolean canTransitionTo(NodeState next) {
        return Arrays.asList(validNextStates).contains(next);
    }
}
```

**状态转换规则**：

| 当前状态 | 目标状态 | 触发条件 |
|---------|---------|---------|
| LEADER | FOLLOWER | 收到更高 Term 心跳 |
| FOLLOWER | CANDIDATE | 超时未收到心跳 |
| FOLLOWER | FOLLOWER | 收到心跳或投票请求 |
| CANDIDATE | LEADER | 获得多数票 |
| CANDIDATE | FOLLOWER | 收到更高 Term 心跳 |
| CANDIDATE | CANDIDATE | 选举超时，重新发起 |

**错误处理**：
- 非法转换 → 强制转为 CANDIDATE（触发重新选举）

### 4. VoteWeightStrategy 权重策略接口

**设计思路**：
- 通过组合而非继承实现扩展
- 默认实现返回 0，无特殊权重
- 特化逻辑通过策略注入

```java
public interface VoteWeightStrategy {
    
    // 计算额外投票权重
    // 返回值：正数=提权，负数=降权，0=无特殊
    int calculateAdditionalWeight(VoteContext context);
}
```

**偶数节点策略**：

```java
public class EvenNodeVoteWeightStrategy implements VoteWeightStrategy {
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        int additional = 0;
        
        // 先启动者提权
        if (context.isStartupFirst()) {
            additional += 1;
        }
        
        // 第一个发起选举提权
        if (context.isElectionInitiator()) {
            additional += 1;
        }
        
        return additional;
    }
}
```

**机房感知策略**：

```java
public class DatacenterVoteWeightStrategy implements VoteWeightStrategy {
    
    private String currentDatacenter;
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        String candidateDatacenter = context.getCandidateDatacenter();
        
        if (candidateDatacenter.equals(currentDatacenter)) {
            return 1;  // 同城提权
        } else {
            return -1; // 异地降权
        }
    }
}
```

**组合策略**：

```java
public class CompositeVoteWeightStrategy implements VoteWeightStrategy {
    
    private List<VoteWeightStrategy> strategies;
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        int total = 0;
        for (VoteWeightStrategy strategy : strategies) {
            total += strategy.calculateAdditionalWeight(context);
        }
        return total;
    }
}
```

### 5. GroupStrategy 组策略接口

**设计思路**：
- 支持单层 Raft 和双层级联
- 通过策略配置超时和心跳参数

```java
public interface GroupStrategy {
    
    // 是否允许参与选举
    boolean canParticipateInElection(RaftNode node);
    
    // 选举超时时间
    long getElectionTimeout();
    
    // 心跳间隔
    long getHeartbeatInterval();
    
    // 是否允许跨组通信
    boolean allowCrossGroupCommunication();
}
```

**机房内层策略**：

```java
public class DatacenterGroupStrategy implements GroupStrategy {
    
    @Override
    public long getElectionTimeout() {
        return 150 + ThreadLocalRandom.current().nextLong(0, 150);
    }
    
    @Override
    public long getHeartbeatInterval() {
        return 50;
    }
    
    @Override
    public boolean allowCrossGroupCommunication() {
        return true;
    }
}
```

**跨机房层策略**：

```java
public class GlobalGroupStrategy implements GroupStrategy {
    
    @Override
    public long getElectionTimeout() {
        return 500 + ThreadLocalRandom.current().nextLong(0, 500);
    }
    
    @Override
    public long getHeartbeatInterval() {
        return 200;
    }
    
    @Override
    public boolean allowCrossGroupCommunication() {
        return false;
    }
}
```

### 6. RaftGroup 级联支持

**核心设计**：
- 统一的 RaftGroup 类，支持单层和级联
- 通过 parentGroup 和 childGroup 建立级联关系

```java
public class RaftGroup {
    
    private List<RaftNode> nodes;
    private RaftNode leader;
    
    // 级联支持
    private RaftGroup parentGroup;
    private List<RaftGroup> childGroups;
    
    // 策略接口
    private GroupStrategy groupStrategy;
    
    // 初始化
    public RaftGroup(List<String> nodeUrls, SpeedboatConfig config) {
        this.nodes = createNodes(nodeUrls, config);
        this.groupStrategy = config.getGroupStrategy();
    }
    
    // 选主
    public void electLeader();
    
    // 获取当前主节点
    public RaftNode getLeader();
    
    // 是否为主
    public boolean isMain();
}
```

**级联组装示例**：

```java
// 机房内层
RaftGroup beijingGroup = new RaftGroup(
    Arrays.asList("nodeA:22222", "nodeB:22223", "nodeC:22224"),
    config.withGroupStrategy(new DatacenterGroupStrategy())
);

// 跨机房层
RaftGroup globalGroup = new RaftGroup(
    Arrays.asList("beijingRep:33333", "shanghaiRep:33334"),
    config.withGroupStrategy(new GlobalGroupStrategy())
);

// 建立级联
beijingGroup.setParentGroup(globalGroup);
globalGroup.addChildGroup(beijingGroup);
```

## RPC 通信协议

### 1. RequestVote RPC（请求投票）

**请求结构**：

```java
public class RequestVoteRequest {
    private long term;              // 候选人的任期号
    private String candidateId;     // 候选人ID
    private int voteWeight;         // 投票权重
}
```

**响应结构**：

```java
public class RequestVoteResponse {
    private long term;              // 响应节点的当前任期
    private boolean voteGranted;    // 是否同意投票
}
```

### 2. Heartbeat RPC（心跳）

**请求结构**：

```java
public class HeartbeatRequest {
    private long term;              // Leader的任期号
    private String leaderId;        // Leader节点ID
}
```

**响应结构**：

```java
public class HeartbeatResponse {
    private long term;              // 响应节点的当前任期
    private boolean success;        // 是否成功接收心跳
}
```

## 序列化协议

### 自定义序列化格式

**协议格式**：

```
┌────────────┬────────────┬────────────┬─────────────────┐
│ 4 bit len  │ 4 bit crc32│ 1 bit type │ n bit data      │
│ (消息长度)  │ (校验码)    │ (序列化类型)│ (序列化数据)     │
└────────────┴────────────┴────────────┴─────────────────┘
```

**序列化类型枚举**：

| Type | 描述 |
|------|------|
| 0 | Java Original（JDK原生序列化） |
| 1 | Protobuf |
| 2 | Protostuff |
| 3 | Kryo |
| 其他 | 预留扩展 |

**Serializer 接口**：

```java
public interface Serializer {
    
    // 序列化
    byte[] serialize(Object obj);
    
    // 反序列化
    <T> T deserialize(byte[] data, Class<T> clazz);
}
```

**CustomSerializer 实现**：

```java
public class CustomSerializer implements Serializer {
    
    @Override
    public byte[] serialize(Object obj) {
        // 1. 选择序列化方式（默认 Protostuff）
        byte type = 2;
        byte[] data = protostuffSerialize(obj);
        
        // 2. 计算 CRC32
        int crc32 = CRC32.calculate(data);
        
        // 3. 构造消息：len(4) + crc32(4) + type(1) + data(n)
        int len = 4 + 4 + 1 + data.length;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.putInt(len);
        buffer.putInt(crc32);
        buffer.put(type);
        buffer.put(data);
        
        return buffer.array();
    }
    
    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        
        // 1. 解析 header
        int len = buffer.getInt();
        int crc32 = buffer.getInt();
        byte type = buffer.get();
        
        // 2. 提取 data
        byte[] data = new byte[len - 9];
        buffer.get(data);
        
        // 3. 校验 CRC32
        int calculated = CRC32.calculate(data);
        if (calculated != crc32) {
            throw new IllegalStateException("CRC32 check failed");
        }
        
        // 4. 根据类型反序列化
        return deserializeByType(type, data, clazz);
    }
}
```

## 数据一致性保证

### Term 单调性

**规则**：Term 必须单调递增，不能降低

**校验逻辑**：

```java
// 收到 RPC 请求时
if (request.getTerm() < currentTerm) {
    // 拒绝更低 Term 的请求
    return Response(currentTerm, false);
}

if (request.getTerm() > currentTerm) {
    // 更新 Term 并转为 Follower
    currentTerm = request.getTerm();
    transitionTo(NodeState.FOLLOWER);
}
```

**异常恢复**：

```java
// 内部校验
if (currentTerm < lastKnownTerm) {
    // Term 降低，数据异常 → 重置为 CANDIDATE
    transitionTo(NodeState.CANDIDATE);
}
```

### 状态机自我修复

**触发条件**：
1. Term 降低（数据异常）
2. Leader 身份失效
3. 非法状态转换

**恢复机制**：
- 统一重置为 CANDIDATE
- 发起新一轮选举
- 系统具备自我修复能力

## 跨机房级联架构

### 双层级联设计

```
┌──────────────────────────────────────────────────────┐
│              跨机房层 Raft（全局选主）                  │
│  Beijing-Rep ← → Shanghai-Rep ← → Shenzhen-Rep      │
│  - 超时：500-1000ms                                  │
│  - 心跳：200ms                                       │
│  - 节点：各机房代表节点                                │
└──────────────────────────────────────────────────────┘
                         ↓ 代表节点
┌──────────────────────────────────────────────────────┐
│              机房内层 Raft（本地选主）                  │
│  Beijing: NodeA(Rep) ← → NodeB, NodeC               │
│  Shanghai: NodeD(Rep) ← → NodeE, NodeF              │
│  Shenzhen: NodeG(Rep) ← → NodeH, NodeI              │
│  - 超时：150-300ms                                   │
│  - 心跳：50ms                                        │
│  - 选出机房代表节点                                   │
└──────────────────────────────────────────────────────┘
```

### 同城提权 / 异地降权

**权重计算公式**：

```
投票权重 = 基础权重(1) + 机房权重 + 启动优先级

机房权重：
- 同机房：+1（提权）
- 异地机房：-1（降权）
- 本机房故障时：0（异地节点恢复权重）

启动优先级：
- 先启动：+1
- 第一个发起选举：+1
```

**权重示例**：

```
跨机房层选举（北京机房优先）：
- Beijing-Rep: 权重=2（基础1 + 同城提权1）
- Shanghai-Rep: 权重=0（基础1 + 异地降权-1）
- Shenzhen-Rep: 权重=0（基础1 + 异地降权-1）

北京机房宕机后：
- Shanghai-Rep: 权重=1（本机房故障，异地降权解除）
- Shenzhen-Rep: 权重=1（本机房故障，异地降权解除）
```

## 场景推演

### 场景一：正常选主（3节点单机房）

```
T=0ms:    NodeA、NodeB、NodeC 启动，FOLLOWER，Term=0
          NodeA 先启动，权重=2

T=150ms:  NodeA 超时，CANDIDATE，Term=1
          发起投票 RequestVote(term=1, voteWeight=2)

T=152ms:  NodeB 收到投票请求
          term=1 >= currentTerm=0 ✓
          投票同意

T=155ms:  NodeA 收到 NodeB、NodeC 的投票响应
          票数统计：自己(权重2) + NodeB(权重1) + NodeC(权重1) = 4
          总权重：2+1+1=4，获得100%投票（4/4）> 50%阈值
          转为 LEADER，Term=1

T=160ms:  NodeA 开始发送心跳
          NodeB、NodeC 重置超时计时器

结果：NodeA 成为 Leader，切换时间 ~160ms ✅
```

### 场景二：Leader 故障，同机房切换

```
T=0ms:    NodeA(Leader) 故障，停止发送心跳

T=200ms:  NodeB 超时，CANDIDATE，Term=2
          权重=2（第一个发起选举）

T=202ms:  NodeC 收到投票请求
          term=2 > currentTerm=1 ✓
          投票同意

T=205ms:  NodeB 收到 NodeC 的投票响应
          票数：自己(权重2) + NodeC(权重1) = 3
          总权重：2+1=3，获得100%投票（3/3）> 50%阈值
          转为 LEADER，Term=2

T=210ms:  NodeB 开始发送心跳

结果：NodeB 成为新 Leader，切换时间 ~210ms ✅
```

### 场景三：偶数节点平票解决（2节点）

```
T=0ms:    NodeA、NodeB 启动，FOLLOWER，Term=0
          NodeA 先启动，权重=2（先启动者优先权）

T=150ms:  NodeA 超时，CANDIDATE，Term=1
          权重=2，发起投票

T=152ms:  NodeB 收到投票请求
          投票同意

T=155ms:  NodeA 收到 NodeB 的投票响应
          票数：自己(权重2) + NodeB(权重1) = 3
          总权重：2+1=3，获得100%投票（3/3）> 50%阈值
          转为 LEADER

结果：偶数节点通过权重偏置避免平票 ✅
```

### 场景四：跨机房选主（双层级联）

```
阶段1：机房内选主（并行）
T=0-200ms:  北京机房 NodeA 成为北京代表，Term=1
T=0-200ms:  上海机房 NodeD 成为上海代表，Term=1

阶段2：跨机房选主
T=200ms:   北京代表 NodeA 发起跨机房投票
           权重=2（同城提权）
           
T=250ms:   上海代表 NodeD 收到投票请求
           term=1 >= currentTerm=1 ✓
           权重=0（异地降权）
           投票同意

T=255ms:   NodeA 获得多数票（权重优势）
           NodeA 成为全局主，Term=1

结果：北京机房成为全局主，切换时间 ~255ms ✅
```

### 场景五：主机房整体宕机

```
T=0ms:    北京机房整体宕机（NodeA、NodeB、NodeC 全部故障）

T=500ms:  上海代表 NodeD 检测到跨机房超时
          北京机房无响应

T=501ms:  权重重算：异地降权解除
          NodeD 权重=1

T=700ms:  NodeD 发起跨机房投票，Term=2

T=750ms:  如果有深圳代表：投票同意

T=800ms:  NodeD 获得多数票
          NodeD 成为新全局主，Term=2

结果：异地机房接管，切换时间 ~800ms ✅
```

## 总结

### 核心设计要点

1. **组合式扩展**：RaftNode 和 RaftGroup 保持完整，通过策略接口注入特化逻辑
2. **状态转换固化**：NodeState 内置合法转换校验，非法转换自动恢复
3. **数据单调性保证**：Term 单调递增，异常时自动重置为 CANDIDATE
4. **权重偏置机制**：偶数节点和跨机房场景通过权重偏置解决平票问题
5. **双层级联架构**：机房内层 + 跨机房层，支持同机房快速切换和异地容灾

### 性能保障

| 场景 | 切换时间 | 关键机制 |
|------|---------|---------|
| 同机房 Leader 故障 | 150-300ms | 随机超时 + 权重偏置 |
| 同城双机房 | 200-500ms | 机房内快速切换 |
| 异地机房接管 | 500-1000ms | 双层级联 + 权重恢复 |

### 扩展能力

- ✅ 网络层抽象：NettyTransport 可替换为 Rust JNI 实现
- ✅ 序列化抽象：支持多种序列化方式，预留扩展字段
- ✅ 权重策略：支持组合多个策略，灵活配置
- ✅ 组策略：支持单层和级联部署，动态调整参数
