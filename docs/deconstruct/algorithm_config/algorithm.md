# 配置算法

## 概述

Speedboat 配置模块提供 **多来源配置加载** 和 **配置抽象接口**，支持 Properties 文件、环境变量等多种配置方式。

**核心组件**：
- SpeedboatConfig：配置数据结构
- SpeedboatConfigProvider：配置提供者接口
- PropertiesConfigProvider：Properties 文件实现
- MembershipConfig：成员变更配置
- SpeedboatConsts：常量定义

---

## 配置层级

```
+------------------------+
| SpeedboatConfigProvider | (接口)
+------------------------+
            |
            v
+------------------------+
| PropertiesConfigProvider| (实现)
+------------------------+
            |
            v
+------------------------+
|    SpeedboatConfig     | (数据)
+------------------------+
```

---

## SpeedboatConfig 结构

### 字段定义

```
SpeedboatConfig {
    String nodeId
    List<String> peerIds
    int port
    String datacenter
    VoteWeightStrategy voteWeightStrategy
    GroupStrategy groupStrategy
    MembershipConfig membershipConfig
}
```

### Builder 模式

```
SpeedboatConfig config = new SpeedboatConfig.Builder()
    .nodeId("node-1")
    .peerIds(Arrays.asList("node-2", "node-3"))
    .port(22222)
    .datacenter("dc1")
    .build();
```

---

## SpeedboatConfigProvider 接口

### 方法定义

```java
public interface SpeedboatConfigProvider {
    String getNodeId();
    List<String> getPeerIds();
    int getPort();
    String getDatacenter();
    SpeedboatConfig getConfig();
}
```

---

## PropertiesConfigProvider 实现

### 配置文件格式

```properties
# speedboat.properties

# 节点配置
speedboat.node.id=node-1
speedboat.node.port=22222
speedboat.node.datacenter=dc1

# 集群配置
speedboat.peers=node-2:22222,node-3:22222

# 选举配置
speedboat.election.timeout.min=150
speedboat.election.timeout.max=300
speedboat.heartbeat.interval=50

# 成员变更配置
speedboat.membership.enabled=true
speedboat.membership.health.check.interval=5000
speedboat.membership.failure.threshold=3
```

---

### 加载算法

```
PropertiesConfigProvider(propertiesFile):
    1. 加载 Properties 文件:
       properties = new Properties()
       inputStream = readFile(propertiesFile)
       properties.load(inputStream)
    
    2. 解析配置:
       nodeId = properties.getProperty("speedboat.node.id")
       port = parseInt(properties.getProperty("speedboat.node.port"))
       datacenter = properties.getProperty("speedboat.node.datacenter")
       
    3. 解析 peers:
       peersStr = properties.getProperty("speedboat.peers")
       peerIds = parsePeers(peersStr)  // 提取 nodeId
    
    4. 构建 SpeedboatConfig:
       config = new SpeedboatConfig.Builder()
           .nodeId(nodeId)
           .peerIds(peerIds)
           .port(port)
           .datacenter(datacenter)
           .build()
```

---

### parsePeers 算法

```
parsePeers(peersStr):
    if peersStr == null || peersStr.isEmpty():
        return emptyList()
    
    peers = peersStr.split(",")
    peerIds = new ArrayList()
    
    for peer in peers:
        // 格式: "nodeId:port"
        parts = peer.trim().split(":")
        peerIds.add(parts[0])
    
    return peerIds
```

---

## MembershipConfig 结构

### 字段定义

```
MembershipConfig {
    boolean membershipChangeEnabled
    long healthCheckInterval
    int failureThreshold
    long confirmationNanos
}
```

### 默认值

| 参数 | 默认值 | 说明 |
|------|--------|------|
| membershipChangeEnabled | true | 是否启用成员变更 |
| healthCheckInterval | 5000 ms | 健康检测间隔 |
| failureThreshold | 3 | 故障次数阈值 |
| confirmationNanos | 10 s | 确认时间窗口 |

---

## SpeedboatConsts 常量

### 超时配置

| 常量 | 值 | 说明 |
|------|-----|------|
| MIN_ELECTION_TIMEOUT_MS | 150 | 最小选举超时 |
| MAX_ELECTION_TIMEOUT_MS | 300 | 最大选举超时 |
| HEARTBEAT_INTERVAL_MS | 50 | 心跳间隔 |
| GLOBAL_MIN_ELECTION_TIMEOUT_MS | 500 | 全局组最小选举超时 |
| GLOBAL_MAX_ELECTION_TIMEOUT_MS | 1000 | 全局组最大选举超时 |
| GLOBAL_HEARTBEAT_INTERVAL_MS | 200 | 全局组心跳间隔 |

### 序列化配置

| 常量 | 值 | 说明 |
|------|-----|------|
| SERIALIZATION_HEADER_LEN | 9 | 协议头长度（旧版） |
| CRC32_LEN | 4 | CRC32 长度 |
| TYPE_LEN | 1 | 类型长度 |

### 端口配置

| 常量 | 值 | 说明 |
|------|-----|------|
| DEFAULT_PORT | 22222 | 默认端口 |
| GLOBAL_PORT | 33333 | 全局组端口 |

### 其他配置

| 常量 | 值 | 说明 |
|------|-----|------|
| SHUTDOWN_TIMEOUT_SECONDS | 1 | 关闭超时 |
| DEFAULT_MAX_LOG_SIZE | 10 | 最大日志大小 |

---

## 配置验证

### validateConfig 算法

```
validateConfig(config):
    // 1. 节点 ID 非空
    if config.nodeId == null || config.nodeId.isEmpty():
        throw IllegalArgumentException("nodeId required")
    
    // 2. Peers 非空
    if config.peerIds == null || config.peerIds.isEmpty():
        throw IllegalArgumentException("peerIds required")
    
    // 3. 端口范围
    if config.port < 1024 || config.port > 65535:
        throw IllegalArgumentException("port out of range")
    
    // 4. 选举超时范围
    if config.minElectionTimeout >= config.maxElectionTimeout:
        throw IllegalArgumentException("invalid election timeout range")
    
    return true
```

---

## 配置优先级

### 加载顺序

```
1. 代码默认值（SpeedboatConsts）
2. 配置文件（speedboat.properties）
3. 环境变量（SPEEDBOAT_*）
4. 代码显式设置（Builder）
```

### 优先级规则

```
后加载覆盖先加载：
代码显式设置 > 环境变量 > 配置文件 > 默认值
```

---

## 环境变量支持

### 命名规则

```
SPEEDBOAT_NODE_ID=node-1
SPEEDBOAT_NODE_PORT=22222
SPEEDBOAT_DATACENTER=dc1
```

### 读取算法

```
getEnvConfig(key):
    envValue = System.getenv("SPEEDBOAT_" + key.toUpperCase())
    if envValue != null:
        return envValue
    
    return properties.getProperty("speedboat." + key.toLowerCase())
```

---

## 动态配置

### 运行时更新（未实现）

```
updateConfig(newConfig):
    // 1. 验证新配置
    validateConfig(newConfig)
    
    // 2. 应用新配置
    synchronized(this):
        this.config = newConfig
    
    // 3. 通知监听器
    notifyConfigChange(newConfig)
```

---

## 配置示例

### 单机房配置

```properties
speedboat.node.id=node-1
speedboat.node.port=22222
speedboat.peers=node-2:22222,node-3:22222

speedboat.election.timeout.min=150
speedboat.election.timeout.max=300
speedboat.heartbeat.interval=50

speedboat.membership.enabled=true
```

### 跨机房配置

```properties
speedboat.node.id=node-dc1-1
speedboat.node.port=22222
speedboat.node.datacenter=dc1
speedboat.peers=node-dc1-2:22222,node-dc2-1:33333,node-dc2-2:33333

speedboat.election.timeout.min=150
speedboat.election.timeout.max=300
speedboat.heartbeat.interval=50

speedboat.membership.enabled=true
speedboat.membership.health.check.interval=10000
```

---

## 设计权衡

| 决策点 | 选择方案 | 优点 | 缺点 |
|--------|----------|------|------|
| 配置来源 | Properties 文件 | 简单通用 | 不支持 YAML/JSON |
| 接口抽象 | Provider 接口 | 可扩展 | 增加复杂度 |
| Builder | 分步构建 | 灵活 | 代码冗长 |
| 常量类 | final class | 集中管理 | 不可覆盖 |

---

## 参考

- Java Properties API
- Builder 模式：Effective Java
- 相关源码：`src/main/java/cn/itcraft/speedboat/config/`
