# Speedboat 使用手册

## 目录

- [概述](#概述)
- [快速开始](#快速开始)
- [配置详解](#配置详解)
- [API 参考](#api-参考)
- [部署指南](#部署指南)
- [进阶用法](#进阶用法)
- [故障排查](#故障排查)

---

## 概述

Speedboat 是一个极简的 Raft 共识算法实现，专注于主节点选举场景。

### 核心特性

- **极简 API**：`Speedboat.start(config)` 一行启动
- **集群视角配置**：用户只需配置 `datacenter + nodes`
- **自动匹配**：系统自动检测本机 IP 并匹配节点
- **自动模式**：单机房扁平模式，跨机房级联模式
- **零依赖配置**：默认 PropertiesConfigProvider，可选实现 JSON/YAML

### 适用场景

- 分布式系统主节点选举
- 跨机房高可用部署
- 微服务主备切换

---

## 快速开始

### 1. 添加依赖

```xml
<dependency>
    <groupId>cn.itcraft</groupId>
    <artifactId>speedboat</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 2. 创建配置文件

**config.properties**（单机房）：

```properties
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000
```

### 3. 启动集群

```java
import cn.itcraft.speedboat.Speedboat;
import cn.itcraft.speedboat.config.PropertiesConfigProvider;
import cn.itcraft.speedboat.config.SpeedboatConfigProvider;

public class Application {
    public static void main(String[] args) {
        SpeedboatConfigProvider config = new PropertiesConfigProvider("config.properties");
        Speedboat.start(config);
        
        if (Speedboat.isMain()) {
            System.out.println("I am the leader!");
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Speedboat.stop();
        }));
    }
}
```

---

## 配置详解

### Properties 配置格式

#### 单机房场景

```properties
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000

election.intra.timeout.min=1000
election.intra.timeout.max=2000
```

#### 跨机房场景

```properties
datacenter=hangzhou001

nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000

nodes.1.0=10.3.1.1:3000
nodes.1.1=10.3.1.2:3000
nodes.1.2=10.3.1.3:3000

election.intra.timeout.min=1000
election.intra.timeout.max=2000
election.cross.timeout.min=3000
election.cross.timeout.max=5000
```

### 配置项说明

| 配置项 | 必需 | 默认值 | 说明 |
|-------|------|-------|------|
| `nodes.{dc}.{node}` | ✅ | - | 节点地址，格式 `ip:port` |
| `datacenter` | ❌ | `dc-{index}` | 机房 ID |
| `election.intra.timeout.min` | ❌ | 1000 | 机房内选举超时下限（ms） |
| `election.intra.timeout.max` | ❌ | 2000 | 机房内选举超时上限（ms） |
| `election.cross.timeout.min` | ❌ | 3000 | 机房间选举超时下限（ms） |
| `election.cross.timeout.max` | ❌ | 5000 | 机房间选举超时上限（ms） |

### 节点索引规则

```
nodes.{机房索引}.{节点索引}=ip:port
```

- **机房索引**：从 0 开始，递增
- **节点索引**：从 0 开始，递增
- **示例**：
  - `nodes.0.0` → 机房0，节点0
  - `nodes.0.1` → 机房0，节点1
  - `nodes.1.0` → 机房1，节点0

### 自动模式判断

| nodes 第一层大小 | 模式 | 说明 |
|----------------|------|------|
| `== 1` | 单机房扁平模式 | 所有节点平等选举 |
| `> 1` | 跨机房级联模式 | 本机房选举 + 跨机房级联 |

---

## API 参考

### 静态方法

#### 启动集群

```java
Speedboat.start(SpeedboatConfigProvider config)
```

启动单例实例。如果已运行，则忽略。

**参数**：
- `config` - 配置提供者

**示例**：
```java
SpeedboatConfigProvider config = new PropertiesConfigProvider("config.properties");
Speedboat.start(config);
```

#### 停止集群

```java
Speedboat.stop()
```

停止单例实例并释放资源。

#### 判断是否主节点

```java
boolean isMain = Speedboat.isMain()
```

返回当前节点是否是集群主节点。

**返回**：
- `true` - 当前节点是主节点
- `false` - 当前节点是从节点

#### 获取主节点 ID

```java
String leaderId = Speedboat.getLeaderId()
```

返回当前主节点的 ID。

**返回**：
- 主节点 ID（如 `server01-192.168.10.1-0012`）
- 未选举时返回 `null`

#### 获取当前任期

```java
long term = Speedboat.getTerm()
```

返回当前 Raft 任期号。

#### 获取本节点 ID

```java
String nodeId = Speedboat.getNodeId()
```

返回本节点的自动生成 ID，格式：`hostname-ip-random`。

#### 获取机房 ID

```java
String dcId = Speedboat.getDatacenterId()
```

返回本机房 ID。

#### 检查运行状态

```java
boolean running = Speedboat.isRunning()
```

返回单例实例是否正在运行。

---

## 部署指南

### 单机房部署

#### 环境要求

- JDK 8+
- 网络互通

#### 部署步骤

1. **创建配置文件** `config.properties`：

```properties
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000
```

2. **分发配置**：每台机器放置相同的配置文件。

3. **启动应用**：每台机器执行：

```bash
java -jar your-app.jar
```

4. **验证**：检查日志确认选举完成。

### 跨机房部署

#### 环境要求

- JDK 8+
- 机房间网络可达
- 机房内网络低延迟

#### 部署步骤

1. **创建配置文件** `config.properties`：

```properties
datacenter=hangzhou001

# 杭州机房
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000

# 北京机房
nodes.1.0=10.3.1.1:3000
nodes.1.1=10.3.1.2:3000
nodes.1.2=10.3.1.3:3000

election.intra.timeout.min=1000
election.intra.timeout.max=2000
election.cross.timeout.min=3000
election.cross.timeout.max=5000
```

2. **杭州机房**：配置 `datacenter=hangzhou001`，分发到 3 台机器。

3. **北京机房**：配置 `datacenter=beijing001`，分发到 3 台机器。

4. **启动应用**：所有机器执行：

```bash
java -jar your-app.jar
```

5. **验证**：检查日志确认两级选举完成。

### 容器化部署

#### Docker 示例

```dockerfile
FROM openjdk:8-jdk-alpine
COPY target/your-app.jar app.jar
COPY config.properties config.properties
ENTRYPOINT ["java", "-jar", "app.jar"]
```

#### Kubernetes 示例

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: speedboat
spec:
  serviceName: speedboat
  replicas: 3
  template:
    spec:
      containers:
      - name: speedboat
        image: your-image
        volumeMounts:
        - name: config
          mountPath: /app/config.properties
          subPath: config.properties
      volumes:
      - name: config
        configMap:
          name: speedboat-config
```

---

## 进阶用法

### 自定义配置源

实现 `SpeedboatConfigProvider` 接口支持 JSON/YAML/数据库等配置源。

```java
public class JsonConfigProvider implements SpeedboatConfigProvider {
    
    private final JsonObject config;
    
    public JsonConfigProvider(String jsonFile) {
        this.config = loadJson(jsonFile);
    }
    
    @Override
    public String getDatacenter() {
        return config.getString("datacenter");
    }
    
    @Override
    public List<List<String>> getNodes() {
        return parseNodes(config.getJsonArray("nodes"));
    }
    
    @Override
    public int getIntraDatacenterElectionTimeoutMin() {
        return config.getInt("election.intra.timeout.min", 1000);
    }
    
    @Override
    public int getIntraDatacenterElectionTimeoutMax() {
        return config.getInt("election.intra.timeout.max", 2000);
    }
    
    @Override
    public int getCrossDatacenterElectionTimeoutMin() {
        return config.getInt("election.cross.timeout.min", 3000);
    }
    
    @Override
    public int getCrossDatacenterElectionTimeoutMax() {
        return config.getInt("election.cross.timeout.max", 5000);
    }
}
```

### 监听主节点变化

```java
public class LeaderChangeListener {
    
    private String currentLeader;
    
    public void checkLeader() {
        String leader = Speedboat.getLeaderId();
        if (!Objects.equals(leader, currentLeader)) {
            onLeaderChange(currentLeader, leader);
            currentLeader = leader;
        }
    }
    
    private void onLeaderChange(String oldLeader, String newLeader) {
        System.out.println("Leader changed: " + oldLeader + " -> " + newLeader);
    }
}
```

### 集成 Spring Boot

```java
@Component
public class SpeedboatLifecycle implements ApplicationRunner, DisposableBean {
    
    @Value("${speedboat.config}")
    private String configFile;
    
    @Override
    public void run(ApplicationArguments args) {
        SpeedboatConfigProvider config = new PropertiesConfigProvider(configFile);
        Speedboat.start(config);
    }
    
    @Override
    public void destroy() {
        Speedboat.stop();
    }
}
```

---

## 故障排查

### 常见问题

#### 1. 选举超时

**现象**：长时间无主节点。

**原因**：
- 网络分区
- 选举超时配置过短
- 节点数量不足（偶数节点）

**解决**：
- 检查网络连通性
- 调大 `election.intra.timeout`
- 使用奇数节点（推荐 3/5/7）

#### 2. 节点无法启动

**现象**：启动报错 `Local IP not found in configured nodes`。

**原因**：本机 IP 不在配置的节点列表中。

**解决**：
- 检查本机 IP：`hostname -I` 或 `ifconfig`
- 确保配置文件中包含本机 IP

#### 3. 跨机房选举失败

**现象**：机房内选举成功，但跨机房选举失败。

**原因**：
- 机房间网络不可达
- `election.cross.timeout` 配置过短

**解决**：
- 检查机房网络延迟
- 调大 `election.cross.timeout`（建议 3000-5000ms）

### 日志级别

调整日志级别查看详细信息：

```xml
<logger name="cn.itcraft.speedboat" level="DEBUG"/>
```

### 健康检查接口

```java
@RestController
public class HealthController {
    
    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> health = new HashMap<>();
        health.put("running", Speedboat.isRunning());
        health.put("isMain", Speedboat.isMain());
        health.put("leaderId", Speedboat.getLeaderId());
        health.put("term", Speedboat.getTerm());
        health.put("nodeId", Speedboat.getNodeId());
        health.put("datacenter", Speedboat.getDatacenterId());
        return health;
    }
}
```

---

## 附录

### nodeId 生成规则

```
{hostname}-{ip}-{random4位}
```

示例：`server01-192.168.10.1-0012`

### 选举超时建议值

| 场景 | intra 超时 | cross 超时 |
|-----|-----------|-----------|
| 本地测试 | 500-1000ms | 1000-2000ms |
| 单机房生产 | 1000-2000ms | - |
| 跨机房生产 | 1000-2000ms | 3000-5000ms |

### 推荐节点数量

- **开发/测试**：1 节点（无容错）
- **生产环境**：3/5/7 节点（奇数）
- **跨机房**：每个机房 3 节点，至少 2 个机房
