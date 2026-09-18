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
- 多名目互斥发布（命名锁：报价/开幕/定时任务等"同名目恰好一个活跃者"）

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
| `vote.weight.strategy` | ❌ | none | 投票权重策略：`prefer` / `even` / `none` |
| `vote.weight.prefer` | ⚠️ prefer 必填 | - | 享受额外权重的节点 ID（格式 `node-<ip>-<port>`） |
| `vote.weight.prefer.weight` | ❌ | 3 | 该节点获得的额外权重 |
| `raft.persistence` | ❌ | mem | 持久化三档：`mmap` / `mem`（默认）/ `none` |
| `raft.persistence.dir` | ❌ | `speedboat-data` | mmap 档持久化目录（按 `{nodeId}` 自动分子目录） |
| `raft.mmap.size.mb` | ❌ | 128 | mmap 档 WAL 区域大小上限 |
| `raft.mmap.file.name` | ❌ | `raft.mmap` | mmap 文件名（可自定义检测 `{nodeId}` 占位） |
| `raft.max.log.size` | ❌ | 4096 | 内存日志上限（defect-20260918-01：重启副本重建判定状态的唯一依据，不可截断过深） |
| `raft.checkpoint.interval` | ❌ | 1024 | 状态机检查点触发阈值（applied 增量；仅 mmap 档生效，放大省检查点/缩小省重启恢复） |
| `lock.lease.ms` | ❌ | 30000 | 分布式锁默认租约时长（ms；越短失联迁移越快、续期开销越高） |

### ⚠️ 投票权重：全网一致性约束

权重表是**集群级拓扑事实**，不是本机视角。"给节点 X 权重 3"的含义是：
**每一台机器的配置都要表达同一个事实** —— 相同的 `vote.weight.prefer`、
相同的 `vote.weight.prefer.weight`（或全部不配置）。

**为什么**：每个节点独立计算候选者权重与 required 多数派，再交换选票。
若 A 机认为权重表是 `(4,1,1,1)` 而 B 机认为 `(1,1,1,1)`，两端会算出
**不同的 total/required**，同一 term 内可能宣布**不同的胜者** —— 双主。

**实机验证**（4 节点、全机器权重表 `(3,1,1,1)` → total 6 / required 4）：

- 三个普通节点 = 3 < 4 → 单独永远构不成多数派
- 权重节点（1+3=4）+ 任一普通 = 5 ≥ 4 ✓
- 结论：**该拓扑下任何 leader 都必须由"包含加权节点"的多数派选出**，
  这正是加权选举的路由语义。


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

### 分布式命名锁

> **语义要点（2026-09-18 设计落地后）**：主节点与锁是两个正交抽象——
> Leader 仅负责锁命令的日志复制与全序化；**任意成员**都可申请任意名目的锁，
> 经 Raft 日志共识成功即持有；同名目任意时刻**至多一个持有者**（互斥硬约束）；
> 持有者失联 → 租约到期 → 他人可公平竞争接管（**非抢占**，不抢存量持有者的锁）。

#### 获取锁对象

```java
DistributedLock lock = Speedboat.getLock("forex")
```

- 锁名即"名目"（如 forex/metals/commodity），同集群可多把锁并存、持有者互不相同
- Leader 本地申请与 follower 转发共用一套判定；判定只发生在状态机 apply（日志全序）

#### 申请 / 释放 / fencing token

```java
LockHandle handle = lock.tryLock(5000);
if (handle.isSuccess()) {
    long epoch = handle.getEpoch();   // fencing token：所有权代际，单调递增
    // ... 业务持有期（框架自动每 leaseMs/2 续期：RENEW 被拒立即停发续期）
    handle.close();                   // 释放（全网生效后返回）
}
```

**关键语义**：

| 语义 | 说明 |
|------|------|
| 互斥 | 同名目锁任意时刻至多一个持有者；被拒时判定结果立即可读（不再超时盲猜） |
| epoch fencing | 下游信凭 `lockName + epoch` 单调递增拒绝旧主（旧持有者分区自认持有的旧 token 一律拒收）|
| 非抢占 | 已被持有的锁不会被夺回；原持有者恢复后只能等新持有者失联或释放 |
| 迁移时延 | 持有者失联 → ≤ leaseMs 到期 → 接管；Leader 宕机额外 ≤ 1 选举超时时锁状态变更暂停（租约倒计时不受影响） |
| 跨锁并存 | 同一节点可同时持多把锁（名目间互不阻塞） |
| 失锁可观测 | `lock.isLost()`：RENEW 被拒或本地视图失效即为 true；**发布前必须校验**（无回调，轮询为唯一通道） |
| 公平性 | 非抢占=不保证无饥饿（先到先得续期即长持）；申请失败重试含 ±50% 抖动防惊群 |

#### 重启副本边界（2026-09-18 P4 起：三档持久化解决）

`raft.persistence` 三档持久化（**默认 mem**；mmap 为"丢了能挂回的"极稳定档）落地后，进程重启副本可挂回检查点/WAL 确定性地重建锁表与 epoch，epoch 单调性全网成立（真网已复核：重启副本接管迁移 grant 的 epoch 与长驻副本一致）。
- **mem（缺省）**：进程内可恢复，跨进程重启等同"首次启动"，仅适合长驻进程场景；
- **mmap**（`raft.persistence=mmap`，默认目录 `./speedboat-data/{nodeId}/`、默认 128MB 单文件）：重启挂回，锁表 epoch 跨重启保真；
- **none**（`raft.persistence=none`）：NopRaftStore 测试基线。

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
