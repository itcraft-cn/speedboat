# Speedboat

> 极简 Raft 主节点选举组件

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-8+-green.svg)]()

## 特性

- **极简 API**：`Speedboat.start(config)` 一行启动
- **集群视角配置**：用户只需配置 `datacenter + nodes`
- **自动匹配**：系统自动检测本机 IP 并匹配节点
- **自动模式**：单机房扁平模式，跨机房级联模式
- **两套超时**：机房内/机房间独立配置
- **零依赖配置**：默认 Properties，可选实现 JSON/YAML

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

**config.properties**：

```properties
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000
```

### 3. 启动集群

```java
SpeedboatConfigProvider config = new PropertiesConfigProvider("config.properties");
Speedboat.start(config);

if (Speedboat.isMain()) {
    System.out.println("I am the leader!");
}

// 关闭时
Speedboat.stop();
```

## 配置示例

### 单机房

```properties
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000
```

### 跨机房

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

# 机房内选举超时
election.intra.timeout.min=1000
election.intra.timeout.max=2000

# 机房间选举超时
election.cross.timeout.min=3000
election.cross.timeout.max=5000
```

### 投票权重（可选）

```properties
# 加权选举：给节点 "node-192.168.10.1:3000" 附加权重
vote.weight.strategy=prefer
vote.weight.prefer=node-192.168.10.1:3000
vote.weight.prefer.weight=3
```

⚠️ **权重表是集群级拓扑事实**：每台节点的配置必须持有
**相同** 的 `vote.weight.*`。表不同步会导致各节点计算出
不同的 required 多数派 → 同一 term 宣布不同胜者 → 双主。
实机 4 节点验证：权重 (3,1,1,1) → total 6 / required 4，
三个普通节点永不能单独取胜，加权节点必须参与每次多数派。

## API

```java
// 启动
Speedboat.start(config);

// 状态查询
Speedboat.isMain();           // 是否主节点
Speedboat.getLeaderId();      // 主节点ID
Speedboat.getTerm();          // 当前任期
Speedboat.getNodeId();        // 本节点ID
Speedboat.getDatacenterId();  // 机房ID
Speedboat.isRunning();        // 运行状态

// 停止
Speedboat.stop();
```

## 自动模式

| nodes.size() | 模式 | 说明 |
|-------------|------|------|
| `== 1` | 单机房扁平模式 | 所有节点平等选举 |
| `> 1` | 跨机房级联模式 | 本机房选举 + 跨机房级联 |

## 文档

- [使用手册](MANUAL.md) — 详细配置和 API 说明
- [User Manual](MANUAL_en.md) — Detailed configuration and API reference (English)
- [Changelog](CHANGELOG_en.md) — Version history
- [变更日志](CHANGELOG.md) — 版本更新记录
- [English](README_en.md) — English README

## 构建

```bash
mvn clean package
```

## 测试

```bash
mvn test
```

## License

[Apache 2.0](LICENSE)
