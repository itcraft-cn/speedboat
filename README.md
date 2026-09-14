# Speedboat

> Minimalist Raft-based Master Election Component

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-8+-green.svg)]()

## Features

- **Minimalist API**: `Speedboat.start(config)` — one line to start
- **Cluster-Oriented Configuration**: Just configure `datacenter + nodes`
- **Auto-Discovery**: Automatically detects local IP and matches the node
- **Auto Mode**: Flat mode for single datacenter, cascading mode for cross-datacenter
- **Dual Timeouts**: Independent timeout settings for intra-datacenter and cross-datacenter
- **Zero-Dependency Config**: Default Properties support, optional JSON/YAML via custom provider

## Quick Start

### 1. Add Dependency

```xml
<dependency>
    <groupId>cn.itcraft</groupId>
    <artifactId>speedboat</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 2. Create Configuration File

**config.properties**:

```properties
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000
```

### 3. Start the Cluster

```java
SpeedboatConfigProvider config = new PropertiesConfigProvider("config.properties");
Speedboat.start(config);

if (Speedboat.isMain()) {
    System.out.println("I am the leader!");
}

// On shutdown
Speedboat.stop();
```

## Configuration Examples

### Single Datacenter

```properties
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000
```

### Cross-Datacenter

```properties
datacenter=hangzhou001

# Hangzhou datacenter
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000

# Beijing datacenter
nodes.1.0=10.3.1.1:3000
nodes.1.1=10.3.1.2:3000
nodes.1.2=10.3.1.3:3000

# Intra-datacenter election timeout
election.intra.timeout.min=1000
election.intra.timeout.max=2000

# Cross-datacenter election timeout
election.cross.timeout.min=3000
election.cross.timeout.max=5000
```

### Vote Weight (optional)

```properties
# Weighted election: give node "node-192.168.10.1:3000" extra weight
vote.weight.strategy=prefer
vote.weight.prefer=node-192.168.10.1:3000
vote.weight.prefer.weight=3
```

⚠️ **The weight table is a cluster-wide topology fact.** Every node's config must
contain the *same* `vote.weight.*` values. A mismatched table means each node
computes a different required-quorum for the same election → split view of who
won → duelling leaders. Verified on real 4-node cluster: weights (3,1,1,1) →
total 6, required 4 — three ordinary nodes alone can never win; the weighted
node must participate in every quorum.

## API

```java
// Start
Speedboat.start(config);

// Status queries
Speedboat.isMain();           // Is this node the leader?
Speedboat.getLeaderId();      // Current leader node ID
Speedboat.getTerm();          // Current term
Speedboat.getNodeId();        // Local node ID
Speedboat.getDatacenterId();  // Datacenter ID
Speedboat.isRunning();        // Running status

// Stop
Speedboat.stop();
```

## Auto Mode

| nodes.size() | Mode | Description |
|-------------|------|-------------|
| `== 1` | Single DC Flat | All nodes participate equally in election |
| `> 1` | Cross-DC Cascading | Intra-DC election + cross-DC cascading |

## Documentation

- [User Manual](MANUAL.md) — Detailed configuration and API reference (English)
- [使用手册](MANUAL_cn.md) — 详细配置和 API 说明（中文）
- [Changelog](CHANGELOG.md) — Version history
- [中文文档](README_cn.md) — 中文版 README

## Build

```bash
mvn clean package
```

## Test

```bash
mvn test
```

## License

[Apache 2.0](LICENSE)
