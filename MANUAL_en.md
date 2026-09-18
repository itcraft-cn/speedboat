# Speedboat User Manual

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Configuration Reference](#configuration-reference)
- [API Reference](#api-reference)
- [Deployment Guide](#deployment-guide)
- [Advanced Usage](#advanced-usage)
- [Troubleshooting](#troubleshooting)

---

## Overview

Speedboat is a minimalist Raft consensus algorithm implementation focused on master election.

### Key Features

- **Minimalist API**: `Speedboat.start(config)` — one line to start
- **Cluster-Oriented Configuration**: Just configure `datacenter + nodes`
- **Auto-Discovery**: Automatically detects local IP and matches the node
- **Auto Mode**: Flat mode for single datacenter, cascading mode for cross-datacenter
- **Zero-Dependency Config**: Default PropertiesConfigProvider, optional JSON/YAML

### Use Cases

- Distributed system master election
- Cross-datacenter high-availability deployment
- Microservice primary/standby failover
- Per-subject exclusive publishing (named locks: quoting / opening ceremonies / scheduled jobs — exactly one active holder per subject)

---

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

**config.properties** (single datacenter):

```properties
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000
```

### 3. Start the Cluster

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

## Configuration Reference

### Properties Format

#### Single Datacenter

```properties
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000

election.intra.timeout.min=1000
election.intra.timeout.max=2000
```

#### Cross-Datacenter

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

### Configuration Items

| Key | Required | Default | Description |
|-------|------|-------|------|
| `nodes.{dc}.{node}` | ✅ | - | Node address, format `ip:port` |
| `datacenter` | ❌ | `dc-{index}` | Datacenter ID |
| `election.intra.timeout.min` | ❌ | 1000 | Intra-DC election timeout min (ms) |
| `election.intra.timeout.max` | ❌ | 2000 | Intra-DC election timeout max (ms) |
| `election.cross.timeout.min` | ❌ | 3000 | Cross-DC election timeout min (ms) |
| `election.cross.timeout.max` | ❌ | 5000 | Cross-DC election timeout max (ms) |
| `vote.weight.strategy` | ❌ | none | Weighted election: `prefer` / `even` / `none` |
| `vote.weight.prefer` | ⚠️ for prefer | - | Node ID (format `node-<ip>-<port>`) that gets the extra weight |
| `vote.weight.prefer.weight` | ❌ | 3 | Extra weight given to the preferred node |
| `raft.persistence` | ❌ | mem | Persistence tier: `mmap` / `mem` (default) / `none` |
| `raft.persistence.dir` | ❌ | `speedboat-data` | mmap tier persistence dir (auto sub-dir per `{nodeId}`) |
| `raft.mmap.size.mb` | ❌ | 128 | mmap tier WAL region size cap (MB) |
| `raft.mmap.file.name` | ❌ | `raft.mmap` | mmap file name (supports `{nodeId}` placeholder) |
| `raft.max.log.size` | ❌ | 4096 | In-memory log cap (defect-20260918-01: the sole rebuild basis for restarted replicas; must not truncate too deep) |

### ⚠️ Vote Weight: cluster-wide consistency constraint

The vote-weight table is a **cluster-wide topology fact**, not a per-node
view. "Give node X weight 3" means **every node's config must express the
same fact** — the same `vote.weight.prefer`, the same
`vote.weight.prefer.weight`, or none at all.

**Why**: each node computes candidate weights and the required quorum
independently and then exchanges votes. If node A believes the weight table
is `(4,1,1,1)` while node B believes `(1,1,1,1)`, the two sides compute
different `total/required` numbers and can declare **different winners in the
same election term** — resulting in duelling leaders.

**Real-cluster verification** (4 nodes, weights `(3,1,1,1)` on all four
machines → total 6, required 4):

- Three ordinary nodes alone = 3 < 4 → can never form a quorum
- Weighted node (4 after base 1 + extra 3) + any one ordinary = 5 ≥ 4 ✓
- Consequence: **every leader in this topology is elected only via a quorum
  that includes the weighted node**, which is exactly the intended routing
  behavior of weighted election.

Verified on a real 4-host intranet cluster (kill-leader failover ×3, extreme
2-node quorum, and full recovery); see the raft `required weight` in the
logs for end-to-end evidence.


### Node Index Convention

```
nodes.{dc-index}.{node-index}=ip:port
```

- **dc-index**: Starting from 0, incrementing
- **node-index**: Starting from 0, incrementing
- **Examples**:
  - `nodes.0.0` → DC 0, Node 0
  - `nodes.0.1` → DC 0, Node 1
  - `nodes.1.0` → DC 1, Node 0

### Auto Mode Detection

| nodes first-level size | Mode | Description |
|----------------|------|------|
| `== 1` | Single DC Flat | All nodes participate equally in election |
| `> 1` | Cross-DC Cascading | Intra-DC election + cross-DC cascading |

---

## API Reference

### Static Methods

#### Start Cluster

```java
Speedboat.start(SpeedboatConfigProvider config)
```

Starts the singleton instance. If already running, the call is ignored.

**Parameters**:
- `config` - Configuration provider

**Example**:
```java
SpeedboatConfigProvider config = new PropertiesConfigProvider("config.properties");
Speedboat.start(config);
```

#### Stop Cluster

```java
Speedboat.stop()
```

Stops the singleton instance and releases resources.

#### Check if Leader

```java
boolean isMain = Speedboat.isMain()
```

Returns whether the current node is the cluster leader.

**Returns**:
- `true` - Current node is the leader
- `false` - Current node is a follower

#### Get Leader ID

```java
String leaderId = Speedboat.getLeaderId()
```

Returns the current leader node ID.

**Returns**:
- Leader ID (e.g. `server01-192.168.10.1-0012`)
- `null` if no leader has been elected

#### Get Current Term

```java
long term = Speedboat.getTerm()
```

Returns the current Raft term number.

#### Get Local Node ID

```java
String nodeId = Speedboat.getNodeId()
```

Returns the auto-generated local node ID, format: `hostname-ip-random`.

#### Get Datacenter ID

```java
String dcId = Speedboat.getDatacenterId()
```

Returns the local datacenter ID.

#### Check Running Status

```java
boolean running = Speedboat.isRunning()
```

Returns whether the singleton instance is running.

### Named Distributed Lock

> **Semantics (as of the 2026-09-18 design)**: primacy and locking are orthogonal
> abstractions — the Leader only serialises lock commands into the Raft log;
> **any member** may apply for any named lock and holds it once consensus succeeds.
> Each named lock has **at most one holder at any time** (mutual-exclusion hard
> constraint); renew / release are supported; a holder lost → lease expires → a
> contender takes over fairly (**non-preemptive**, no revocation).

#### Get Lock Handle

```java
DistributedLock lock = Speedboat.getLock("forex")
```

- The lock name is the "subject" (forex / metals / commodity …). Multiple locks
  coexist in one cluster with independent holders.
- Leader-local and forwarded (follower) applications converge on one path; the
  judgement happens only in the state-machine apply (log total order).

#### Acquire / Release / Fencing Token

```java
LockHandle handle = lock.tryLock(5000);
if (handle.isSuccess()) {
    long epoch = handle.getEpoch();   // fencing token: owner generation, monotonic
    // ... holding window (auto renewal at leaseMs/2; a rejected RENEW stops renewal)
    handle.close();                   // release (effective cluster-wide on return)
}
```

**Key semantics**:

| Semantics | Guarantee |
|-----------|-----------|
| Mutual exclusion | ≤1 holder per named lock at any instant; a rejected acquire is observable immediately (no blind timeout) |
| Epoch fencing | Downstream envelope shall carry `lockName + epoch` and reject a stale token by strict monotonic increase |
| Non-preemption | A held lock is never revoked; the returning A must wait for the new holder's loss/release |
| Migration latency | holder lost → ≤ leaseMs to takeover; leader crash adds ≤1 election timeout of state-change pause (lease countdown unaffected) |
| Multi-lock coexistence | One node may hold several named locks at once (subjects never block each other) |

#### Restart-replica boundary (resolved by the P4 persistence tiers)

With `raft.persistence` tiers (**default `mem`**; `mmap` as the extreme-stability
tier) a restarted process re-attaches checkpoint/WAL and rebuilds lock table and
epoch deterministically — epoch monotonicity holds cluster-wide (verified on real
network: the restarted replica's migration takeover epoch equals the long-lived
replicas').

- **mem (default)**: recoverable within the process; cross-process restart behaves like a first boot; best for long-lived processes;
- **mmap** (`raft.persistence=mmap`, default dir `./speedboat-data/{nodeId}/`, default 128MB single file): attaches back on restart; lock/epoch survive restarts;
- **none** (`raft.persistence=none`): `NopRaftStore` test baseline.

---

## Deployment Guide

### Single Datacenter Deployment

#### Requirements

- JDK 8+
- Network connectivity

#### Steps

1. **Create configuration file** `config.properties`:

```properties
nodes.0.0=192.168.10.1:3000
nodes.0.1=192.168.10.2:3000
nodes.0.2=192.168.10.3:3000
```

2. **Distribute config**: Place the same config file on each machine.

3. **Start application** on each machine:

```bash
java -jar your-app.jar
```

4. **Verify**: Check logs to confirm election is complete.

### Cross-Datacenter Deployment

#### Requirements

- JDK 8+
- Network connectivity between datacenters
- Low latency within each datacenter

#### Steps

1. **Create configuration file** `config.properties`:

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

election.intra.timeout.min=1000
election.intra.timeout.max=2000
election.cross.timeout.min=3000
election.cross.timeout.max=5000
```

2. **Hangzhou DC**: Set `datacenter=hangzhou001`, distribute to 3 machines.

3. **Beijing DC**: Set `datacenter=beijing001`, distribute to 3 machines.

4. **Start application** on all machines:

```bash
java -jar your-app.jar
```

5. **Verify**: Check logs to confirm two-level election is complete.

### Container Deployment

#### Docker Example

```dockerfile
FROM openjdk:8-jdk-alpine
COPY target/your-app.jar app.jar
COPY config.properties config.properties
ENTRYPOINT ["java", "-jar", "app.jar"]
```

#### Kubernetes Example

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

## Advanced Usage

### Custom Configuration Source

Implement the `SpeedboatConfigProvider` interface to support JSON/YAML/database config sources.

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

### Monitor Leader Changes

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

### Spring Boot Integration

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

## Troubleshooting

### Common Issues

#### 1. Election Timeout

**Symptom**: No leader for an extended period.

**Causes**:
- Network partition
- Election timeout too short
- Insufficient nodes (even number of nodes)

**Solutions**:
- Check network connectivity
- Increase `election.intra.timeout`
- Use an odd number of nodes (recommended 3/5/7)

#### 2. Node Fails to Start

**Symptom**: Error `Local IP not found in configured nodes`.

**Cause**: Local IP is not in the configured node list.

**Solution**:
- Check local IP: `hostname -I` or `ifconfig`
- Ensure the config file includes the local IP

#### 3. Cross-Datacenter Election Failure

**Symptom**: Intra-DC election succeeds, but cross-DC election fails.

**Causes**:
- Network unreachable between datacenters
- `election.cross.timeout` too short

**Solutions**:
- Check network latency between datacenters
- Increase `election.cross.timeout` (recommended 3000-5000ms)

### Log Level

Adjust log level for detailed information:

```xml
<logger name="cn.itcraft.speedboat" level="DEBUG"/>
```

### Health Check Endpoint

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

## Appendix

### nodeId Format

```
{hostname}-{ip}-{random4digits}
```

Example: `server01-192.168.10.1-0012`

### Recommended Timeout Values

| Scenario | intra timeout | cross timeout |
|-----|-----------|-----------|
| Local testing | 500-1000ms | 1000-2000ms |
| Single DC production | 1000-2000ms | - |
| Cross-DC production | 1000-2000ms | 3000-5000ms |

### Recommended Node Count

- **Dev/Test**: 1 node (no fault tolerance)
- **Production**: 3/5/7 nodes (odd number)
- **Cross-DC**: 3 nodes per DC, at least 2 DCs
