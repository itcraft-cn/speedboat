# Speedboat

> Minimalist Raft-based Master Election Component

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-8+-green.svg)]()

## Features

- **Minimalist API**: `Speedboat.start(config)` — one line to start
- **Cluster-Oriented Configuration**: Just configure `datacenter + nodes`
- **Auto-Discovery**: Automatically detects local IP and matches the node
- **Auto Mode**: Flat mode for single datacenter, cascading mode for cross-datacenter
- **Named Distributed Lock**: any member may acquire any named lock; exactly one holder; non-preemptive takeover with epoch fencing
- **Three Persistence Tiers**: mem (default) / mmap (re-attachable across restarts) / none — locks and epochs survive restarts
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

### Persistence tiers (optional, default `mem`)

```properties
# raft.persistence=mem | mmap | none
#   mem (default): in-process recoverable, fast, no disk overhead (stable default)
#   mmap         : 128MB single-file mmap WAL — "attach back if lost",
#                  recoverable across process restarts (extreme-stability tier)
#   none         : NopRaftStore, test baseline
raft.persistence=mmap
raft.persistence.dir=speedboat-data        # mmap dir ({nodeId} auto sub-dir)
raft.mmap.size.mb=128                      # mmap single-file size cap
raft.mmap.file.name=raft.mmap              # file name, {nodeId} placeholder allowed
raft.max.log.size=4096                     # in-memory log cap (rebuild basis)
raft.checkpoint.interval=1024              # checkpoint threshold (mmap tier)
lock.lease.ms=30000                        # default named-lock lease
```

## Named Distributed Lock

Primacy and locking are deliberately decoupled: **any member** may acquire any
named lock (not only the leader); consensus grants holdership; each named lock
has **exactly one holder at any time** (mutual-exclusion hard constraint);
renewable and releasable; a lost holder's lease expires and a contender takes
over fairly (**non-preemptive** — an existing holder is never revoked).

```java
DistributedLock lock = Speedboat.getLock("forex");   // subject name = lock name
LockHandle handle = lock.tryLock(5000);
if (handle.isSuccess()) {
    long epoch = handle.getEpoch();   // fencing token: consumers reject via lockName+epoch monotonicity
    // ... holding window (auto renewal; rejected RENEW stops immediately)
    handle.close();
}
```

| Semantics | Guarantee |
|-----------|-----------|
| Mutual exclusion | ≤1 holder per named lock at any instant; judgement happens only in state-machine apply (log total order) |
| Epoch fencing | Survives restarts (mmap tier); consumers trust lockName+epoch monotonicity |
| Non-preemption | A holder never loses the lock while alive; a returning A cannot steal it back |
| Migration | holder lost → ≤ lease expiry → contender takes over; leader crash pauses state changes ≤1 election timeout |

The quoting scenario (A holds the forex lock / B the metals lock / C the
commodities lock) fits **one single Raft group** — no extra processes or ports.

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

// Named distributed lock
Speedboat.getLock("forex");   // subject name = lock name
```

## Design Constraint: Single Cluster

> ⚠️ **One process = one Raft cluster topology**. Speedboat uses a singleton facade:
> each application instance maintains the leader/follower relationship of
> exactly **one** cluster (a single cluster may still span multiple datacenters).

```text
✅ Supported: one application participating in one cluster (single or cross-datacenter)
✅ Supported: same-tier named-lock subjects coexisting
             (getLock("forex")/getLock("metals") side by side)
❌ Not supported: one application maintaining leadership of multiple clusters
                 simultaneously (e.g. both a/b/c and a/x/y clusters)
```

Implications:

- `Speedboat.start(config)` is a singleton; a second start is ignored
- The config holds exactly one `nodes` topology; the local IP is matched only once
- All static APIs (`isMain()` / `getLock()` etc.) refer to this single cluster
- Per-subject mutual exclusion is carried by **named locks** (lock name = namespace);
  intra-lock mutual exclusion is guaranteed by the Raft log total order

**Why this design**: multi-cluster (multi-group) use cases are rare.
To keep the API and transport protocol minimal, support is deliberately omitted.
If you truly need it, deploy one process per cluster, or extend the config layer
and transport layer yourself (RPC messages would need a groupId for multiplexing).

## Auto Mode

| nodes.size() | Mode | Description |
|-------------|------|-------------|
| `== 1` | Single DC Flat | All nodes participate equally in election |
| `> 1` | Cross-DC Cascading | Intra-DC election + cross-DC cascading |

## Documentation

- [User Manual](MANUAL_en.md) — Detailed configuration and API reference (English)
- [使用手册](MANUAL.md) — 详细配置和 API 说明（中文）
- [Changelog](CHANGELOG_en.md) — Version history
- [变更日志](CHANGELOG.md) — 版本更新记录
- [中文文档](README.md) — 中文版 README

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
