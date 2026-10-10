# Speedboat User Manual

> [中文](MANUAL.md) · [README](README_en.md) · [Changelog](CHANGELOG_en.md)

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
| `cross.port.offset` | ❌ | 1000 | Parent-group port = child-group port + this offset (**must be identical across the whole cluster**, otherwise the two groups cannot reach each other) |
| `datacenter.weight.{dcId}` | ❌ | derived by index | Parent-group datacenter weight, keyed by datacenter ID (e.g. `datacenter.weight.dc-0=2`); datacenters not listed fall back to weight 1 |
| `vote.weight.strategy` | ❌ | none | Weighted election: `prefer` / `even` / `none` |
| `consistency.policy` | ❌ | cp | Consistency policy: `cp` (strongly-consistent uniqueness, never dual-leader; full datacenter loss degrades to leaderless) / `ap` (availability first; after the peer datacenter is unreachable past the threshold, degrade and take over — a brief dual-leader window is possible during partition). **Chosen at startup and constant for the whole lifecycle**; applies to the two-datacenter cascading parent group only, single-datacenter deployments are unaffected |
| `consistency.degraded.timeout.ms` | ❌ | 10000 | `ap` only: how long an unresponsive peer datacenter may persist before degraded takeover is allowed; should be significantly larger than the check-quorum freshness threshold (5s) and the cross-DC election timeout upper bound (5s) |
| `promote.term.leap` | ❌ | 100 | Term leap applied when manually promoting a datacenter's priority (must be > 0); the larger it is, the more reliably the peer's stale terms yield after healing, at the cost of faster term growth |
| `vote.weight.prefer` | ⚠️ for prefer | - | Node ID (format `node-<ip>-<port>`) that gets the extra weight |
| `vote.weight.prefer.weight` | ❌ | 3 | Extra weight given to the preferred node |
| `raft.persistence` | ❌ | mmap | Persistence tier: `mmap` (default, re-attachable) / `mem` (explicit, no cross-process recovery) / `none` |
| `raft.persistence.dir` | ❌ | `speedboat-data` | mmap tier persistence dir (auto sub-dir per `{nodeId}`) |
| `raft.mmap.size.mb` | ❌ | 128 | mmap tier WAL region size cap (MB) |
| `raft.mmap.file.name` | ❌ | `raft.mmap` | mmap file name (supports `{nodeId}` placeholder) |
| `raft.max.log.size` | ❌ | 4096 | In-memory log cap (defect-20260918-01: the sole rebuild basis for restarted replicas; must not truncate too deep) |
| `raft.checkpoint.interval` | ❌ | 1024 | State-machine checkpoint trigger threshold (applied delta; mmap tier only) |
| `lock.lease.ms` | ❌ | 30000 | Default distributed-lock lease (ms; shorter = faster takeover, more renewal cost) |

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

#### Cascading Cross-DC: Two-Level Observation API

In cross-datacenter cascading mode each process holds **two independent Rafts**:

- **Child group** (intra-datacenter layer, port `21001`) → elects the "local datacenter representative"
- **Parent group** (cross-datacenter layer, port `21001 + cross.port.offset`) → datacenter representatives contest for the "global leader"

State is therefore exposed per group, and the two `term` values are **each monotonically increasing and mutually unpolluted** — there is no conversion between them.

| Method | Description |
|---|---|
| `boolean isIntraLeader()` | Whether this process is the **child-group** Leader, i.e. the local datacenter representative |
| `boolean isParentLeader()` | Whether this process is the **parent-group** Leader (single datacenter has no parent group; always `true`) |
| `long getIntraTerm()` | Child-group (intra-datacenter) term |
| `long getParentTerm()` | Parent-group (cross-datacenter) term; `0` for single datacenter |
| `String getIntraLeaderId()` | nodeId of the child group's currently known Leader |
| `String getParentLeaderId()` | **Parent-group** nodeId of the parent group's currently known Leader; `null` for single datacenter |
| `String getParentNodeId()` | This process's parent-group nodeId (derived from the parent port); `null` for single datacenter |
| `int getParentPort()` | Parent-group bind port; `0` for single datacenter |

**Relationship of the three**: `isMain() == isIntraLeader() && isParentLeader()`.

```java
// Expected shape of a standby datacenter while the primary datacenter is alive — not a fault
Speedboat.isIntraLeader();   // true  —— I am the local datacenter representative
Speedboat.isParentLeader();  // false —— but did not win the parent-group election
Speedboat.isMain();          // false —— therefore not the global leader
```

> **Monitoring note**: `getParentLeaderId()` and `getIntraLeaderId()` draw from **different value domains**
> (the former is derived from the parent port, e.g. `node-x.x.x.x-22001`); never compare them to each other.
> Watching only the `leader` field misreads "one Leader per datacenter" as healthy —
> **global uniqueness must be counted via processes with `isMain() == true`**: at any instant ≤ 1.

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

#### Manually Promote Datacenter Priority (operational fallback)

```java
boolean ok = Speedboat.promoteDatacenter(operator, datacenterId, reason)
```

In cross-datacenter mode, raises this datacenter's parent-group priority to the point where
"casting one's own vote is enough to win". See the deployment guide section
"Manual Promotion and Restore". **Effective in CP mode only**: in AP mode the call is
ignored (an audit log entry only — no state change, no exception) and returns `false`.

#### Restore Datacenter Priority to Configured Values

```java
boolean ok = Speedboat.restoreDefaultPriorities(operator, reason)
```

Rolls the priority table back to the config-derived values. A restore does not leap the
term and does not seize leadership. Like the promotion, it is **effective in CP mode only**
and ignored in AP mode.

#### View Current Priority Snapshot

```java
DatacenterPriorityTable.Snapshot snap = Speedboat.getPrioritySnapshot()
```

Returns the parent group's current priority snapshot (epoch + weight table + source);
returns `null` in single-datacenter mode or for the child group.

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
| Loss observability | `lock.isLost()` turns true on a rejected RENEW or a stale local view; **check before publishing** (no callbacks — polling is the only channel in this version) |
| Fairness | Non-preemptive => no starvation-free guarantee (renew-and-hold wins); retries carry ±50% jitter against thundering herd |

#### Restart-replica boundary (resolved by the P4 persistence tiers)

With `raft.persistence` tiers (**default `mmap**` — write cost is same order as
`mem`, but cross-process restart safety is strictly superior, so the safest tier
is the default) a restarted process re-attaches checkpoint/WAL and rebuilds lock table and
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

### CP/AP Consistency Dual Mode

When an entire datacenter loses connectivity, the parent group faces two mutually
exclusive trade-offs. The choice is made via `consistency.policy` **at startup** and is
constant for the whole lifecycle (a runtime switch would equal redefining the majority
quorum and would break safety).

| Mode | Trade-off | On full datacenter loss | Cluster-wide `isMain` count |
|---|---|---|---|
| `cp` (default) | Strong consistency / uniqueness | Degrades to **leaderless** (0 leaders); never takes over | Always ≤ 1 |
| `ap` | Availability first | After the loss exceeds `consistency.degraded.timeout.ms` (default 10s), **degrades and takes over** — the local datacenter becomes leader alone | May briefly be 2 during a partition |

```properties
# Availability first: take over after 10s of peer-datacenter unreachability
consistency.policy=ap
consistency.degraded.timeout.ms=10000
```

**Conditions for AP degraded takeover (all must hold)**: this process holds the local
datacenter representative seat, an explicit peer datacenter exists, and the peer has been
unresponsive past the threshold. After degrading, the peer datacenter is removed from the
majority denominator — a standby datacenter goes from `self(1) < required(2)` to
`self(1) ≥ required(1)` and can therefore become leader alone. Once the peer responds
again the process **automatically exits** the degraded state, the denominator returns to
full membership, and standard Raft term mechanics converge back to a single leader.

**Audit logs**: entering and leaving the degraded state each emit one `WARN`-level
`CONSISTENCY AUDIT` log line containing the datacenter, outage duration, threshold and
reason, for post-hoc tracing of the dual-leader window.

**Observation semantics differ per mode**:

- CP mode: existing semantics hold — the number of processes with `isMain == true` is
  **≤ 1 at any instant**.
- AP mode: during a partition window that count **may briefly be 2** (each datacenter has
  a self-believed leader). Alerting rules must be mode-aware — under CP "count > 1" is a
  P0 fault; under AP it is **expected behaviour**, and only a count that stays > 1 after
  partition healing is a fault.
- In either mode, after healing the system **must** converge to exactly 1 leader
  (or 0, only when an entire CP datacenter is gone).

> **Cost of AP**: degraded takeover means both datacenters may acknowledge writes to
> clients during the partition. Enable it only when the two sides' data can be merged
> eventually / the business tolerates brief dual-writes; keep `cp` (default) when strong
> data consistency is a hard requirement.

### Manual Promotion and Restore (Operational Fallback)

When the **primary datacenter as a whole is unreachable**, standby datacenters cannot win
with their lower weight (e.g. `1 < required 2`) and CP degrades to leaderless. Operations
can then **explicitly promote** a standby datacenter's parent-group priority through the
facade API, letting it win on its own vote and restoring the `isMain` unique-leader
semantics. This is a **manual fallback path** — never triggered automatically, no remote
interface, no console.

> **Mode applicability (CP only)**: manual switching is **effective only when
> `consistency.policy=cp`**. AP has explicitly accepted "brief dual leaders during
> partition"; "make one datacenter the unique leader" has no well-defined semantics in
> that mode, so calls under AP are **ignored** — only an `op=IGNORED` `PROMOTE-AUDIT`
> log entry is written, **no state change, no exception, no error return**; the call
> returns `false`. This is deliberate: operational scripts can treat "mode not
> applicable" as an identifiable no-op rather than retrying it as a failure.
> `getPrioritySnapshot()` is a read-only query and keeps working under AP.

#### Promote: `promoteDatacenter`

```java
// Call on any representative process of the standby datacenter (operator is mandatory, for audit)
boolean ok = Speedboat.promoteDatacenter("alice", "beijing002", "hangzhou datacenter fully down");
```

**Acceptance preconditions (all must hold)**:

1. The singleton is running;
2. **CP mode** (AP ignores the call, see above);
3. Cross-datacenter mode, and `operator` is non-empty (mandatory for audit);
4. The target `datacenterId` **equals the local datacenter** (you cannot promote the peer);
5. **Reachability gate**: probe every parent-group endpoint of the peer datacenter —
   **if any is reachable, reject**: forcing promotion while the network is still alive
   would open a "dual-leader" window.

**Execution order after acceptance** (inside the raft single thread): swap table (local
weight = `Σ(other datacenter weights) + 1`, so one's own vote wins for any number of
datacenters) → term leap (`promote.term.leap`, default 100) → self-vote coronation →
append and replicate one `PRIORITY_CHANGE` log entry → write the parent-side standalone
persistent file. After promotion the local datacenter exactly reaches the quorum threshold
(Σ peer weights + 1 ≥ required), the peers are permanently one vote short, dual leadership
is **mathematically impossible in weights**, independent of network state, and the same
holds in three-or-more-datacenter topologies.

#### Restore: `restoreDefaultPriorities`

```java
// After the primary datacenter recovers, hand the table back to config-derived values
// (call on the current parent-group Leader)
boolean ok = Speedboat.restoreDefaultPriorities("alice", "hangzhou datacenter recovered");
```

A restore uses the config-derived table as the new snapshot, increments the epoch with
source `CONFIG`, and travels the same replicate + persist + heartbeat-propagation path.
A restore **does not leap the term and does not seize leadership** — it only lowers the
local weight back to the configured value. The primary datacenter can win the leadership
back at the next election thanks to its higher weight, but this **does not happen
automatically**: its representative must itself start an election, or in CP it is
naturally triggered after a check-quorum demotion.

#### View Current Priority Snapshot

```java
DatacenterPriorityTable.Snapshot snap = Speedboat.getPrioritySnapshot();
// snap.epoch() / snap.weights() / snap.source()
```

#### Audit Log

Every promotion/restore call (accepted / ignored / rejected) emits one `WARN`-level
structured `PROMOTE-AUDIT` log line with a fixed field order for easy grepping:
`op / operator / node / dc / target / reason / detail /
termBefore / termAfter / epochBefore / epochAfter / weightsBefore / weightsAfter`.

`op` values: `ACCEPTED` (took effect), `EXEC-FAILED` (execution failed inside the raft
thread), `REJECTED` (precondition check / reachability gate rejected, `detail` carries
the reason), `IGNORED` (manual switch ignored under AP mode,
`detail=manual-switch-ignored-in-ap-mode`).
`IGNORED` means "mode not applicable", **not a fault** — it must not trigger alert
retries.

#### Persistence Across Restarts

The priority table is written to a **standalone file** `parent-priority.properties`
(in the parent-group persistence directory), fully separate from the `RaftStore` mmap
term tier. On parent-group startup, if that file exists its values override the
config-derived table — **a restart never loses the manual promotion state**.

#### Risk Warnings (read before use)

> **Never automate promotion/restore.** The premise of this path is that operations has
> already confirmed out-of-band that the primary datacenter is truly down or truly
> partitioned. The reachability gate is only the last mechanical line of defence: it
> probes "are the peer's parent-group endpoints reachable" and cannot distinguish "the
> primary is truly dead" from "the probe link merely flickered". Promoting rashly while
> the primary only suffered a **transient blip** may still produce a brief dual-leader
> window.
>
> **Restores need equal care.** If the primary has not genuinely recovered, a restore
> returns the system to the "standby datacenter, leaderless" state. Restore only after
> confirming the primary can participate in elections again.
>
> **Promotion solves "unique leader", not data.** It lets a standby become leader to
> restore write semantics, but the primary's historical data still has to catch up
> through Raft log replication; promotion never skips log replication or state-machine
> replay.

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
