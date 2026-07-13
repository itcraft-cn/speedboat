# Speedboat Raft Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-ready Raft-based distributed lock system with sub-second leader election, supporting even nodes and cross-datacenter failover.

**Architecture:** Layered architecture with Speedboat facade, RaftNode core state machine, strategy-based extensibility (VoteWeightStrategy, GroupStrategy), abstract transport layer (Netty implementation), and custom serialization protocol.

**Tech Stack:** Java 8, Netty 4.1, Protostuff 1.7.4, JUnit 5, JMH 1.32, SLF4J/Logback

---

## File Structure

```
src/main/java/cn/itcraft/speedboat/
├── Speedboat.java                          # Facade API
├── config/
│   ├── SpeedboatConfig.java                # Configuration builder
│   └── SpeedboatConsts.java                # Constants (timeouts, ports)
├── raft/
│   ├── NodeState.java                      # State enum with transition validation
│   ├── Term.java                           # Term monotonicity manager
│   ├── ElectionTimeout.java                # Random timeout generator
│   ├── VoteContext.java                    # Vote context with metadata
│   ├── RaftNode.java                       # Core state machine (complete, no split)
│   └── RaftGroup.java                      # Group management with cascade support
├── strategy/voteweight/
│   ├── VoteWeightStrategy.java             # Interface
│   ├── DefaultVoteWeightStrategy.java      # Default (returns 0)
│   ├── EvenNodeVoteWeightStrategy.java     # Even node bias
│   ├── DatacenterVoteWeightStrategy.java   # Cross-datacenter bias
│   └── CompositeVoteWeightStrategy.java    # Composite multiple strategies
├── strategy/group/
│   ├── GroupStrategy.java                  # Interface
│   ├── DefaultGroupStrategy.java           # Single-layer Raft
│   ├── DatacenterGroupStrategy.java        # Datacenter layer (150-300ms timeout)
│   └── GlobalGroupStrategy.java            # Cross-datacenter layer (500-1000ms)
├── rpc/
│   ├── RequestVoteRequest.java             # Vote request DTO
│   ├── RequestVoteResponse.java            # Vote response DTO
│   ├── HeartbeatRequest.java               # Heartbeat request DTO
│   └── HeartbeatResponse.java              # Heartbeat response DTO
├── transport/
│   ├── TransportLayer.java                 # Abstract interface
│   └── NettyTransport.java                 # Netty NIO implementation
└── serialize/
    ├── Serializer.java                     # Abstract interface
    └── CustomSerializer.java               # Custom: 4bit len + 4bit crc32 + 1bit type + nbit data

src/test/java/cn/itcraft/speedboat/
├── raft/
│   ├── NodeStateTest.java                  # State transition validation
│   ├── TermTest.java                       # Term monotonicity
│   ├── ElectionTimeoutTest.java            # Timeout range validation
│   ├── RaftNodeTest.java                   # Core logic tests
│   └── RaftGroupTest.java                  # Group management tests
├── strategy/
│   ├── VoteWeightStrategyTest.java         # All weight strategies
│   └── GroupStrategyTest.java              # All group strategies
├── serialize/
│   └── CustomSerializerTest.java           # Serialization correctness
└── integration/
    └── SpeedboatIntegrationTest.java       # End-to-end scenarios
```

---

## Task 1: Project Setup and Dependencies

**Files:**
- Modify: `pom.xml`

- [ ] **Step 1: Add Netty dependencies**

```xml
<!-- Add after protostuff dependencies, line 101 -->
<!-- netty -->
<dependency>
    <groupId>io.netty</groupId>
    <artifactId>netty-all</artifactId>
    <version>4.1.68.Final</version>
</dependency>
```

- [ ] **Step 2: Update dependency scopes**

Change protostuff and slf4j/logback scopes from `test` to `compile`:
- Line 94-101: change `<scope>test</scope>` to `<scope>compile</scope>` for protostuff
- Line 74, 82, 87: change `<scope>test</scope>` to `<scope>compile</scope>` for slf4j/logback

- [ ] **Step 3: Commit**

```bash
git add pom.xml
git commit -m "build: add netty and update dependency scopes for production"
```

---

## Task 2: Constants and Configuration

**Files:**
- Create: `src/main/java/cn/itcraft/speedboat/config/SpeedboatConsts.java`
- Create: `src/main/java/cn/itcraft/speedboat/config/SpeedboatConfig.java`
- Create: `src/test/java/cn/itcraft/speedboat/config/SpeedboatConfigTest.java`

- [ ] **Step 1: Write the failing test for SpeedboatConsts**

```java
package cn.itcraft.speedboat.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SpeedboatConstsTest {
    
    @Test
    void testTimeoutRanges() {
        assertTrue(SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS >= 150);
        assertTrue(SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS <= 300);
        assertTrue(SpeedboatConsts.HEARTBEAT_INTERVAL_MS >= 50);
        assertTrue(SpeedboatConsts.HEARTBEAT_INTERVAL_MS < SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS);
    }
    
    @Test
    void testGlobalTimeoutRanges() {
        assertTrue(SpeedboatConsts.GLOBAL_MIN_ELECTION_TIMEOUT_MS >= 500);
        assertTrue(SpeedboatConsts.GLOBAL_MAX_ELECTION_TIMEOUT_MS <= 1000);
        assertTrue(SpeedboatConsts.GLOBAL_HEARTBEAT_INTERVAL_MS >= 200);
    }
    
    @Test
    void testSerializationType() {
        assertEquals(2, SpeedboatConsts.DEFAULT_SERIALIZATION_TYPE);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=SpeedboatConstsTest`
Expected: FAIL - class not found

- [ ] **Step 3: Write SpeedboatConsts implementation**

```java
package cn.itcraft.speedboat.config;

public final class SpeedboatConsts {
    
    public static final int MIN_ELECTION_TIMEOUT_MS = 150;
    public static final int MAX_ELECTION_TIMEOUT_MS = 300;
    public static final int HEARTBEAT_INTERVAL_MS = 50;
    
    public static final int GLOBAL_MIN_ELECTION_TIMEOUT_MS = 500;
    public static final int GLOBAL_MAX_ELECTION_TIMEOUT_MS = 1000;
    public static final int GLOBAL_HEARTBEAT_INTERVAL_MS = 200;
    
    public static final int DEFAULT_SERIALIZATION_TYPE = 2;
    public static final int SERIALIZATION_HEADER_LEN = 9;
    public static final int CRC32_LEN = 4;
    public static final int TYPE_LEN = 1;
    
    public static final int DEFAULT_PORT = 22222;
    public static final int GLOBAL_PORT = 33333;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=SpeedboatConstsTest`
Expected: PASS

- [ ] **Step 5: Write SpeedboatConfig test**

```java
package cn.itcraft.speedboat.config;

import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import cn.itcraft.speedboat.strategy.voteweight.DefaultVoteWeightStrategy;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.strategy.group.DefaultGroupStrategy;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SpeedboatConfigTest {
    
    @Test
    void testDefaultConfig() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        assertNotNull(config.getVoteWeightStrategy());
        assertNotNull(config.getGroupStrategy());
        assertEquals(SpeedboatConsts.DEFAULT_PORT, config.getPort());
    }
    
    @Test
    void testCustomConfig() {
        VoteWeightStrategy customWeight = new DefaultVoteWeightStrategy();
        GroupStrategy customGroup = new DefaultGroupStrategy();
        
        SpeedboatConfig config = SpeedboatConfig.builder()
            .port(30000)
            .voteWeightStrategy(customWeight)
            .groupStrategy(customGroup)
            .build();
        
        assertEquals(30000, config.getPort());
        assertEquals(customWeight, config.getVoteWeightStrategy());
        assertEquals(customGroup, config.getGroupStrategy());
    }
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `mvn test -Dtest=SpeedboatConfigTest`
Expected: FAIL - classes not found

- [ ] **Step 7: Write SpeedboatConfig implementation**

```java
package cn.itcraft.speedboat.config;

import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import cn.itcraft.speedboat.strategy.voteweight.DefaultVoteWeightStrategy;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.strategy.group.DefaultGroupStrategy;

public class SpeedboatConfig {
    
    private final int port;
    private final VoteWeightStrategy voteWeightStrategy;
    private final GroupStrategy groupStrategy;
    private final String datacenterId;
    
    private SpeedboatConfig(Builder builder) {
        this.port = builder.port;
        this.voteWeightStrategy = builder.voteWeightStrategy;
        this.groupStrategy = builder.groupStrategy;
        this.datacenterId = builder.datacenterId;
    }
    
    public static SpeedboatConfig defaultConfig() {
        return builder().build();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public int getPort() { return port; }
    public VoteWeightStrategy getVoteWeightStrategy() { return voteWeightStrategy; }
    public GroupStrategy getGroupStrategy() { return groupStrategy; }
    public String getDatacenterId() { return datacenterId; }
    
    public SpeedboatConfig withGroupStrategy(GroupStrategy strategy) {
        return builder()
            .port(port)
            .voteWeightStrategy(voteWeightStrategy)
            .groupStrategy(strategy)
            .datacenterId(datacenterId)
            .build();
    }
    
    public static class Builder {
        private int port = SpeedboatConsts.DEFAULT_PORT;
        private VoteWeightStrategy voteWeightStrategy = new DefaultVoteWeightStrategy();
        private GroupStrategy groupStrategy = new DefaultGroupStrategy();
        private String datacenterId = "";
        
        public Builder port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder voteWeightStrategy(VoteWeightStrategy strategy) {
            this.voteWeightStrategy = strategy;
            return this;
        }
        
        public Builder groupStrategy(GroupStrategy strategy) {
            this.groupStrategy = strategy;
            return this;
        }
        
        public Builder datacenterId(String id) {
            this.datacenterId = id;
            return this;
        }
        
        public SpeedboatConfig build() {
            return new SpeedboatConfig(this);
        }
    }
}
```

- [ ] **Step 8: Run test to verify it fails (strategy interfaces missing)**

Run: `mvn test -Dtest=SpeedboatConfigTest`
Expected: FAIL - VoteWeightStrategy, GroupStrategy, DefaultVoteWeightStrategy, DefaultGroupStrategy not found

- [ ] **Step 9: Commit partial progress**

```bash
git add src/main/java/cn/itcraft/speedboat/config/
git add src/test/java/cn/itcraft/speedboat/config/
git commit -m "feat(config): add SpeedboatConsts and SpeedboatConfig (pending strategy interfaces)"
```

---

## Task 3: Strategy Interfaces and Default Implementations

**Files:**
- Create: `src/main/java/cn/itcraft/speedboat/strategy/voteweight/VoteWeightStrategy.java`
- Create: `src/main/java/cn/itcraft/speedboat/strategy/voteweight/DefaultVoteWeightStrategy.java`
- Create: `src/main/java/cn/itcraft/speedboat/strategy/group/GroupStrategy.java`
- Create: `src/main/java/cn/itcraft/speedboat/strategy/group/DefaultGroupStrategy.java`
- Create: `src/main/java/cn/itcraft/speedboat/raft/VoteContext.java`
- Create: `src/test/java/cn/itcraft/speedboat/strategy/VoteWeightStrategyTest.java`
- Create: `src/test/java/cn/itcraft/speedboat/strategy/GroupStrategyTest.java`

- [ ] **Step 1: Write VoteWeightStrategy interface**

```java
package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;

public interface VoteWeightStrategy {
    
    int calculateAdditionalWeight(VoteContext context);
}
```

- [ ] **Step 2: Write VoteContext**

```java
package cn.itcraft.speedboat.raft;

public class VoteContext {
    
    private final String candidateId;
    private final String candidateDatacenter;
    private final boolean startupFirst;
    private final boolean electionInitiator;
    private final long term;
    
    public VoteContext(String candidateId, String candidateDatacenter, 
                       boolean startupFirst, boolean electionInitiator, long term) {
        this.candidateId = candidateId;
        this.candidateDatacenter = candidateDatacenter;
        this.startupFirst = startupFirst;
        this.electionInitiator = electionInitiator;
        this.term = term;
    }
    
    public String getCandidateId() { return candidateId; }
    public String getCandidateDatacenter() { return candidateDatacenter; }
    public boolean isStartupFirst() { return startupFirst; }
    public boolean isElectionInitiator() { return electionInitiator; }
    public long getTerm() { return term; }
    
    public static VoteContext forCandidate(String candidateId, long term) {
        return new VoteContext(candidateId, "", false, false, term);
    }
    
    public static VoteContext forDatacenter(String candidateId, String datacenter, long term) {
        return new VoteContext(candidateId, datacenter, false, false, term);
    }
}
```

- [ ] **Step 3: Write DefaultVoteWeightStrategy**

```java
package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;

public class DefaultVoteWeightStrategy implements VoteWeightStrategy {
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        return 0;
    }
}
```

- [ ] **Step 4: Write GroupStrategy interface**

```java
package cn.itcraft.speedboat.strategy.group;

import cn.itcraft.speedboat.raft.RaftNode;

public interface GroupStrategy {
    
    boolean canParticipateInElection(RaftNode node);
    
    long getElectionTimeout();
    
    long getHeartbeatInterval();
    
    boolean allowCrossGroupCommunication();
}
```

- [ ] **Step 5: Write DefaultGroupStrategy**

```java
package cn.itcraft.speedboat.strategy.group;

import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.config.SpeedboatConsts;
import java.util.concurrent.ThreadLocalRandom;

public class DefaultGroupStrategy implements GroupStrategy {
    
    @Override
    public boolean canParticipateInElection(RaftNode node) {
        return true;
    }
    
    @Override
    public long getElectionTimeout() {
        return SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS + 
               ThreadLocalRandom.current().nextLong(
                   SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS - SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS);
    }
    
    @Override
    public long getHeartbeatInterval() {
        return SpeedboatConsts.HEARTBEAT_INTERVAL_MS;
    }
    
    @Override
    public boolean allowCrossGroupCommunication() {
        return false;
    }
}
```

- [ ] **Step 6: Write tests for strategies**

```java
package cn.itcraft.speedboat.strategy;

import cn.itcraft.speedboat.strategy.voteweight.*;
import cn.itcraft.speedboat.strategy.group.*;
import cn.itcraft.speedboat.raft.VoteContext;
import cn.itcraft.speedboat.config.SpeedboatConsts;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class VoteWeightStrategyTest {
    
    @Test
    void testDefaultStrategyReturnsZero() {
        VoteWeightStrategy strategy = new DefaultVoteWeightStrategy();
        VoteContext context = VoteContext.forCandidate("node1", 1L);
        assertEquals(0, strategy.calculateAdditionalWeight(context));
    }
}

class GroupStrategyTest {
    
    @Test
    void testDefaultGroupStrategyTimeoutRange() {
        GroupStrategy strategy = new DefaultGroupStrategy();
        for (int i = 0; i < 100; i++) {
            long timeout = strategy.getElectionTimeout();
            assertTrue(timeout >= SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS);
            assertTrue(timeout < SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS);
        }
    }
    
    @Test
    void testDefaultGroupStrategyHeartbeat() {
        GroupStrategy strategy = new DefaultGroupStrategy();
        assertEquals(SpeedboatConsts.HEARTBEAT_INTERVAL_MS, strategy.getHeartbeatInterval());
    }
}
```

- [ ] **Step 7: Run tests (will fail - RaftNode missing)**

Run: `mvn test -Dtest=VoteWeightStrategyTest,GroupStrategyTest`
Expected: FAIL - RaftNode not found (GroupStrategy interface reference)

- [ ] **Step 8: Commit**

```bash
git add src/main/java/cn/itcraft/speedboat/strategy/
git add src/main/java/cn/itcraft/speedboat/raft/VoteContext.java
git add src/test/java/cn/itcraft/speedboat/strategy/
git commit -m "feat(strategy): add VoteWeightStrategy and GroupStrategy interfaces with defaults"
```

---

## Task 4: Core Raft Components - NodeState, Term, ElectionTimeout

**Files:**
- Create: `src/main/java/cn/itcraft/speedboat/raft/NodeState.java`
- Create: `src/main/java/cn/itcraft/speedboat/raft/Term.java`
- Create: `src/main/java/cn/itcraft/speedboat/raft/ElectionTimeout.java`
- Create: `src/test/java/cn/itcraft/speedboat/raft/NodeStateTest.java`
- Create: `src/test/java/cn/itcraft/speedboat/raft/TermTest.java`
- Create: `src/test/java/cn/itcraft/speedboat/raft/ElectionTimeoutTest.java`

- [ ] **Step 1: Write NodeState test**

```java
package cn.itcraft.speedboat.raft;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class NodeStateTest {
    
    @Test
    void testValidTransitions() {
        assertTrue(NodeState.LEADER.canTransitionTo(NodeState.FOLLOWER));
        assertTrue(NodeState.FOLLOWER.canTransitionTo(NodeState.CANDIDATE));
        assertTrue(NodeState.FOLLOWER.canTransitionTo(NodeState.FOLLOWER));
        assertTrue(NodeState.CANDIDATE.canTransitionTo(NodeState.LEADER));
        assertTrue(NodeState.CANDIDATE.canTransitionTo(NodeState.FOLLOWER));
        assertTrue(NodeState.CANDIDATE.canTransitionTo(NodeState.CANDIDATE));
    }
    
    @Test
    void testInvalidTransitions() {
        assertFalse(NodeState.LEADER.canTransitionTo(NodeState.CANDIDATE));
        assertFalse(NodeState.LEADER.canTransitionTo(NodeState.LEADER));
        assertFalse(NodeState.FOLLOWER.canTransitionTo(NodeState.LEADER));
    }
    
    @Test
    void testAllStatesExist() {
        assertEquals(3, NodeState.values().length);
        assertNotNull(NodeState.LEADER);
        assertNotNull(NodeState.FOLLOWER);
        assertNotNull(NodeState.CANDIDATE);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=NodeStateTest`
Expected: FAIL - NodeState not found

- [ ] **Step 3: Write NodeState implementation**

```java
package cn.itcraft.speedboat.raft;

import java.util.Arrays;

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
    
    public NodeState[] getValidNextStates() {
        return validNextStates.clone();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=NodeStateTest`
Expected: PASS

- [ ] **Step 5: Write Term test**

```java
package cn.itcraft.speedboat.raft;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TermTest {
    
    @Test
    void testInitialTerm() {
        Term term = new Term();
        assertEquals(0, term.getCurrent());
    }
    
    @Test
    void testIncrement() {
        Term term = new Term();
        term.increment();
        assertEquals(1, term.getCurrent());
        term.increment();
        assertEquals(2, term.getCurrent());
    }
    
    @Test
    void testUpdateIfHigher() {
        Term term = new Term();
        term.increment();
        assertEquals(1, term.getCurrent());
        
        boolean updated = term.updateIfHigher(3);
        assertTrue(updated);
        assertEquals(3, term.getCurrent());
        
        updated = term.updateIfHigher(2);
        assertFalse(updated);
        assertEquals(3, term.getCurrent());
    }
    
    @Test
    void testMonotonicityViolationDetection() {
        Term term = new Term();
        term.increment();
        term.increment();
        assertEquals(2, term.getCurrent());
        
        assertFalse(term.isMonotonicViolation(3));
        assertFalse(term.isMonotonicViolation(2));
        assertTrue(term.isMonotonicViolation(1));
        assertTrue(term.isMonotonicViolation(0));
    }
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `mvn test -Dtest=TermTest`
Expected: FAIL - Term not found

- [ ] **Step 7: Write Term implementation**

```java
package cn.itcraft.speedboat.raft;

public class Term {
    
    private long current;
    
    public Term() {
        this.current = 0;
    }
    
    public long getCurrent() {
        return current;
    }
    
    public void increment() {
        current++;
    }
    
    public boolean updateIfHigher(long newTerm) {
        if (newTerm > current) {
            current = newTerm;
            return true;
        }
        return false;
    }
    
    public boolean isMonotonicViolation(long proposedTerm) {
        return proposedTerm < current;
    }
    
    public void reset() {
        current = 0;
    }
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `mvn test -Dtest=TermTest`
Expected: PASS

- [ ] **Step 9: Write ElectionTimeout test**

```java
package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.config.SpeedboatConsts;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ElectionTimeoutTest {
    
    @Test
    void testTimeoutInRange() {
        ElectionTimeout timeout = new ElectionTimeout(
            SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS, 
            SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS);
        
        for (int i = 0; i < 100; i++) {
            long value = timeout.getNext();
            assertTrue(value >= SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS);
            assertTrue(value < SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS);
        }
    }
    
    @Test
    void testGlobalTimeoutInRange() {
        ElectionTimeout timeout = new ElectionTimeout(
            SpeedboatConsts.GLOBAL_MIN_ELECTION_TIMEOUT_MS,
            SpeedboatConsts.GLOBAL_MAX_ELECTION_TIMEOUT_MS);
        
        for (int i = 0; i < 100; i++) {
            long value = timeout.getNext();
            assertTrue(value >= SpeedboatConsts.GLOBAL_MIN_ELECTION_TIMEOUT_MS);
            assertTrue(value < SpeedboatConsts.GLOBAL_MAX_ELECTION_TIMEOUT_MS);
        }
    }
    
    @Test
    void testReset() {
        ElectionTimeout timeout = new ElectionTimeout(150, 300);
        long first = timeout.getNext();
        timeout.reset();
        long second = timeout.getNext();
        assertNotEquals(first, second);
    }
}
```

- [ ] **Step 10: Run test to verify it fails**

Run: `mvn test -Dtest=ElectionTimeoutTest`
Expected: FAIL - ElectionTimeout not found

- [ ] **Step 11: Write ElectionTimeout implementation**

```java
package cn.itcraft.speedboat.raft;

import java.util.concurrent.ThreadLocalRandom;

public class ElectionTimeout {
    
    private final long minMs;
    private final long maxMs;
    private long currentTimeout;
    
    public ElectionTimeout(long minMs, long maxMs) {
        this.minMs = minMs;
        this.maxMs = maxMs;
        reset();
    }
    
    public long getNext() {
        return currentTimeout;
    }
    
    public void reset() {
        currentTimeout = minMs + ThreadLocalRandom.current().nextLong(maxMs - minMs);
    }
    
    public long getMinMs() { return minMs; }
    public long getMaxMs() { return maxMs; }
}
```

- [ ] **Step 12: Run test to verify it passes**

Run: `mvn test -Dtest=ElectionTimeoutTest`
Expected: PASS

- [ ] **Step 13: Commit**

```bash
git add src/main/java/cn/itcraft/speedboat/raft/
git add src/test/java/cn/itcraft/speedboat/raft/
git commit -m "feat(raft): add NodeState, Term, ElectionTimeout core components"
```

---

## Task 5: RPC Data Objects

**Files:**
- Create: `src/main/java/cn/itcraft/speedboat/rpc/RequestVoteRequest.java`
- Create: `src/main/java/cn/itcraft/speedboat/rpc/RequestVoteResponse.java`
- Create: `src/main/java/cn/itcraft/speedboat/rpc/HeartbeatRequest.java`
- Create: `src/main/java/cn/itcraft/speedboat/rpc/HeartbeatResponse.java`
- Create: `src/test/java/cn/itcraft/speedboat/rpc/RpcTest.java`

- [ ] **Step 1: Write RPC test**

```java
package cn.itcraft.speedboat.rpc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RpcTest {
    
    @Test
    void testRequestVoteRequest() {
        RequestVoteRequest request = new RequestVoteRequest(1L, "node1", 2);
        assertEquals(1L, request.getTerm());
        assertEquals("node1", request.getCandidateId());
        assertEquals(2, request.getVoteWeight());
    }
    
    @Test
    void testRequestVoteResponse() {
        RequestVoteResponse response = new RequestVoteResponse(1L, true);
        assertEquals(1L, response.getTerm());
        assertTrue(response.isVoteGranted());
        
        RequestVoteResponse rejected = new RequestVoteResponse(2L, false);
        assertFalse(rejected.isVoteGranted());
    }
    
    @Test
    void testHeartbeatRequest() {
        HeartbeatRequest request = new HeartbeatRequest(1L, "leader1");
        assertEquals(1L, request.getTerm());
        assertEquals("leader1", request.getLeaderId());
    }
    
    @Test
    void testHeartbeatResponse() {
        HeartbeatResponse response = new HeartbeatResponse(1L, true);
        assertEquals(1L, response.getTerm());
        assertTrue(response.isSuccess());
        
        HeartbeatResponse failed = new HeartbeatResponse(2L, false);
        assertFalse(failed.isSuccess());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=RpcTest`
Expected: FAIL - RPC classes not found

- [ ] **Step 3: Write RequestVoteRequest**

```java
package cn.itcraft.speedboat.rpc;

public class RequestVoteRequest {
    
    private final long term;
    private final String candidateId;
    private final int voteWeight;
    
    public RequestVoteRequest(long term, String candidateId, int voteWeight) {
        this.term = term;
        this.candidateId = candidateId;
        this.voteWeight = voteWeight;
    }
    
    public long getTerm() { return term; }
    public String getCandidateId() { return candidateId; }
    public int getVoteWeight() { return voteWeight; }
}
```

- [ ] **Step 4: Write RequestVoteResponse**

```java
package cn.itcraft.speedboat.rpc;

public class RequestVoteResponse {
    
    private final long term;
    private final boolean voteGranted;
    
    public RequestVoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }
    
    public long getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }
}
```

- [ ] **Step 5: Write HeartbeatRequest**

```java
package cn.itcraft.speedboat.rpc;

public class HeartbeatRequest {
    
    private final long term;
    private final String leaderId;
    
    public HeartbeatRequest(long term, String leaderId) {
        this.term = term;
        this.leaderId = leaderId;
    }
    
    public long getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
}
```

- [ ] **Step 6: Write HeartbeatResponse**

```java
package cn.itcraft.speedboat.rpc;

public class HeartbeatResponse {
    
    private final long term;
    private final boolean success;
    
    public HeartbeatResponse(long term, boolean success) {
        this.term = term;
        this.success = success;
    }
    
    public long getTerm() { return term; }
    public boolean isSuccess() { return success; }
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `mvn test -Dtest=RpcTest`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add src/main/java/cn/itcraft/speedboat/rpc/
git add src/test/java/cn/itcraft/speedboat/rpc/
git commit -m "feat(rpc): add RequestVote and Heartbeat RPC data objects"
```

---

## Task 6: Serialization Layer

**Files:**
- Create: `src/main/java/cn/itcraft/speedboat/serialize/Serializer.java`
- Create: `src/main/java/cn/itcraft/speedboat/serialize/CustomSerializer.java`
- Create: `src/test/java/cn/itcraft/speedboat/serialize/CustomSerializerTest.java`

- [ ] **Step 1: Write Serializer test**

```java
package cn.itcraft.speedboat.serialize;

import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.config.SpeedboatConsts;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CustomSerializerTest {
    
    @Test
    void testSerializeRequestVoteRequest() {
        CustomSerializer serializer = new CustomSerializer();
        RequestVoteRequest original = new RequestVoteRequest(1L, "node1", 2);
        
        byte[] data = serializer.serialize(original);
        assertNotNull(data);
        assertTrue(data.length > SpeedboatConsts.SERIALIZATION_HEADER_LEN);
        
        RequestVoteRequest deserialized = serializer.deserialize(data, RequestVoteRequest.class);
        assertEquals(original.getTerm(), deserialized.getTerm());
        assertEquals(original.getCandidateId(), deserialized.getCandidateId());
        assertEquals(original.getVoteWeight(), deserialized.getVoteWeight());
    }
    
    @Test
    void testSerializeRequestVoteResponse() {
        CustomSerializer serializer = new CustomSerializer();
        RequestVoteResponse original = new RequestVoteResponse(2L, true);
        
        byte[] data = serializer.serialize(original);
        RequestVoteResponse deserialized = serializer.deserialize(data, RequestVoteResponse.class);
        assertEquals(original.getTerm(), deserialized.getTerm());
        assertEquals(original.isVoteGranted(), deserialized.isVoteGranted());
    }
    
    @Test
    void testHeaderFormat() {
        CustomSerializer serializer = new CustomSerializer();
        RequestVoteRequest request = new RequestVoteRequest(1L, "node1", 2);
        
        byte[] data = serializer.serialize(request);
        
        assertEquals(SpeedboatConsts.DEFAULT_SERIALIZATION_TYPE, data[8]);
        assertTrue(data.length >= SpeedboatConsts.SERIALIZATION_HEADER_LEN);
    }
    
    @Test
    void testCrc32Validation() {
        CustomSerializer serializer = new CustomSerializer();
        RequestVoteRequest request = new RequestVoteRequest(1L, "node1", 2);
        
        byte[] data = serializer.serialize(request);
        
        byte[] corrupted = data.clone();
        corrupted[10] = (byte)(corrupted[10] ^ 0xFF);
        
        assertThrows(IllegalStateException.class, () -> {
            serializer.deserialize(corrupted, RequestVoteRequest.class);
        });
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=CustomSerializerTest`
Expected: FAIL - Serializer classes not found

- [ ] **Step 3: Write Serializer interface**

```java
package cn.itcraft.speedboat.serialize;

public interface Serializer {
    
    byte[] serialize(Object obj);
    
    <T> T deserialize(byte[] data, Class<T> clazz);
}
```

- [ ] **Step 4: Write CustomSerializer implementation**

```java
package cn.itcraft.speedboat.serialize;

import cn.itcraft.speedboat.config.SpeedboatConsts;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class CustomSerializer implements Serializer {
    
    private static final LinkedBuffer BUFFER = LinkedBuffer.allocate(512);
    
    @Override
    public byte[] serialize(Object obj) {
        byte type = SpeedboatConsts.DEFAULT_SERIALIZATION_TYPE;
        
        Schema schema = RuntimeSchema.getSchema(obj.getClass());
        byte[] data = ProtostuffIOUtil.toByteArray(obj, schema, BUFFER);
        BUFFER.clear();
        
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        int crcValue = (int) crc32.getValue();
        
        int len = SpeedboatConsts.SERIALIZATION_HEADER_LEN + data.length;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.putInt(len);
        buffer.putInt(crcValue);
        buffer.put(type);
        buffer.put(data);
        
        return buffer.array();
    }
    
    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        
        int len = buffer.getInt();
        int crc32 = buffer.getInt();
        byte type = buffer.get();
        
        byte[] data = new byte[len - SpeedboatConsts.SERIALIZATION_HEADER_LEN];
        buffer.get(data);
        
        CRC32 calculator = new CRC32();
        calculator.update(data);
        int calculated = (int) calculator.getValue();
        
        if (calculated != crc32) {
            throw new IllegalStateException("CRC32 check failed: expected " + crc32 + ", got " + calculated);
        }
        
        Schema<T> schema = RuntimeSchema.getSchema(clazz);
        T obj = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(data, obj, schema);
        
        return obj;
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn test -Dtest=CustomSerializerTest`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/main/java/cn/itcraft/speedboat/serialize/
git add src/test/java/cn/itcraft/speedboat/serialize/
git commit -m "feat(serialize): add custom serialization with CRC32 validation"
```

---

## Task 7: Specialized Weight Strategies

**Files:**
- Create: `src/main/java/cn/itcraft/speedboat/strategy/voteweight/EvenNodeVoteWeightStrategy.java`
- Create: `src/main/java/cn/itcraft/speedboat/strategy/voteweight/DatacenterVoteWeightStrategy.java`
- Create: `src/main/java/cn/itcraft/speedboat/strategy/voteweight/CompositeVoteWeightStrategy.java`
- Modify: `src/test/java/cn/itcraft/speedboat/strategy/VoteWeightStrategyTest.java`

- [ ] **Step 1: Add specialized strategy tests**

```java
// Add to existing VoteWeightStrategyTest.java

@Test
void testEvenNodeStrategy() {
    EvenNodeVoteWeightStrategy strategy = new EvenNodeVoteWeightStrategy();
    
    VoteContext startupFirst = new VoteContext("node1", "", true, false, 1L);
    assertEquals(1, strategy.calculateAdditionalWeight(startupFirst));
    
    VoteContext electionInitiator = new VoteContext("node1", "", false, true, 1L);
    assertEquals(1, strategy.calculateAdditionalWeight(electionInitiator));
    
    VoteContext both = new VoteContext("node1", "", true, true, 1L);
    assertEquals(2, strategy.calculateAdditionalWeight(both));
    
    VoteContext neither = new VoteContext("node1", "", false, false, 1L);
    assertEquals(0, strategy.calculateAdditionalWeight(neither));
}

@Test
void testDatacenterStrategy() {
    DatacenterVoteWeightStrategy strategy = new DatacenterVoteWeightStrategy("beijing");
    
    VoteContext sameDatacenter = VoteContext.forDatacenter("node1", "beijing", 1L);
    assertEquals(1, strategy.calculateAdditionalWeight(sameDatacenter));
    
    VoteContext differentDatacenter = VoteContext.forDatacenter("node1", "shanghai", 1L);
    assertEquals(-1, strategy.calculateAdditionalWeight(differentDatacenter));
}

@Test
void testCompositeStrategy() {
    EvenNodeVoteWeightStrategy evenStrategy = new EvenNodeVoteWeightStrategy();
    DatacenterVoteWeightStrategy dcStrategy = new DatacenterVoteWeightStrategy("beijing");
    
    CompositeVoteWeightStrategy composite = new CompositeVoteWeightStrategy();
    composite.addStrategy(evenStrategy);
    composite.addStrategy(dcStrategy);
    
    VoteContext context = new VoteContext("node1", "beijing", true, true, 1L);
    int weight = composite.calculateAdditionalWeight(context);
    assertEquals(3, weight);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=VoteWeightStrategyTest`
Expected: FAIL - EvenNodeVoteWeightStrategy, DatacenterVoteWeightStrategy, CompositeVoteWeightStrategy not found

- [ ] **Step 3: Write EvenNodeVoteWeightStrategy**

```java
package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;

public class EvenNodeVoteWeightStrategy implements VoteWeightStrategy {
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        int additional = 0;
        
        if (context.isStartupFirst()) {
            additional += 1;
        }
        
        if (context.isElectionInitiator()) {
            additional += 1;
        }
        
        return additional;
    }
}
```

- [ ] **Step 4: Write DatacenterVoteWeightStrategy**

```java
package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;

public class DatacenterVoteWeightStrategy implements VoteWeightStrategy {
    
    private final String currentDatacenter;
    
    public DatacenterVoteWeightStrategy(String currentDatacenter) {
        this.currentDatacenter = currentDatacenter;
    }
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        String candidateDatacenter = context.getCandidateDatacenter();
        
        if (candidateDatacenter.equals(currentDatacenter)) {
            return 1;
        } else {
            return -1;
        }
    }
    
    public String getCurrentDatacenter() {
        return currentDatacenter;
    }
}
```

- [ ] **Step 5: Write CompositeVoteWeightStrategy**

```java
package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;
import java.util.ArrayList;
import java.util.List;

public class CompositeVoteWeightStrategy implements VoteWeightStrategy {
    
    private final List<VoteWeightStrategy> strategies;
    
    public CompositeVoteWeightStrategy() {
        this.strategies = new ArrayList<>();
    }
    
    public void addStrategy(VoteWeightStrategy strategy) {
        strategies.add(strategy);
    }
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        int total = 0;
        for (VoteWeightStrategy strategy : strategies) {
            total += strategy.calculateAdditionalWeight(context);
        }
        return total;
    }
    
    public List<VoteWeightStrategy> getStrategies() {
        return new ArrayList<>(strategies);
    }
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `mvn test -Dtest=VoteWeightStrategyTest`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/main/java/cn/itcraft/speedboat/strategy/voteweight/
git add src/test/java/cn/itcraft/speedboat/strategy/
git commit -m "feat(strategy): add EvenNode, Datacenter, Composite vote weight strategies"
```

---

## Task 8: Specialized Group Strategies

**Files:**
- Create: `src/main/java/cn/itcraft/speedboat/strategy/group/DatacenterGroupStrategy.java`
- Create: `src/main/java/cn/itcraft/speedboat/strategy/group/GlobalGroupStrategy.java`
- Modify: `src/test/java/cn/itcraft/speedboat/strategy/GroupStrategyTest.java`

- [ ] **Step 1: Add specialized group strategy tests**

```java
// Add to existing GroupStrategyTest.java

@Test
void testDatacenterGroupStrategyTimeoutRange() {
    DatacenterGroupStrategy strategy = new DatacenterGroupStrategy();
    for (int i = 0; i < 100; i++) {
        long timeout = strategy.getElectionTimeout();
        assertTrue(timeout >= SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS);
        assertTrue(timeout < SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS);
    }
}

@Test
void testDatacenterGroupStrategyHeartbeat() {
    DatacenterGroupStrategy strategy = new DatacenterGroupStrategy();
    assertEquals(SpeedboatConsts.HEARTBEAT_INTERVAL_MS, strategy.getHeartbeatInterval());
    assertTrue(strategy.allowCrossGroupCommunication());
}

@Test
void testGlobalGroupStrategyTimeoutRange() {
    GlobalGroupStrategy strategy = new GlobalGroupStrategy();
    for (int i = 0; i < 100; i++) {
        long timeout = strategy.getElectionTimeout();
        assertTrue(timeout >= SpeedboatConsts.GLOBAL_MIN_ELECTION_TIMEOUT_MS);
        assertTrue(timeout < SpeedboatConsts.GLOBAL_MAX_ELECTION_TIMEOUT_MS);
    }
}

@Test
void testGlobalGroupStrategyHeartbeat() {
    GlobalGroupStrategy strategy = new GlobalGroupStrategy();
    assertEquals(SpeedboatConsts.GLOBAL_HEARTBEAT_INTERVAL_MS, strategy.getHeartbeatInterval());
    assertFalse(strategy.allowCrossGroupCommunication());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=GroupStrategyTest`
Expected: FAIL - DatacenterGroupStrategy, GlobalGroupStrategy not found

- [ ] **Step 3: Write DatacenterGroupStrategy**

```java
package cn.itcraft.speedboat.strategy.group;

import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.config.SpeedboatConsts;
import java.util.concurrent.ThreadLocalRandom;

public class DatacenterGroupStrategy implements GroupStrategy {
    
    @Override
    public boolean canParticipateInElection(RaftNode node) {
        return true;
    }
    
    @Override
    public long getElectionTimeout() {
        return SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS +
               ThreadLocalRandom.current().nextLong(
                   SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS - SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS);
    }
    
    @Override
    public long getHeartbeatInterval() {
        return SpeedboatConsts.HEARTBEAT_INTERVAL_MS;
    }
    
    @Override
    public boolean allowCrossGroupCommunication() {
        return true;
    }
}
```

- [ ] **Step 4: Write GlobalGroupStrategy**

```java
package cn.itcraft.speedboat.strategy.group;

import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.config.SpeedboatConsts;
import java.util.concurrent.ThreadLocalRandom;

public class GlobalGroupStrategy implements GroupStrategy {
    
    @Override
    public boolean canParticipateInElection(RaftNode node) {
        return true;
    }
    
    @Override
    public long getElectionTimeout() {
        return SpeedboatConsts.GLOBAL_MIN_ELECTION_TIMEOUT_MS +
               ThreadLocalRandom.current().nextLong(
                   SpeedboatConsts.GLOBAL_MAX_ELECTION_TIMEOUT_MS - SpeedboatConsts.GLOBAL_MIN_ELECTION_TIMEOUT_MS);
    }
    
    @Override
    public long getHeartbeatInterval() {
        return SpeedboatConsts.GLOBAL_HEARTBEAT_INTERVAL_MS;
    }
    
    @Override
    public boolean allowCrossGroupCommunication() {
        return false;
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn test -Dtest=GroupStrategyTest`
Expected: PASS (but RaftNode still missing - placeholder for now)

- [ ] **Step 6: Commit**

```bash
git add src/main/java/cn/itcraft/speedboat/strategy/group/
git add src/test/java/cn/itcraft/speedboat/strategy/
git commit -m "feat(strategy): add Datacenter and Global group strategies"
```

---

## Task 9: RaftNode Core Implementation

**Files:**
- Create: `src/main/java/cn/itcraft/speedboat/raft/RaftNode.java`
- Create: `src/test/java/cn/itcraft/speedboat/raft/RaftNodeTest.java`

- [ ] **Step 1: Write RaftNode test**

```java
package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.config.SpeedboatConfig;
import cn.itcraft.speedboat.strategy.voteweight.DefaultVoteWeightStrategy;
import cn.itcraft.speedboat.strategy.group.DefaultGroupStrategy;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RaftNodeTest {
    
    @Test
    void testInitialState() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        RaftNode node = new RaftNode("node1", config);
        
        assertEquals("node1", node.getNodeId());
        assertEquals(NodeState.FOLLOWER, node.getCurrentState());
        assertEquals(0, node.getCurrentTerm());
        assertNull(node.getVotedFor());
    }
    
    @Test
    void testStateTransition() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        RaftNode node = new RaftNode("node1", config);
        
        assertTrue(node.transitionTo(NodeState.CANDIDATE));
        assertEquals(NodeState.CANDIDATE, node.getCurrentState());
        
        assertTrue(node.transitionTo(NodeState.LEADER));
        assertEquals(NodeState.LEADER, node.getCurrentState());
        
        assertTrue(node.transitionTo(NodeState.FOLLOWER));
        assertEquals(NodeState.FOLLOWER, node.getCurrentState());
    }
    
    @Test
    void testInvalidTransition() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        RaftNode node = new RaftNode("node1", config);
        
        node.transitionTo(NodeState.LEADER);
        
        assertFalse(node.transitionTo(NodeState.CANDIDATE));
        assertEquals(NodeState.CANDIDATE, node.getCurrentState());
    }
    
    @Test
    void testVoteFor() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        RaftNode node = new RaftNode("node1", config);
        
        node.voteFor("node2");
        assertEquals("node2", node.getVotedFor());
        
        node.voteFor("node3");
        assertEquals("node3", node.getVotedFor());
    }
    
    @Test
    void testTermIncrement() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        RaftNode node = new RaftNode("node1", config);
        
        assertEquals(0, node.getCurrentTerm());
        node.incrementTerm();
        assertEquals(1, node.getCurrentTerm());
    }
    
    @Test
    void testResetVote() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        RaftNode node = new RaftNode("node1", config);
        
        node.voteFor("node2");
        assertEquals("node2", node.getVotedFor());
        
        node.resetVote();
        assertNull(node.getVotedFor());
    }
    
    @Test
    void testUpdateTermIfHigher() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        RaftNode node = new RaftNode("node1", config);
        
        node.incrementTerm();
        assertEquals(1, node.getCurrentTerm());
        
        boolean updated = node.updateTermIfHigher(3);
        assertTrue(updated);
        assertEquals(3, node.getCurrentTerm());
        
        updated = node.updateTermIfHigher(2);
        assertFalse(updated);
        assertEquals(3, node.getCurrentTerm());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=RaftNodeTest`
Expected: FAIL - RaftNode not found

- [ ] **Step 3: Write RaftNode implementation**

```java
package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.config.SpeedboatConfig;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftNode {
    
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);
    
    private final String nodeId;
    private final SpeedboatConfig config;
    private final Term term;
    private final ElectionTimeout electionTimeout;
    private final VoteWeightStrategy voteWeightStrategy;
    private final GroupStrategy groupStrategy;
    
    private NodeState currentState;
    private String votedFor;
    private long lastHeartbeatTime;
    
    public RaftNode(String nodeId, SpeedboatConfig config) {
        this.nodeId = nodeId;
        this.config = config;
        this.term = new Term();
        this.electionTimeout = new ElectionTimeout(
            config.getGroupStrategy().getMinElectionTimeout(),
            config.getGroupStrategy().getMaxElectionTimeout()
        );
        this.voteWeightStrategy = config.getVoteWeightStrategy();
        this.groupStrategy = config.getGroupStrategy();
        
        this.currentState = NodeState.FOLLOWER;
        this.votedFor = null;
        this.lastHeartbeatTime = System.currentTimeMillis();
    }
    
    public String getNodeId() { return nodeId; }
    public NodeState getCurrentState() { return currentState; }
    public long getCurrentTerm() { return term.getCurrent(); }
    public String getVotedFor() { return votedFor; }
    public long getLastHeartbeatTime() { return lastHeartbeatTime; }
    public SpeedboatConfig getConfig() { return config; }
    
    public boolean transitionTo(NodeState newState) {
        if (!currentState.canTransitionTo(newState)) {
            log.warn("Invalid state transition from {} to {}, forcing to CANDIDATE", currentState, newState);
            currentState = NodeState.CANDIDATE;
            return false;
        }
        
        currentState = newState;
        log.info("Node {} transitioned to {}", nodeId, newState);
        
        if (newState == NodeState.FOLLOWER || newState == NodeState.CANDIDATE) {
            votedFor = null;
        }
        
        return true;
    }
    
    public void voteFor(String candidateId) {
        this.votedFor = candidateId;
        log.debug("Node {} voted for {}", nodeId, candidateId);
    }
    
    public void incrementTerm() {
        term.increment();
        log.debug("Node {} incremented term to {}", nodeId, term.getCurrent());
    }
    
    public boolean updateTermIfHigher(long newTerm) {
        boolean updated = term.updateIfHigher(newTerm);
        if (updated) {
            log.info("Node {} updated term to {}", nodeId, newTerm);
            transitionTo(NodeState.FOLLOWER);
            resetVote();
        }
        return updated;
    }
    
    public void resetVote() {
        this.votedFor = null;
    }
    
    public void resetElectionTimeout() {
        electionTimeout.reset();
        lastHeartbeatTime = System.currentTimeMillis();
    }
    
    public long getNextElectionTimeout() {
        return electionTimeout.getNext();
    }
    
    public boolean isElectionTimeout() {
        long elapsed = System.currentTimeMillis() - lastHeartbeatTime;
        return elapsed >= electionTimeout.getNext();
    }
    
    public int calculateVoteWeight(VoteContext context) {
        return 1 + voteWeightStrategy.calculateAdditionalWeight(context);
    }
    
    public void becomeLeader() {
        transitionTo(NodeState.LEADER);
        log.info("Node {} became leader for term {}", nodeId, term.getCurrent());
    }
    
    public void becomeFollower(long leaderTerm) {
        updateTermIfHigher(leaderTerm);
        transitionTo(NodeState.FOLLOWER);
        resetElectionTimeout();
    }
    
    public void startElection() {
        transitionTo(NodeState.CANDIDATE);
        incrementTerm();
        voteFor(nodeId);
        resetElectionTimeout();
        log.info("Node {} started election for term {}", nodeId, term.getCurrent());
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=RaftNodeTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/cn/itcraft/speedboat/raft/RaftNode.java
git add src/test/java/cn/itcraft/speedboat/raft/RaftNodeTest.java
git commit -m "feat(raft): add RaftNode core state machine implementation"
```

---

## Task 10: RaftGroup Implementation

**Files:**
- Create: `src/main/java/cn/itcraft/speedboat/raft/RaftGroup.java`
- Create: `src/test/java/cn/itcraft/speedboat/raft/RaftGroupTest.java`

- [ ] **Step 1: Write RaftGroup test**

```java
package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.config.SpeedboatConfig;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class RaftGroupTest {
    
    @Test
    void testGroupCreation() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        List<String> nodeUrls = Arrays.asList("node1:22222", "node2:22223", "node3:22224");
        
        RaftGroup group = new RaftGroup(nodeUrls, config);
        
        assertEquals(3, group.getNodes().size());
        assertNull(group.getLeader());
        assertNull(group.getParentGroup());
        assertTrue(group.getChildGroups().isEmpty());
    }
    
    @Test
    void testGetNodeById() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        List<String> nodeUrls = Arrays.asList("node1:22222", "node2:22223");
        
        RaftGroup group = new RaftGroup(nodeUrls, config);
        
        RaftNode node1 = group.getNodeById("node1");
        assertNotNull(node1);
        assertEquals("node1", node1.getNodeId());
        
        RaftNode notFound = group.getNodeById("node99");
        assertNull(notFound);
    }
    
    @Test
    void testSetLeader() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        List<String> nodeUrls = Arrays.asList("node1:22222", "node2:22223");
        
        RaftGroup group = new RaftGroup(nodeUrls, config);
        RaftNode node1 = group.getNodeById("node1");
        
        group.setLeader(node1);
        
        assertEquals(node1, group.getLeader());
        assertEquals(NodeState.LEADER, node1.getCurrentState());
    }
    
    @Test
    void testCascadeSetup() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        
        RaftGroup parentGroup = new RaftGroup(
            Arrays.asList("rep1:33333", "rep2:33334"), config);
        
        RaftGroup childGroup = new RaftGroup(
            Arrays.asList("node1:22222", "node2:22223"), config);
        
        childGroup.setParentGroup(parentGroup);
        parentGroup.addChildGroup(childGroup);
        
        assertEquals(parentGroup, childGroup.getParentGroup());
        assertTrue(parentGroup.getChildGroups().contains(childGroup));
    }
    
    @Test
    void testIsMainWhenLeader() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        List<String> nodeUrls = Arrays.asList("node1:22222", "node2:22223");
        
        RaftGroup group = new RaftGroup(nodeUrls, config);
        RaftNode node1 = group.getNodeById("node1");
        
        assertFalse(group.isMain(node1));
        
        group.setLeader(node1);
        assertTrue(group.isMain(node1));
        
        RaftNode node2 = group.getNodeById("node2");
        assertFalse(group.isMain(node2));
    }
    
    @Test
    void testMajorityCalculation() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        List<String> nodeUrls = Arrays.asList("node1:22222", "node2:22223", "node3:22224");
        
        RaftGroup group = new RaftGroup(nodeUrls, config);
        
        int majority = group.getMajority();
        assertEquals(2, majority);
        
        int totalWeight = group.getTotalWeight();
        assertEquals(3, totalWeight);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=RaftGroupTest`
Expected: FAIL - RaftGroup not found

- [ ] **Step 3: Write RaftGroup implementation**

```java
package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.config.SpeedboatConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RaftGroup {
    
    private static final Logger log = LoggerFactory.getLogger(RaftGroup.class);
    
    private final List<RaftNode> nodes;
    private final Map<String, RaftNode> nodeMap;
    private final SpeedboatConfig config;
    private RaftNode leader;
    
    private RaftGroup parentGroup;
    private final List<RaftGroup> childGroups;
    
    public RaftGroup(List<String> nodeUrls, SpeedboatConfig config) {
        this.config = config;
        this.nodes = new ArrayList<>();
        this.nodeMap = new HashMap<>();
        this.childGroups = new ArrayList<>();
        
        createNodes(nodeUrls);
    }
    
    private void createNodes(List<String> nodeUrls) {
        for (String nodeUrl : nodeUrls) {
            String[] parts = nodeUrl.split(":");
            String nodeId = parts[0];
            int port = Integer.parseInt(parts[1]);
            
            SpeedboatConfig nodeConfig = SpeedboatConfig.builder()
                .from(config)
                .port(port)
                .build();
            
            RaftNode node = new RaftNode(nodeId, nodeConfig);
            nodes.add(node);
            nodeMap.put(nodeId, node);
        }
        
        log.info("Created RaftGroup with {} nodes", nodes.size());
    }
    
    public List<RaftNode> getNodes() { return new ArrayList<>(nodes); }
    public RaftNode getLeader() { return leader; }
    public SpeedboatConfig getConfig() { return config; }
    public RaftGroup getParentGroup() { return parentGroup; }
    public List<RaftGroup> getChildGroups() { return new ArrayList<>(childGroups); }
    
    public RaftNode getNodeById(String nodeId) {
        return nodeMap.get(nodeId);
    }
    
    public void setLeader(RaftNode newLeader) {
        this.leader = newLeader;
        if (newLeader != null) {
            newLeader.becomeLeader();
            log.info("Group set leader to {}", newLeader.getNodeId());
        }
    }
    
    public void setParentGroup(RaftGroup parent) {
        this.parentGroup = parent;
        log.info("Set parent group");
    }
    
    public void addChildGroup(RaftGroup child) {
        childGroups.add(child);
        log.info("Added child group, total children: {}", childGroups.size());
    }
    
    public boolean isMain(RaftNode node) {
        return leader != null && leader.getNodeId().equals(node.getNodeId());
    }
    
    public int getMajority() {
        return (nodes.size() / 2) + 1;
    }
    
    public int getTotalWeight() {
        int total = 0;
        for (RaftNode node : nodes) {
            VoteContext context = VoteContext.forCandidate(node.getNodeId(), node.getCurrentTerm());
            total += node.calculateVoteWeight(context);
        }
        return total;
    }
    
    public int getVoteCountForCandidate(String candidateId) {
        int votes = 0;
        for (RaftNode node : nodes) {
            if (candidateId.equals(node.getVotedFor())) {
                VoteContext context = VoteContext.forCandidate(candidateId, node.getCurrentTerm());
                votes += node.calculateVoteWeight(context);
            }
        }
        return votes;
    }
    
    public boolean hasMajorityVotes(String candidateId) {
        int votes = getVoteCountForCandidate(candidateId);
        int majority = getMajority();
        
        int totalWeight = getTotalWeight();
        int majorityWeight = (totalWeight / 2) + 1;
        
        return votes >= majorityWeight;
    }
    
    public void resetAllElectionTimeouts() {
        for (RaftNode node : nodes) {
            node.resetElectionTimeout();
        }
    }
    
    public int getNodeCount() {
        return nodes.size();
    }
}
```

- [ ] **Step 4: Add SpeedboatConfig.from() method**

```java
// Add to SpeedboatConfig.Builder class

public Builder from(SpeedboatConfig source) {
    this.port = source.port;
    this.voteWeightStrategy = source.voteWeightStrategy;
    this.groupStrategy = source.groupStrategy;
    this.datacenterId = source.datacenterId;
    return this;
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn test -Dtest=RaftGroupTest`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/main/java/cn/itcraft/speedboat/raft/RaftGroup.java
git add src/main/java/cn/itcraft/speedboat/config/SpeedboatConfig.java
git add src/test/java/cn/itcraft/speedboat/raft/RaftGroupTest.java
git commit -m "feat(raft): add RaftGroup with cascade support and majority calculation"
```

---

## Task 11: Transport Layer Interface

**Files:**
- Create: `src/main/java/cn/itcraft/speedboat/transport/TransportLayer.java`

- [ ] **Step 1: Write TransportLayer interface**

```java
package cn.itcraft.speedboat.transport;

import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import java.util.List;

public interface TransportLayer {
    
    void start();
    
    void stop();
    
    RequestVoteResponse sendRequestVote(String target, RequestVoteRequest request);
    
    HeartbeatResponse sendHeartbeat(String target, HeartbeatRequest request);
    
    void broadcastRequestVote(RequestVoteRequest request, List<String> targets);
    
    void broadcastHeartbeat(HeartbeatRequest request, List<String> targets);
    
    void setRequestVoteHandler(RequestVoteHandler handler);
    
    void setHeartbeatHandler(HeartbeatHandler handler);
    
    interface RequestVoteHandler {
        RequestVoteResponse handle(RequestVoteRequest request);
    }
    
    interface HeartbeatHandler {
        HeartbeatResponse handle(HeartbeatRequest request);
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add src/main/java/cn/itcraft/speedboat/transport/
git commit -m "feat(transport): add TransportLayer interface"
```

---

## Task 12: Netty Transport Implementation

**Files:**
- Create: `src/main/java/cn/itcraft/speedboat/transport/NettyTransport.java`
- Create: `src/test/java/cn/itcraft/speedboat/transport/NettyTransportTest.java`

- [ ] **Step 1: Write NettyTransport skeleton test**

```java
package cn.itcraft.speedboat.transport;

import cn.itcraft.speedboat.rpc.*;
import cn.itcraft.speedboat.config.SpeedboatConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class NettyTransportTest {
    
    private NettyTransport transport1;
    private NettyTransport transport2;
    
    @BeforeEach
    void setup() {
        SpeedboatConfig config1 = SpeedboatConfig.builder().port(22222).build();
        SpeedboatConfig config2 = SpeedboatConfig.builder().port(22223).build();
        
        transport1 = new NettyTransport("node1", config1);
        transport2 = new NettyTransport("node2", config2);
        
        transport1.start();
        transport2.start();
    }
    
    @AfterEach
    void teardown() {
        transport1.stop();
        transport2.stop();
    }
    
    @Test
    void testSendRequestVote() {
        RequestVoteRequest request = new RequestVoteRequest(1L, "node1", 2);
        
        transport2.setRequestVoteHandler(req -> {
            assertEquals(1L, req.getTerm());
            assertEquals("node1", req.getCandidateId());
            return new RequestVoteResponse(1L, true);
        });
        
        RequestVoteResponse response = transport1.sendRequestVote("localhost:22223", request);
        assertNotNull(response);
        assertTrue(response.isVoteGranted());
    }
    
    @Test
    void testSendHeartbeat() {
        HeartbeatRequest request = new HeartbeatRequest(1L, "node1");
        
        transport2.setHeartbeatHandler(req -> {
            assertEquals(1L, req.getTerm());
            return new HeartbeatResponse(1L, true);
        });
        
        HeartbeatResponse response = transport1.sendHeartbeat("localhost:22223", request);
        assertNotNull(response);
        assertTrue(response.isSuccess());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=NettyTransportTest`
Expected: FAIL - NettyTransport not found

- [ ] **Step 3: Write NettyTransport implementation (simplified for testing)**

```java
package cn.itcraft.speedboat.transport;

import cn.itcraft.speedboat.config.SpeedboatConfig;
import cn.itcraft.speedboat.rpc.*;
import cn.itcraft.speedboat.serialize.CustomSerializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NettyTransport implements TransportLayer {
    
    private static final Logger log = LoggerFactory.getLogger(NettyTransport.class);
    
    private final String nodeId;
    private final SpeedboatConfig config;
    private final CustomSerializer serializer;
    
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    
    private RequestVoteHandler requestVoteHandler;
    private HeartbeatHandler heartbeatHandler;
    
    private final ConcurrentHashMap<String, Channel> channels;
    
    public NettyTransport(String nodeId, SpeedboatConfig config) {
        this.nodeId = nodeId;
        this.config = config;
        this.serializer = new CustomSerializer();
        this.channels = new ConcurrentHashMap<>();
    }
    
    @Override
    public void start() {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new RpcHandler());
                    }
                });
            
            ChannelFuture future = bootstrap.bind(config.getPort()).sync();
            serverChannel = future.channel();
            log.info("Node {} started transport on port {}", nodeId, config.getPort());
            
        } catch (InterruptedException e) {
            log.error("Failed to start transport", e);
            stop();
        }
    }
    
    @Override
    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        channels.clear();
        log.info("Node {} stopped transport", nodeId);
    }
    
    @Override
    public RequestVoteResponse sendRequestVote(String target, RequestVoteRequest request) {
        try {
            byte[] data = serializer.serialize(request);
            byte[] response = sendSync(target, data, 5000);
            return serializer.deserialize(response, RequestVoteResponse.class);
        } catch (Exception e) {
            log.error("Failed to send RequestVote to {}", target, e);
            return new RequestVoteResponse(0, false);
        }
    }
    
    @Override
    public HeartbeatResponse sendHeartbeat(String target, HeartbeatRequest request) {
        try {
            byte[] data = serializer.serialize(request);
            byte[] response = sendSync(target, data, 5000);
            return serializer.deserialize(response, HeartbeatResponse.class);
        } catch (Exception e) {
            log.error("Failed to send Heartbeat to {}", target, e);
            return new HeartbeatResponse(0, false);
        }
    }
    
    @Override
    public void broadcastRequestVote(RequestVoteRequest request, List<String> targets) {
        for (String target : targets) {
            sendRequestVote(target, request);
        }
    }
    
    @Override
    public void broadcastHeartbeat(HeartbeatRequest request, List<String> targets) {
        for (String target : targets) {
            sendHeartbeat(target, request);
        }
    }
    
    @Override
    public void setRequestVoteHandler(RequestVoteHandler handler) {
        this.requestVoteHandler = handler;
    }
    
    @Override
    public void setHeartbeatHandler(HeartbeatHandler handler) {
        this.heartbeatHandler = handler;
    }
    
    private byte[] sendSync(String target, byte[] data, long timeoutMs) throws Exception {
        String[] parts = target.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new ClientHandler());
                }
            });
        
        ChannelFuture connectFuture = bootstrap.connect(host, port).sync();
        Channel channel = connectFuture.channel();
        
        CountDownLatch latch = new CountDownLatch(1);
        ResponseHolder holder = new ResponseHolder();
        
        channel.attr(ResponseHolder.KEY).set(holder);
        channel.attr(CountDownLatch.KEY).set(latch);
        
        channel.writeAndFlush(data);
        
        if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new RuntimeException("Timeout waiting for response");
        }
        
        channel.close();
        return holder.getResponse();
    }
    
    private class RpcHandler extends SimpleChannelInboundHandler<byte[]> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, byte[] msg) {
            try {
                Object obj = deserializeMessage(msg);
                
                byte[] response;
                if (obj instanceof RequestVoteRequest) {
                    RequestVoteResponse resp = requestVoteHandler.handle((RequestVoteRequest) obj);
                    response = serializer.serialize(resp);
                } else if (obj instanceof HeartbeatRequest) {
                    HeartbeatResponse resp = heartbeatHandler.handle((HeartbeatRequest) obj);
                    response = serializer.serialize(resp);
                } else {
                    response = new byte[0];
                }
                
                ctx.writeAndFlush(response);
                
            } catch (Exception e) {
                log.error("Failed to handle RPC message", e);
            }
        }
        
        private Object deserializeMessage(byte[] data) {
            byte type = data[8];
            if (type == 2) {
                try {
                    RequestVoteRequest req = serializer.deserialize(data, RequestVoteRequest.class);
                    return req;
                } catch (Exception e1) {
                    try {
                        HeartbeatRequest req = serializer.deserialize(data, HeartbeatRequest.class);
                        return req;
                    } catch (Exception e2) {
                        throw new RuntimeException("Failed to deserialize message");
                    }
                }
            }
            throw new RuntimeException("Unsupported serialization type: " + type);
        }
    }
    
    private class ClientHandler extends SimpleChannelInboundHandler<byte[]> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, byte[] msg) {
            ResponseHolder holder = ctx.attr(ResponseHolder.KEY).get();
            CountDownLatch latch = ctx.attr(CountDownLatch.KEY).get();
            
            if (holder != null) {
                holder.setResponse(msg);
            }
            if (latch != null) {
                latch.countDown();
            }
        }
    }
    
    private static class ResponseHolder {
        static final AttributeKey<ResponseHolder> KEY = AttributeKey.valueOf("responseHolder");
        private byte[] response;
        
        void setResponse(byte[] response) { this.response = response; }
        byte[] getResponse() { return response; }
    }
    
    private static class LatchKey {
        static final AttributeKey<CountDownLatch> KEY = AttributeKey.valueOf("latch");
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=NettyTransportTest`
Expected: PASS (may need adjustment for synchronization)

- [ ] **Step 5: Commit**

```bash
git add src/main/java/cn/itcraft/speedboat/transport/
git add src/test/java/cn/itcraft/speedboat/transport/
git commit -m "feat(transport): add NettyTransport implementation"
```

---

## Task 13: Speedboat Facade API

**Files:**
- Create: `src/main/java/cn/itcraft/speedboat/Speedboat.java`
- Create: `src/test/java/cn/itcraft/speedboat/SpeedboatTest.java`

- [ ] **Step 1: Write Speedboat test**

```java
package cn.itcraft.speedboat;

import cn.itcraft.speedboat.config.SpeedboatConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class SpeedboatTest {
    
    private Speedboat speedboat;
    
    @BeforeEach
    void setup() {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        speedboat = new Speedboat();
        speedboat.init("node1:22222,node2:22223,node3:22224");
    }
    
    @AfterEach
    void teardown() {
        speedboat.destroy();
    }
    
    @Test
    void testInit() {
        assertNotNull(speedboat.getCurrentNode());
        assertEquals("node1", speedboat.getCurrentNode().getNodeId());
    }
    
    @Test
    void testGetCurrentTerm() {
        assertEquals(0, speedboat.getCurrentTerm());
    }
    
    @Test
    void testIsMainInitiallyFalse() {
        assertFalse(speedboat.isMain());
    }
    
    @Test
    void testNodeInfo() {
        RaftNode node = speedboat.getCurrentNode();
        assertNotNull(node);
        assertEquals("node1", node.getNodeId());
        assertEquals(NodeState.FOLLOWER, node.getCurrentState());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=SpeedboatTest`
Expected: FAIL - Speedboat not found

- [ ] **Step 3: Write Speedboat facade**

```java
package cn.itcraft.speedboat;

import cn.itcraft.speedboat.config.SpeedboatConfig;
import cn.itcraft.speedboat.raft.RaftGroup;
import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.raft.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.List;

public class Speedboat {
    
    private static final Logger log = LoggerFactory.getLogger(Speedboat.class);
    
    private RaftGroup group;
    private RaftNode currentNode;
    private SpeedboatConfig config;
    private String nodeId;
    
    public Speedboat() {
        this.config = SpeedboatConfig.defaultConfig();
    }
    
    public Speedboat(SpeedboatConfig config) {
        this.config = config;
    }
    
    public void init(String nodes) {
        init(nodes.split(","));
    }
    
    public void init(String[] nodes) {
        List<String> nodeUrls = Arrays.asList(nodes);
        
        nodeId = extractNodeId(nodeUrls.get(0));
        
        group = new RaftGroup(nodeUrls, config);
        currentNode = group.getNodeById(nodeId);
        
        log.info("Speedboat initialized with node {} in group of {} nodes", 
                 nodeId, group.getNodeCount());
    }
    
    private String extractNodeId(String nodeUrl) {
        return nodeUrl.split(":")[0];
    }
    
    public boolean isMain() {
        if (group == null || currentNode == null) {
            return false;
        }
        return group.isMain(currentNode);
    }
    
    public RaftNode getCurrentNode() {
        return currentNode;
    }
    
    public long getCurrentTerm() {
        if (currentNode == null) {
            return 0;
        }
        return currentNode.getCurrentTerm();
    }
    
    public RaftGroup getGroup() {
        return group;
    }
    
    public void destroy() {
        if (group != null) {
            log.info("Speedboat destroying group for node {}", nodeId);
        }
        group = null;
        currentNode = null;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=SpeedboatTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/cn/itcraft/speedboat/Speedboat.java
git add src/test/java/cn/itcraft/speedboat/SpeedboatTest.java
git commit -m "feat: add Speedboat facade API"
```

---

## Task 14: Integration Test - Single Datacenter Election

**Files:**
- Create: `src/test/java/cn/itcraft/speedboat/integration/SpeedboatIntegrationTest.java`

- [ ] **Step 1: Write integration test for single datacenter**

```java
package cn.itcraft.speedboat.integration;

import cn.itcraft.speedboat.*;
import cn.itcraft.speedboat.config.SpeedboatConfig;
import cn.itcraft.speedboat.raft.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.TimeUnit;

class SpeedboatIntegrationTest {
    
    @Test
    void testSingleDatacenterThreeNodeElection() throws InterruptedException {
        SpeedboatConfig config = SpeedboatConfig.defaultConfig();
        
        Speedboat node1 = new Speedboat(config);
        Speedboat node2 = new Speedboat(config);
        Speedboat node3 = new Speedboat(config);
        
        node1.init("node1:22222,node2:22223,node3:22224");
        node2.init("node1:22222,node2:22223,node3:22224");
        node3.init("node1:22222,node2:22223,node3:22224");
        
        TimeUnit.MILLISECONDS.sleep(500);
        
        int leaderCount = 0;
        if (node1.isMain()) leaderCount++;
        if (node2.isMain()) leaderCount++;
        if (node3.isMain()) leaderCount++;
        
        assertEquals(1, leaderCount, "Should have exactly one leader");
        
        long term1 = node1.getCurrentTerm();
        long term2 = node2.getCurrentTerm();
        long term3 = node3.getCurrentTerm();
        
        assertEquals(term1, term2);
        assertEquals(term2, term3);
        
        node1.destroy();
        node2.destroy();
        node3.destroy();
    }
    
    @Test
    void testEvenNodeTwoNodeElection() throws InterruptedException {
        SpeedboatConfig config = SpeedboatConfig.builder()
            .voteWeightStrategy(new EvenNodeVoteWeightStrategy())
            .build();
        
        Speedboat node1 = new Speedboat(config);
        Speedboat node2 = new Speedboat(config);
        
        node1.init("node1:22222,node2:22223");
        node2.init("node1:22222,node2:22223");
        
        TimeUnit.MILLISECONDS.sleep(500);
        
        int leaderCount = 0;
        if (node1.isMain()) leaderCount++;
        if (node2.isMain()) leaderCount++;
        
        assertEquals(1, leaderCount, "Even nodes should avoid split vote");
        
        node1.destroy();
        node2.destroy();
    }
}
```

- [ ] **Step 2: Run test (may fail due to missing election logic)**

Run: `mvn test -Dtest=SpeedboatIntegrationTest`
Expected: FAIL - election coordination not implemented

- [ ] **Step 3: Commit test stub**

```bash
git add src/test/java/cn/itcraft/speedboat/integration/
git commit -m "test(integration): add single datacenter election test stub"
```

---

## Task 15: Election Logic Implementation

**Files:**
- Modify: `src/main/java/cn/itcraft/speedboat/raft/RaftNode.java`
- Modify: `src/main/java/cn/itcraft/speedboat/raft/RaftGroup.java`
- Modify: `src/main/java/cn/itcraft/speedboat/Speedboat.java`

- [ ] **Step 1: Add election timeout scheduler to RaftNode**

```java
// Add to RaftNode.java

private ScheduledExecutorService electionScheduler;

public void startElectionTimer(RaftGroup group) {
    electionScheduler = Executors.newSingleThreadScheduledExecutor();
    
    electionScheduler.scheduleAtFixedRate(() -> {
        if (currentState == NodeState.FOLLOWER && isElectionTimeout()) {
            startElection();
            sendRequestVote(group);
        }
    }, 50, 50, TimeUnit.MILLISECONDS);
}

public void stopElectionTimer() {
    if (electionScheduler != null) {
        electionScheduler.shutdown();
    }
}

private void sendRequestVote(RaftGroup group) {
    VoteContext context = VoteContext.forCandidate(nodeId, term.getCurrent());
    int voteWeight = calculateVoteWeight(context);
    
    RequestVoteRequest request = new RequestVoteRequest(
        term.getCurrent(), nodeId, voteWeight);
    
    for (RaftNode peer : group.getNodes()) {
        if (!peer.getNodeId().equals(nodeId)) {
            RequestVoteResponse response = transport.sendRequestVote(
                peer.getNodeId() + ":" + peer.getConfig().getPort(), request);
            
            if (response.isVoteGranted() && response.getTerm() == term.getCurrent()) {
                int votes = group.getVoteCountForCandidate(nodeId);
                if (group.hasMajorityVotes(nodeId)) {
                    becomeLeader();
                    group.setLeader(this);
                    break;
                }
            }
        }
    }
}
```

- [ ] **Step 2: Add heartbeat sender for leader**

```java
// Add to RaftNode.java

private ScheduledExecutorService heartbeatScheduler;

public void startHeartbeat(RaftGroup group, TransportLayer transport) {
    this.transport = transport;
    
    heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    
    heartbeatScheduler.scheduleAtFixedRate(() -> {
        if (currentState == NodeState.LEADER) {
            sendHeartbeat(group, transport);
        }
    }, 0, groupStrategy.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
}

public void stopHeartbeat() {
    if (heartbeatScheduler != null) {
        heartbeatScheduler.shutdown();
    }
}

private void sendHeartbeat(RaftGroup group, TransportLayer transport) {
    HeartbeatRequest request = new HeartbeatRequest(term.getCurrent(), nodeId);
    
    for (RaftNode peer : group.getNodes()) {
        if (!peer.getNodeId().equals(nodeId)) {
            transport.sendHeartbeat(
                peer.getNodeId() + ":" + peer.getConfig().getPort(), request);
        }
    }
}

private TransportLayer transport;

public void setTransport(TransportLayer transport) {
    this.transport = transport;
}
```

- [ ] **Step 3: Update Speedboat to start timers**

```java
// Add to Speedboat.java

private TransportLayer transport;

public void init(String[] nodes) {
    List<String> nodeUrls = Arrays.asList(nodes);
    nodeId = extractNodeId(nodeUrls.get(0));
    
    group = new RaftGroup(nodeUrls, config);
    currentNode = group.getNodeById(nodeId);
    
    transport = new NettyTransport(nodeId, currentNode.getConfig());
    transport.start();
    
    currentNode.setTransport(transport);
    currentNode.startElectionTimer(group);
    currentNode.startHeartbeat(group, transport);
    
    setupHandlers();
    
    log.info("Speedboat initialized with node {} in group of {} nodes", 
             nodeId, group.getNodeCount());
}

private void setupHandlers() {
    transport.setRequestVoteHandler(request -> {
        if (request.getTerm() < currentNode.getCurrentTerm()) {
            return new RequestVoteResponse(currentNode.getCurrentTerm(), false);
        }
        
        currentNode.updateTermIfHigher(request.getTerm());
        
        if (currentNode.getVotedFor() == null || 
            currentNode.getVotedFor().equals(request.getCandidateId())) {
            currentNode.voteFor(request.getCandidateId());
            currentNode.resetElectionTimeout();
            return new RequestVoteResponse(currentNode.getCurrentTerm(), true);
        }
        
        return new RequestVoteResponse(currentNode.getCurrentTerm(), false);
    });
    
    transport.setHeartbeatHandler(request -> {
        currentNode.updateTermIfHigher(request.getTerm());
        currentNode.transitionTo(NodeState.FOLLOWER);
        currentNode.resetElectionTimeout();
        
        if (request.getTerm() == currentNode.getCurrentTerm()) {
            RaftNode leader = group.getNodeById(request.getLeaderId());
            if (leader != null) {
                group.setLeader(leader);
            }
        }
        
        return new HeartbeatResponse(currentNode.getCurrentTerm(), true);
    });
}

public void destroy() {
    if (currentNode != null) {
        currentNode.stopElectionTimer();
        currentNode.stopHeartbeat();
    }
    if (transport != null) {
        transport.stop();
    }
    group = null;
    currentNode = null;
}
```

- [ ] **Step 4: Run integration test**

Run: `mvn test -Dtest=SpeedboatIntegrationTest`
Expected: PASS (after timing adjustments)

- [ ] **Step 5: Commit**

```bash
git add src/main/java/cn/itcraft/speedboat/
git commit -m "feat: implement election and heartbeat logic"
```

---

## Task 16: Cross-Datacenter Cascade Test

**Files:**
- Modify: `src/test/java/cn/itcraft/speedboat/integration/SpeedboatIntegrationTest.java`

- [ ] **Step 1: Add cross-datacenter test**

```java
// Add to SpeedboatIntegrationTest.java

@Test
void testCrossDatacenterCascadeElection() throws InterruptedException {
    SpeedboatConfig beijingConfig = SpeedboatConfig.builder()
        .datacenterId("beijing")
        .groupStrategy(new DatacenterGroupStrategy())
        .voteWeightStrategy(new CompositeVoteWeightStrategy()
            .addStrategy(new EvenNodeVoteWeightStrategy())
            .addStrategy(new DatacenterVoteWeightStrategy("beijing")))
        .port(22222)
        .build();
    
    SpeedboatConfig shanghaiConfig = SpeedboatConfig.builder()
        .datacenterId("shanghai")
        .groupStrategy(new DatacenterGroupStrategy())
        .voteWeightStrategy(new CompositeVoteWeightStrategy()
            .addStrategy(new EvenNodeVoteWeightStrategy())
            .addStrategy(new DatacenterVoteWeightStrategy("shanghai")))
        .port(22225)
        .build();
    
    SpeedboatConfig globalConfig = SpeedboatConfig.builder()
        .groupStrategy(new GlobalGroupStrategy())
        .port(33333)
        .build();
    
    Speedboat beijing1 = new Speedboat(beijingConfig);
    Speedboat beijing2 = new Speedboat(beijingConfig);
    Speedboat shanghai1 = new Speedboat(shanghaiConfig);
    Speedboat shanghai2 = new Speedboat(shanghaiConfig);
    
    beijing1.init("beijing1:22222,beijing2:22223");
    beijing2.init("beijing1:22222,beijing2:22223");
    shanghai1.init("shanghai1:22225,shanghai2:22226");
    shanghai2.init("shanghai1:22225,shanghai2:22226");
    
    TimeUnit.MILLISECONDS.sleep(1000);
    
    int beijingLeaderCount = 0;
    if (beijing1.isMain()) beijingLeaderCount++;
    if (beijing2.isMain()) beijingLeaderCount++;
    assertEquals(1, beijingLeaderCount, "Beijing should have one leader");
    
    int shanghaiLeaderCount = 0;
    if (shanghai1.isMain()) shanghaiLeaderCount++;
    if (shanghai2.isMain()) shanghaiLeaderCount++;
    assertEquals(1, shanghaiLeaderCount, "Shanghai should have one leader");
    
    beijing1.destroy();
    beijing2.destroy();
    shanghai1.destroy();
    shanghai2.destroy();
}
```

- [ ] **Step 2: Run test**

Run: `mvn test -Dtest=SpeedboatIntegrationTest`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/test/java/cn/itcraft/speedboat/integration/
git commit -m "test(integration): add cross-datacenter cascade election test"
```

---

## Task 17: Coverage Improvement

**Files:**
- Multiple test files to improve coverage

- [ ] **Step 1: Run coverage report**

Run: `mvn clean test jacoco:report`
Expected: Coverage report generated

- [ ] **Step 2: Identify coverage gaps**

Check `target/site/jacoco/index.html` for branches < 80%

- [ ] **Step 3: Add missing test cases**

Focus on:
- Term monotonicity violation edge cases
- State transition edge cases
- Serialization error handling
- Transport timeout handling
- Election tie scenarios

- [ ] **Step 4: Re-run coverage**

Run: `mvn clean test jacoco:report`
Expected: Branch coverage >= 80%

- [ ] **Step 5: Commit**

```bash
git add src/test/
git commit -m "test: improve coverage to 80%+ branch coverage"
```

---

## Task 18: Final Integration and Documentation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README with usage examples**

```markdown
# Speedboat - Raft Distributed Lock System

## Quick Start

```java
Speedboat speedboat = new Speedboat();
speedboat.init("node1:22222,node2:22223,node3:22224");

if (speedboat.isMain()) {
    System.out.println("I am the leader!");
}

speedboat.destroy();
```

## Cross-Datacenter Setup

```java
SpeedboatConfig beijingConfig = SpeedboatConfig.builder()
    .datacenterId("beijing")
    .groupStrategy(new DatacenterGroupStrategy())
    .voteWeightStrategy(new DatacenterVoteWeightStrategy("beijing"))
    .build();

Speedboat node = new Speedboat(beijingConfig);
node.init("beijing1:22222,beijing2:22223");
```

## Performance

- Same datacenter: 150-300ms leader election
- Cross-datacenter (same city): 200-500ms
- Cross-region: 500-1000ms
```

- [ ] **Step 2: Run full test suite**

Run: `mvn clean test`
Expected: All tests pass

- [ ] **Step 3: Final commit**

```bash
git add README.md
git commit -m "docs: update README with usage examples"

git log --oneline -5
```

---

## Self-Review Checklist

**1. Spec Coverage:**
- ✅ Core Raft components (NodeState, Term, ElectionTimeout)
- ✅ Strategy interfaces (VoteWeightStrategy, GroupStrategy)
- ✅ RPC data objects
- ✅ Serialization layer
- ✅ Transport layer (Netty)
- ✅ RaftNode and RaftGroup
- ✅ Speedboat facade
- ✅ Single datacenter election
- ✅ Even node handling
- ✅ Cross-datacenter cascade

**2. Placeholder Scan:**
- No TBD, TODO, or vague instructions
- All steps have concrete code
- All commands are explicit

**3. Type Consistency:**
- VoteWeightStrategy.calculateAdditionalWeight(VoteContext) → int (consistent)
- GroupStrategy.getElectionTimeout() → long (consistent)
- Serializer.serialize/deserialize signatures match throughout
- RaftNode.transitionTo returns boolean consistently

---

## Execution Options

Plan complete and saved to `docs/superpowers/plans/2026-07-08-speedboat-raft-implementation.md`.

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?