package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.config.SpeedboatConsts;
import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import cn.itcraft.speedboat.transport.NettyTransport;
import cn.itcraft.speedboat.transport.TransportLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import cn.itcraft.speedboat.statemachine.StateMachine;

/**
 * Raft 共识算法节点核心实现。
 * 
 * <p>基于 Raft 论文 "In Search of an Understandable Consensus Algorithm" 实现，
 * 支持 Leader 选举、日志复制和成员变更等核心功能。</p>
 * 
 * <p>节点状态机：</p>
 * <ul>
 *   <li><b>FOLLOWER</b>：跟随者，响应 Leader 的心跳和日志复制请求</li>
 *   <li><b>CANDIDATE</b>：候选者，发起选举请求投票</li>
 *   <li><b>LEADER</b>：领导者，处理客户端请求、复制日志、发送心跳</li>
 * </ul>
 * 
 * <p>核心算法：</p>
 * <ol>
 *   <li><b>选举</b>：Follower 超时未收到心跳 → Candidate → 请求投票 → 获得多数票 → Leader</li>
 *   <li><b>日志复制</b>：Leader 接收客户端命令 → 追加到本地日志 → AppendEntries RPC 复制到 Followers</li>
 *   <li><b>安全性</b>：选举安全性（每个任期最多一个 Leader）、日志匹配特性</li>
 *   <li><b>成员变更</b>：支持动态添加/移除节点，通过日志复制实现一致的状态变更</li>
 * </ol>
 * 
 * <p>配置项：</p>
 * <ul>
 *   <li>选举超时：150-300ms（推荐值），防止分裂投票</li>
 *   <li>心跳间隔：50ms（默认），保持 Leader 活跃性</li>
 *   <li>最大日志大小：防止日志无限增长</li>
 * </ul>
 * 
 * <p>使用示例：</p>
 * <pre>{@code
 * // 单机房扁平模式
 * RaftNode node = RaftNode.builder()
 *     .nodeId("node-1")
 *     .peerIds(Arrays.asList("node-2", "node-3"))
 *     .electionTimeout(new ElectionTimeout(150, 300))
 *     .groupStrategy(new DefaultGroupStrategy())
 *     .build();
 * 
 * // 跨机房级联模式（需要配置 GroupStrategy）
 * RaftNode node = RaftNode.builder()
 *     .nodeId("node-1")
 *     .peerIds(Arrays.asList("node-2", "node-3"))
 *     .electionTimeout(new ElectionTimeout(1000, 2000))
 *     .groupStrategy(new DatacenterGroupStrategy())
 *     .build();
 * 
 * node.start();
 * 
 * if (node.isLeader()) {
 *     node.propose("command".getBytes());
 * }
 * }</pre>
 * 
 * @author speedboat
 * @see ElectionTimeout
 * @see LogEntry
 * @see Term
 * @see RaftGroup
 * @since 1.0.0
 */
public class RaftNode {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private volatile NodeState currentState;
    private final Term term;
    private volatile String votedFor;
    private volatile long votedForTerm;
    private volatile String leaderId;

    private final ElectionTimeout electionTimeout;
    private volatile long lastHeartbeatNanos;

    private final VoteWeightStrategy voteWeightStrategy;
    private final GroupStrategy groupStrategy;
    private TransportLayer transportLayer;

    private final List<String> peerIds;
    private final Map<String, Boolean> votesReceived;

    private volatile boolean running;
    private ScheduledExecutorService scheduler;
    private Future<?> electionTimeoutFuture;
    private Future<?> heartbeatFuture;
    private Future<?> membershipChangeFuture;

    private final List<LogEntry> log;
    private volatile long commitIndex;
    private volatile long lastApplied;
    private final Map<String, Long> nextIndex;
    private final Map<String, Long> matchIndex;
    private final int maxLogSize;
    // 成员变更相关字段
    private final cn.itcraft.speedboat.config.MembershipConfig membershipConfig;
    private final cn.itcraft.speedboat.strategy.membership.HealthCheckStrategy healthCheckStrategy;
    private final cn.itcraft.speedboat.strategy.membership.RegistryStrategy registryStrategy;
    private final cn.itcraft.speedboat.strategy.membership.ChangeValidationStrategy changeValidationStrategy;
    private final java.util.concurrent.ConcurrentHashMap<String, FailureRecord> failureRecords;
    private StateMachine stateMachine;

    private RaftNode(Builder builder) {
        this.nodeId = builder.nodeId;
        this.currentState = NodeState.FOLLOWER;
        this.term = new Term();
        this.votedFor = null;
        this.votedForTerm = -1;
        this.leaderId = null;
        this.electionTimeout = builder.electionTimeout;
        this.voteWeightStrategy = builder.voteWeightStrategy;
        this.groupStrategy = builder.groupStrategy;
        this.transportLayer = builder.transportLayer;
        this.peerIds = builder.peerIds != null ? new ArrayList<>(builder.peerIds) : new ArrayList<>();
        this.votesReceived = new ConcurrentHashMap<>();
        this.lastHeartbeatNanos = System.nanoTime();
        this.log = new CopyOnWriteArrayList<>();
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        this.maxLogSize = builder.maxLogSize > 0 ? builder.maxLogSize : SpeedboatConsts.DEFAULT_MAX_LOG_SIZE;
        
        // 成员变更初始化
        this.membershipConfig = builder.membershipConfig != null ? builder.membershipConfig : new cn.itcraft.speedboat.config.MembershipConfig();
        this.healthCheckStrategy = builder.healthCheckStrategy != null ? builder.healthCheckStrategy : 
            new cn.itcraft.speedboat.strategy.membership.impl.PassiveHealthCheckStrategy();
        this.registryStrategy = builder.registryStrategy != null ? builder.registryStrategy : 
            new cn.itcraft.speedboat.strategy.membership.impl.NoOpRegistryStrategy();
        this.changeValidationStrategy = builder.changeValidationStrategy != null ? builder.changeValidationStrategy : 
            new cn.itcraft.speedboat.strategy.membership.impl.DefaultChangeValidationStrategy();
        this.failureRecords = new java.util.concurrent.ConcurrentHashMap<>();
        this.stateMachine = builder.stateMachine;
        
        // 初始化成员变更检测任务（如果需要）
        if (membershipConfig.isMembershipChangeEnabled()) {
            startMembershipChangeDetection();
        }
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        scheduler = Executors.newScheduledThreadPool(2);
        
        if (transportLayer != null) {
            transportLayer.setRequestVoteHandler(this::handleRequestVote);
            transportLayer.setHeartbeatHandler(this::handleHeartbeat);
            transportLayer.setAppendEntriesHandler(this::handleAppendEntries);
        }
        
        resetElectionTimeout();
        logger.info("Node {} started as {}", nodeId, currentState);
    }

    public void shutdown() {
        if (!running) {
            return;
        }
        running = false;
        
        if (electionTimeoutFuture != null) {
            electionTimeoutFuture.cancel(false);
        }
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(false);
        }
        if (membershipChangeFuture != null) {
            membershipChangeFuture.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(SpeedboatConsts.SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        logger.info("Node {} shutdown", nodeId);
    }

    public synchronized void transitionTo(NodeState newState) {
        if (!currentState.canTransitionTo(newState)) {
            throw new IllegalStateException(
                String.format("Invalid state transition from %s to %s on node %s", 
                    currentState, newState, nodeId));
        }
        
        NodeState oldState = currentState;
        currentState = newState;
        
        if (oldState == NodeState.LEADER && newState == NodeState.FOLLOWER) {
            cancelHeartbeat();
        }
        
        if (newState == NodeState.CANDIDATE) {
            votesReceived.clear();
            votedFor = nodeId;
            term.increment();
        } else if (newState == NodeState.LEADER) {
            startHeartbeat();
        } else if (newState == NodeState.FOLLOWER) {
            cancelHeartbeat();
            resetElectionTimeout();
        }
        
        logger.info("Node {} transitioned from {} to {}", nodeId, oldState, newState);
    }

    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        if (term.isMonotonicViolation(request.getTerm())) {
            logger.info("Rejecting vote request from {} with lower term {}", 
                request.getCandidateId(), request.getTerm());
            return new RequestVoteResponse(term.getCurrent(), false);
        }

        if (request.getTerm() > term.getCurrent()) {
            term.updateIfHigher(request.getTerm());
            if (currentState != NodeState.FOLLOWER) {
                transitionTo(NodeState.FOLLOWER);
            }
            votedFor = null;
            votedForTerm = -1;
            leaderId = null;
        }

        boolean canVote = (votedFor == null || votedFor.equals(request.getCandidateId())) 
            && votedForTerm != term.getCurrent();
        
        if (canVote && request.getTerm() >= term.getCurrent()) {
            votedFor = request.getCandidateId();
            votedForTerm = term.getCurrent();
            resetElectionTimeout();
            logger.info("Node {} voted for {} in term {}", nodeId, request.getCandidateId(), request.getTerm());
            return new RequestVoteResponse(term.getCurrent(), true);
        }
        
        logger.info("Node {} rejected vote for {} (votedFor: {}, currentTerm: {})", 
            nodeId, request.getCandidateId(), votedFor, term.getCurrent());
        return new RequestVoteResponse(term.getCurrent(), false);
    }

    public synchronized HeartbeatResponse handleHeartbeat(HeartbeatRequest request) {
        AppendEntriesRequest appendRequest = AppendEntriesRequest.heartbeat(
            request.getTerm(), request.getLeaderId(), 0, 0, 0
        );
        AppendEntriesResponse response = handleAppendEntries(appendRequest);
        return new HeartbeatResponse(response.getTerm(), response.isSuccess());
    }

    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        if (request.getTerm() < term.getCurrent()) {
            logger.info("Rejecting AppendEntries from {} with lower term {}", 
                request.getLeaderId(), request.getTerm());
            return new AppendEntriesResponse(term.getCurrent(), false, getLastLogIndex());
        }

        if (request.getTerm() > term.getCurrent()) {
            term.updateIfHigher(request.getTerm());
            votedFor = null;
            votedForTerm = -1;
        }

        if (currentState != NodeState.FOLLOWER) {
            transitionTo(NodeState.FOLLOWER);
        }

        leaderId = request.getLeaderId();
        resetElectionTimeout();
        lastHeartbeatNanos = System.nanoTime();

        logger.info("Follower {} received appendEntries from leader {}, entries={}, prevLogIndex={}, leaderCommit={}", 
            nodeId, request.getLeaderId(), request.getEntries().size(), request.getPrevLogIndex(), request.getLeaderCommit());

        if (request.getPrevLogIndex() > 0) {
            if (getLastLogIndex() < request.getPrevLogIndex()) {
                logger.info("Follower {} log too short: {} < prevLogIndex {}", 
                    nodeId, getLastLogIndex(), request.getPrevLogIndex());
                return new AppendEntriesResponse(term.getCurrent(), false, getLastLogIndex());
            }

            LogEntry prevEntry = getEntryAt(request.getPrevLogIndex());
            if (prevEntry == null || prevEntry.getTerm() != request.getPrevLogTerm()) {
                long conflictIndex = request.getPrevLogIndex() - 1;
                if (prevEntry != null) {
                    for (long i = request.getPrevLogIndex(); i >= 1; i--) {
                        LogEntry e = getEntryAt(i);
                        if (e != null && e.getTerm() == request.getPrevLogTerm()) {
                            conflictIndex = i;
                            break;
                        }
                    }
                }
                logger.info("Follower {} log term mismatch at index {}", nodeId, request.getPrevLogIndex());
                return new AppendEntriesResponse(term.getCurrent(), false, Math.max(0, conflictIndex));
            }
        }

        if (!request.getEntries().isEmpty()) {
            List<LogEntry> newLog = new ArrayList<>();
            for (LogEntry entry : log) {
                if (entry.getIndex() < request.getPrevLogIndex() + 1) {
                    newLog.add(entry);
                }
            }

            for (LogEntry entry : request.getEntries()) {
                newLog.add(entry);
            }

            log.clear();
            log.addAll(newLog);
            truncateLogIfNeeded();

            logger.info("Follower {} appended {} entries, log size now {}", 
                nodeId, request.getEntries().size(), log.size());
        }

        if (request.getLeaderCommit() > commitIndex) {
            long oldCommitIndex = commitIndex;
            commitIndex = Math.min(request.getLeaderCommit(), getLastLogIndex());
            applyCommittedEntries();
            logger.info("Node {} commitIndex updated: {} -> {}, lastApplied={}", 
                nodeId, oldCommitIndex, commitIndex, lastApplied);
        }

        return new AppendEntriesResponse(term.getCurrent(), true, getLastLogIndex());
    }

    public void startElection() {
        if (currentState != NodeState.FOLLOWER && currentState != NodeState.CANDIDATE) {
            return;
        }
        
        transitionTo(NodeState.CANDIDATE);
        votesReceived.put(nodeId, true);
        
        int currentWeight = calculateTotalVoteWeight();
        int requiredWeight = calculateRequiredWeight();
        
        logger.info("Node {} starting election for term {}, current weight: {}, required weight: {}", 
            nodeId, term.getCurrent(), currentWeight, requiredWeight);
        
        if (currentWeight >= requiredWeight) {
            becomeLeader();
            return;
        }
        
        requestVotesFromPeers(currentWeight, requiredWeight);
    }

    private void requestVotesFromPeers(int currentWeight, int requiredWeight) {
        if (transportLayer == null || peerIds.isEmpty()) {
            checkElectionResult(currentWeight, requiredWeight);
            return;
        }
        
        for (String peerId : peerIds) {
            RequestVoteRequest request = new RequestVoteRequest(
                term.getCurrent(), nodeId, calculateTotalVoteWeight()
            );
            
            transportLayer.sendRequestVote(peerId, request)
                .thenAccept(response -> {
                    if (response.isVoteGranted() && currentState == NodeState.CANDIDATE) {
                        synchronized (votesReceived) {
                            votesReceived.put(peerId, true);
                            checkElectionResult(calculateReceivedVoteWeight(), requiredWeight);
                        }
                    } else if (response.getTerm() > term.getCurrent()) {
                        term.updateIfHigher(response.getTerm());
                        transitionTo(NodeState.FOLLOWER);
                    }
                });
        }
        
        scheduler.schedule(() -> {
            if (currentState == NodeState.CANDIDATE) {
                logger.info("Node {} election timeout, starting new election", nodeId);
                startElection();
            }
        }, electionTimeout.getNext(), TimeUnit.MILLISECONDS);
    }

    private synchronized void checkElectionResult(int currentWeight, int requiredWeight) {
        if (currentState != NodeState.CANDIDATE) {
            return;
        }
        
        logger.info("Node {} has weight {}, needs {}", nodeId, currentWeight, requiredWeight);
        
        if (currentWeight >= requiredWeight) {
            becomeLeader();
        }
    }

    public synchronized void becomeLeader() {
        if (currentState != NodeState.CANDIDATE) {
            return;
        }
        
        transitionTo(NodeState.LEADER);
        leaderId = nodeId;
        
        appendLeaderEntry();
        
        // Become leader时，设置matchIndex：自己的matchIndex为logSize，peer的matchIndex为0
        long logSize = getLastLogIndex();
        for (String peerId : peerIds) {
            nextIndex.put(peerId, logSize + 1);
            matchIndex.put(peerId, 0L);
        }
        matchIndex.put(nodeId, logSize);
        
        // Become leader时，立即提交自己提出的entry（Raft协议要求）
        commitIndex = logSize;
        applyCommittedEntries();
        
        sendAppendEntries();
        startHeartbeat();
        
        logger.info("Node {} became leader for term {}, commitIndex set to {}", nodeId, term.getCurrent(), commitIndex);
    }

    private void appendLeaderEntry() {
        long newIndex = getLastLogIndex() + 1;
        LogEntry entry = new LogEntry(newIndex, term.getCurrent(), nodeId);
        log.add(entry);
        truncateLogIfNeeded();
        logger.info("Leader {} appended entry at index {} term {}", nodeId, newIndex, term.getCurrent());
    }

    public void sendHeartbeat() {
        sendAppendEntries();
    }

    public void sendAppendEntries() {
        if (currentState != NodeState.LEADER || transportLayer == null) {
            logger.info("Node {} not leader ({}) or transportLayer null ({}), cannot send append entries", 
                nodeId, currentState == NodeState.LEADER, transportLayer != null);
            return;
        }
        
        logger.info("Leader {} sending append entries to {} peers, logSize={}, commitIndex={}", 
            nodeId, peerIds.size(), log.size(), commitIndex);
        for (String peerId : peerIds) {
            sendAppendEntriesToPeer(peerId);
        }
        
        logger.info("Leader {} sent append entries to {} peers", nodeId, peerIds.size());
    }

    private void sendAppendEntriesToPeer(String peerId) {
        long nextIdx = nextIndex.getOrDefault(peerId, 1L);
        long prevLogIndex = nextIdx - 1;
        long prevLogTerm = 0;
        
        if (prevLogIndex > 0) {
            LogEntry prevEntry = getEntryAt(prevLogIndex);
            if (prevEntry != null) {
                prevLogTerm = prevEntry.getTerm();
            }
        }
        
        List<LogEntry> entriesToSend = new ArrayList<>();
        for (LogEntry entry : log) {
            if (entry.getIndex() >= nextIdx) {
                entriesToSend.add(entry);
            }
        }
        
        logger.info("Leader {} sending to peer {}: nextIdx={}, logSize={}, entriesToSend={}, commitIndex={}", 
            nodeId, peerId, nextIdx, log.size(), entriesToSend.size(), commitIndex);
        
        AppendEntriesRequest request = new AppendEntriesRequest(
            term.getCurrent(), nodeId, prevLogIndex, prevLogTerm, entriesToSend, commitIndex
        );
        
        logger.info("Leader {} calling transportLayer.sendAppendEntries to peer {}, transportLayer={}", nodeId, peerId, transportLayer.getClass().getName());
        transportLayer.sendAppendEntries(peerId, request)
            .thenAccept(response -> handleAppendEntriesResponse(peerId, response));
    }

    private synchronized void handleAppendEntriesResponse(String peerId, AppendEntriesResponse response) {
        logger.info("Leader {} received appendEntries response from {}: success={}, matchIndex={}", 
            nodeId, peerId, response.isSuccess(), response.getMatchIndex());
        
        if (response.getTerm() > term.getCurrent()) {
            term.updateIfHigher(response.getTerm());
            transitionTo(NodeState.FOLLOWER);
            return;
        }
        
        if (currentState != NodeState.LEADER) {
            logger.info("Node {} is not leader anymore, current state={}", nodeId, currentState);
            return;
        }
        
        if (response.isSuccess()) {
            matchIndex.put(peerId, response.getMatchIndex());
            nextIndex.put(peerId, response.getMatchIndex() + 1);
            logger.info("Leader {} matchIndex for {} updated to {}, matchIndex={}", 
                nodeId, peerId, response.getMatchIndex(), matchIndex);
            advanceCommitIndex();
        } else {
            long newNextIndex = Math.max(1, response.getMatchIndex() + 1);
            nextIndex.put(peerId, newNextIndex);
            logger.info("Leader {} decrementing nextIndex for {} to {}", nodeId, peerId, newNextIndex);
        }
    }

    private void advanceCommitIndex() {
        logger.info("Leader {} advanceCommitIndex: lastLogIndex={}, commitIndex={}", 
            nodeId, getLastLogIndex(), commitIndex);
        for (long n = getLastLogIndex(); n > commitIndex; n--) {
            LogEntry entry = getEntryAt(n);
            if (entry != null && entry.getTerm() == term.getCurrent()) {
                int matchCount = 1;
                for (Long idx : matchIndex.values()) {
                    if (idx >= n) {
                        matchCount++;
                    }
                }
                
                int totalWeight = calculateRequiredWeight();
                int currentWeight = calculateTotalVoteWeight();
                int matchedWeight = 1;
                
                for (String peerId : peerIds) {
                    if (matchIndex.containsKey(peerId) && matchIndex.get(peerId) >= n) {
                        if (voteWeightStrategy != null) {
                            VoteContext context = VoteContext.forCandidate(peerId, term.getCurrent());
                            matchedWeight += 1 + voteWeightStrategy.calculateAdditionalWeight(context);
                        } else {
                            matchedWeight += 1;
                        }
                    }
                }
                
                if (matchedWeight >= totalWeight) {
                    commitIndex = n;
                    applyCommittedEntries();
                    logger.info("Leader {} advanced commitIndex to {}, matchedWeight={}, totalWeight={}", 
                        nodeId, commitIndex, matchedWeight, totalWeight);
                    break;
                } else {
                    logger.info("Leader {} cannot advance commitIndex to {}, matchedWeight={}, totalWeight={}, matchIndex={}", 
                        nodeId, n, matchedWeight, totalWeight, matchIndex);
                }
            }
        }
    }

    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = getEntryAt(lastApplied);
            if (entry != null) {
                if (entry.getEntryType() == LogEntry.EntryType.COMMAND) {
                    if (stateMachine != null) {
                        stateMachine.apply(entry);
                    }
                } else if (entry.getEntryType() == LogEntry.EntryType.MEMBER_CHANGE) {
                    processMemberChange((MemberChangeEntry) entry);
                } else {
                    leaderId = entry.getLeaderId();
                }
                logger.info("Node {} applied entry at index {}: type={}", 
                    nodeId, lastApplied, entry.getEntryType());
            }
        }
    }

    private void truncateLogIfNeeded() {
        while (log.size() > maxLogSize && !log.isEmpty()) {
            LogEntry oldest = log.get(0);
            if (oldest.getIndex() <= commitIndex) {
                break;
            }
            log.remove(0);
            logger.info("Truncated log entry at index {}, log size now {}", oldest.getIndex(), log.size());
        }
    }

    public boolean isMain() {
        return currentState == NodeState.LEADER;
    }

    public boolean isLeader() {
        return currentState == NodeState.LEADER;
    }

    public RaftGroup getGroup() {
        // 需要查看 RaftGroup 的创建方式
        // 暂时返回 null，后续 Phase 3 会实现
        return null;
    }

    public void resetElectionTimeout() {
        if (scheduler == null || !running) {
            return;
        }
        
        if (electionTimeoutFuture != null) {
            electionTimeoutFuture.cancel(false);
        }
        
        electionTimeout.reset();
        long timeout = electionTimeout.getNext();
        
        electionTimeoutFuture = scheduler.schedule(() -> {
            if (running && currentState != NodeState.LEADER) {
                long elapsedNanos = System.nanoTime() - lastHeartbeatNanos;
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
                if (elapsedMs >= timeout) {
                    logger.info("Node {} election timeout, starting election", nodeId);
                    startElection();
                } else {
                    resetElectionTimeout();
                }
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    private void startHeartbeat() {
        if (scheduler == null || !running) {
            return;
        }
        
        long heartbeatInterval = groupStrategy != null ? 
            groupStrategy.getHeartbeatInterval() : 50;
        
        heartbeatFuture = scheduler.scheduleAtFixedRate(
            this::sendHeartbeat,
            0,
            heartbeatInterval,
            TimeUnit.MILLISECONDS
        );
    }

    private void cancelHeartbeat() {
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(false);
            heartbeatFuture = null;
        }
    }
    
    /**
     * Calculates total vote weight for a vote request.
     * 
     * @param request the vote request containing base weight
     * @return total vote weight (base + additional from strategy)
     */
    public int calculateVoteWeight(RequestVoteRequest request) {
        int baseWeight = request.getVoteWeight();
        int additionalWeight = 0;
        
        if (voteWeightStrategy != null) {
            VoteContext context = VoteContext.forCandidate(request.getCandidateId(), request.getTerm());
            additionalWeight = voteWeightStrategy.calculateAdditionalWeight(context);
        }
        
        return baseWeight + additionalWeight;
    }

    /**
     * Calculates total vote weight for this node as a candidate.
     * 
     * @return total vote weight (base 1 + additional from strategy)
     */
    private int calculateTotalVoteWeight() {
        int baseWeight = 1;
        int additionalWeight = 0;
        
        if (voteWeightStrategy != null) {
            VoteContext context = VoteContext.forCandidate(nodeId, term.getCurrent());
            additionalWeight = voteWeightStrategy.calculateAdditionalWeight(context);
        }
        
        return baseWeight + additionalWeight;
    }

    /**
     * Calculates total received vote weight including self and granted peer votes.
     * 
     * @return total received vote weight
     */
    private int calculateReceivedVoteWeight() {
        int receivedWeight = 1;
        
        for (String peerId : peerIds) {
            if (votesReceived.containsKey(peerId)) {
                int peerWeight = 1;
                if (voteWeightStrategy != null) {
                    VoteContext context = VoteContext.forCandidate(peerId, term.getCurrent());
                    peerWeight = 1 + voteWeightStrategy.calculateAdditionalWeight(context);
                }
                receivedWeight += peerWeight;
            }
        }
        
        return receivedWeight;
    }

    /**
     * Calculates required weight to win election based on total peer weight.
     * Majority is calculated from total weight regardless of who has voted.
     * 
     * @return minimum weight needed (majority of total weight)
     */
    private int calculateRequiredWeight() {
        int totalWeight = 1;
        
        for (String peerId : peerIds) {
            int peerWeight = 1;
            if (voteWeightStrategy != null) {
                VoteContext context = VoteContext.forCandidate(peerId, term.getCurrent());
                peerWeight = 1 + voteWeightStrategy.calculateAdditionalWeight(context);
            }
            totalWeight += peerWeight;
        }
        
        return (totalWeight / 2) + 1;
    }

    /**
     * Calculates votes needed to win election (majority).
     * 
     * @return minimum votes needed (N/2 + 1)
     * @deprecated use calculateRequiredWeight() for weighted voting
     */
    @Deprecated
    private int calculateVotesNeeded() {
        int totalNodes = peerIds.size() + 1;
        return (totalNodes / 2) + 1;
    }

    public NodeState getCurrentState() {
        return currentState;
    }

    public Term getTerm() {
        return term;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public List<LogEntry> getLogEntries() {
        return Collections.unmodifiableList(new ArrayList<>(log));
    }

    public List<LeaderRecord> getLeaderHistory() {
        List<LeaderRecord> history = new ArrayList<>();
        for (LogEntry entry : log) {
            history.add(new LeaderRecord(entry.getTerm(), entry.getLeaderId()));
        }
        return history;
    }
    
    public List<String> getPeerIds() {
        return new ArrayList<>(peerIds);
    }

    public cn.itcraft.speedboat.config.MembershipConfig getMembershipConfig() {
        return membershipConfig;
    }

    public cn.itcraft.speedboat.strategy.membership.HealthCheckStrategy getHealthCheckStrategy() {
        return healthCheckStrategy;
    }

    public cn.itcraft.speedboat.strategy.membership.RegistryStrategy getRegistryStrategy() {
        return registryStrategy;
    }

    public cn.itcraft.speedboat.strategy.membership.ChangeValidationStrategy getChangeValidationStrategy() {
        return changeValidationStrategy;
    }

    public java.util.concurrent.ConcurrentHashMap<String, FailureRecord> getFailureRecords() {
        return failureRecords;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    public boolean propose(byte[] data) {
        if (!isLeader()) {
            logger.warn("Only leader can propose commands");
            return false;
        }

        if (data == null || data.length == 0) {
            logger.warn("Cannot propose empty data");
            return false;
        }

        long newIndex = getLastLogIndex() + 1;
        CommandLogEntry entry = CommandLogEntry.create(newIndex, term.getCurrent(), nodeId, data);
        log.add(entry);
        truncateLogIfNeeded();
        
        logger.info("Leader {} added entry at index {}, log size now {}", nodeId, newIndex, log.size());

        logger.info("Leader {} proposing command at index {}, sending append entries", nodeId, newIndex);
        sendAppendEntries();
        logger.info("Leader {} sent append entries for command at index {}", nodeId, newIndex);

        logger.info("Leader {} proposed command at index {}", nodeId, newIndex);
        return true;
    }

    private long getLastLogIndex() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).getIndex();
    }

    private long getLastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();
    }

    private LogEntry getEntryAt(long index) {
        for (LogEntry entry : log) {
            if (entry.getIndex() == index) {
                return entry;
            }
        }
        return null;
    }

    /**
     * 启动成员变更检测任务
     */
    private void startMembershipChangeDetection() {
        if (scheduler == null || !running || !membershipConfig.isMembershipChangeEnabled()) {
            return;
        }
        
        long checkInterval = membershipConfig.getHealthCheckInterval();
        membershipChangeFuture = scheduler.scheduleAtFixedRate(
            this::checkMembershipChanges,
            checkInterval,
            checkInterval,
            TimeUnit.MILLISECONDS
        );
    }
    
    /**
     * 停止成员变更检测任务
     */
    private void stopMembershipChangeDetection() {
        if (membershipChangeFuture != null) {
            membershipChangeFuture.cancel(false);
            membershipChangeFuture = null;
        }
    }
    
    /**
     * 检查成员变更
     */
    private void checkMembershipChanges() {
        if (!isLeader()) {
            // 只有 Leader 节点执行成员变更检测
            return;
        }
        
        // 执行健康检测
        List<String> unhealthyPeers = healthCheckStrategy.checkUnhealthyPeers(peerIds);
        for (String peerId : unhealthyPeers) {
            recordFailure(peerId);
        }
        
        // 检查注册中心信息
        List<String> registeredPeers = registryStrategy.getRegisteredPeers();
        if (registeredPeers != null && !registeredPeers.isEmpty()) {
            // 与当前成员列表比较
            Set<String> currentSet = new HashSet<>(peerIds);
            currentSet.add(nodeId); // 包括自己
            Set<String> registeredSet = new HashSet<>(registeredPeers);
            
            // 发现新节点
            Set<String> newPeers = new HashSet<>(registeredSet);
            newPeers.removeAll(currentSet);
            for (String newPeer : newPeers) {
                proposeAddMember(newPeer);
            }
            
            // 发现已移除节点（不在注册中心但仍在当前列表中）
            Set<String> removedPeers = new HashSet<>(currentSet);
            removedPeers.removeAll(registeredSet);
            for (String removedPeer : removedPeers) {
                // 跳过自己
                if (!removedPeer.equals(nodeId)) {
                    proposeRemoveMember(removedPeer);
                }
            }
        }
    }
    
    /**
     * 记录节点故障
     */
    private void recordFailure(String peerId) {
        failureRecords.compute(peerId, (key, record) -> {
            if (record == null) {
                record = new FailureRecord(peerId);
            }
            record.incrementFailures();
            
            // 检查是否达到故障阈值
            if (record.shouldProposeRemoval(membershipConfig.getFailureThreshold(), membershipConfig.getConfirmationNanos())) {
                proposeRemoveMember(peerId);
            }
            
            return record;
        });
    }
    
    /**
     * 提出添加成员
     */
    public boolean proposeAddMember(String newPeerId) {
        if (!isLeader()) {
            logger.warn("Only leader can propose member additions");
            return false;
        }
        
        // 验证变更
        boolean valid = changeValidationStrategy.validateAdd(newPeerId, peerIds);
        if (!valid) {
            logger.warn("Failed to validate addition of peer {}", newPeerId);
            return false;
        }
        
        // 创建成员变更条目
        MemberChangeEntry entry = MemberChangeEntry.add(
            getLastLogIndex() + 1,  // index
            term.getCurrent(),      // term
            nodeId,                 // leaderId
            newPeerId,              // nodeId
            null                    // address
        );
        
        // 添加到日志
        log.add(entry);
        
        // 复制到所有节点
        replicateMemberChange(entry);
        
        logger.info("Leader {} proposed to add member {}", nodeId, newPeerId);
        return true;
    }
    
    /**
     * 提出移除成员
     */
    public boolean proposeRemoveMember(String peerId) {
        if (!isLeader()) {
            logger.warn("Only leader can propose member removals");
            return false;
        }
        
        // 不能移除自己
        if (peerId.equals(nodeId)) {
            logger.warn("Cannot remove self from cluster");
            return false;
        }
        
        // 验证变更
        boolean valid = changeValidationStrategy.validateRemove(peerId, peerIds);
        if (!valid) {
            logger.warn("Failed to validate removal of peer {}", peerId);
            return false;
        }
        
        // 创建成员变更条目
        MemberChangeEntry entry = MemberChangeEntry.remove(
            getLastLogIndex() + 1,  // index
            term.getCurrent(),      // term
            nodeId,                 // leaderId
            peerId                  // nodeId
        );
        
        // 添加到日志
        log.add(entry);
        
        // 复制到所有节点
        replicateMemberChange(entry);
        
        logger.info("Leader {} proposed to remove member {}", nodeId, peerId);
        return true;
    }
    
    /**
     * 复制成员变更
     */
    private void replicateMemberChange(MemberChangeEntry entry) {
        if (transportLayer == null) {
            return;
        }
        
        for (String peerId : peerIds) {
            // 如果要移除的节点，跳过
            if (entry.getChangeType() == MemberChangeEntry.ChangeType.REMOVE && 
                entry.getPeerId().equals(peerId)) {
                continue;
            }
            
            List<LogEntry> entries = Collections.singletonList(entry);
            AppendEntriesRequest request = new AppendEntriesRequest(
                term.getCurrent(), nodeId, 
                entry.getIndex() - 1, getEntryTerm(entry.getIndex() - 1),
                entries, commitIndex
            );
            
            transportLayer.sendAppendEntries(peerId, request);
        }
    }
    
    /**
     * 处理成员变更
     */
    private void processMemberChange(MemberChangeEntry entry) {
        String peerId = entry.getPeerId();
        
        if (entry.getChangeType() == MemberChangeEntry.ChangeType.ADD) {
            // 添加新成员
            if (!peerIds.contains(peerId)) {
                peerIds.add(peerId);
                logger.info("Node {} added new peer {}", nodeId, peerId);
                
                // 如果是 Leader，初始化 nextIndex 和 matchIndex
                if (isLeader()) {
                    nextIndex.put(peerId, getLastLogIndex() + 1);
                    matchIndex.put(peerId, 0L);
                }
            }
        } else if (entry.getChangeType() == MemberChangeEntry.ChangeType.REMOVE) {
            // 移除成员
            if (peerIds.contains(peerId)) {
                peerIds.remove(peerId);
                logger.info("Node {} removed peer {}", nodeId, peerId);
                
                // 如果是 Leader，清理状态
                if (isLeader()) {
                    nextIndex.remove(peerId);
                    matchIndex.remove(peerId);
                }
                
                // 清理故障记录
                failureRecords.remove(peerId);
            }
        }
    }
    
    /**
     * 获取指定索引的条目任期
     */
    private long getEntryTerm(long index) {
        if (index <= 0) {
            return 0;
        }
        LogEntry entry = getEntryAt(index);
        return entry != null ? entry.getTerm() : 0;
    }

    public static class Builder {
        private String nodeId;
        private List<String> peerIds;
        private ElectionTimeout electionTimeout;
        private VoteWeightStrategy voteWeightStrategy;
        private GroupStrategy groupStrategy;
        private TransportLayer transportLayer;
        private int maxLogSize;
        private cn.itcraft.speedboat.config.MembershipConfig membershipConfig;
        private cn.itcraft.speedboat.strategy.membership.HealthCheckStrategy healthCheckStrategy;
        private cn.itcraft.speedboat.strategy.membership.RegistryStrategy registryStrategy;
        private cn.itcraft.speedboat.strategy.membership.ChangeValidationStrategy changeValidationStrategy;
        private StateMachine stateMachine;

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder peerIds(List<String> peerIds) {
            this.peerIds = peerIds;
            return this;
        }

        public Builder electionTimeout(ElectionTimeout electionTimeout) {
            this.electionTimeout = electionTimeout;
            return this;
        }

        public Builder voteWeightStrategy(VoteWeightStrategy voteWeightStrategy) {
            this.voteWeightStrategy = voteWeightStrategy;
            return this;
        }

        public Builder groupStrategy(GroupStrategy groupStrategy) {
            this.groupStrategy = groupStrategy;
            return this;
        }

        public Builder transportLayer(TransportLayer transportLayer) {
            this.transportLayer = transportLayer;
            return this;
        }

        public Builder maxLogSize(int maxLogSize) {
            this.maxLogSize = maxLogSize;
            return this;
        }

        public Builder membershipConfig(cn.itcraft.speedboat.config.MembershipConfig membershipConfig) {
            this.membershipConfig = membershipConfig;
            return this;
        }

        public Builder healthCheckStrategy(cn.itcraft.speedboat.strategy.membership.HealthCheckStrategy healthCheckStrategy) {
            this.healthCheckStrategy = healthCheckStrategy;
            return this;
        }

        public Builder registryStrategy(cn.itcraft.speedboat.strategy.membership.RegistryStrategy registryStrategy) {
            this.registryStrategy = registryStrategy;
            return this;
        }

        public Builder changeValidationStrategy(cn.itcraft.speedboat.strategy.membership.ChangeValidationStrategy changeValidationStrategy) {
            this.changeValidationStrategy = changeValidationStrategy;
            return this;
        }

        public Builder stateMachine(StateMachine stateMachine) {
            this.stateMachine = stateMachine;
            return this;
        }

        public RaftNode build() {
            if (nodeId == null) {
                throw new IllegalArgumentException("nodeId is required");
            }
            if (electionTimeout == null) {
                electionTimeout = new ElectionTimeout(
                    SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS, 
                    SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS);
            }
            return new RaftNode(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}