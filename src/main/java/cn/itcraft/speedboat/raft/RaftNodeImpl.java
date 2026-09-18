package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.config.SpeedboatConsts;
import cn.itcraft.speedboat.raft.executor.DefaultRaftNodeExecutor;
import cn.itcraft.speedboat.raft.executor.RaftNodeExecutor;
import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.PreVoteRequest;
import cn.itcraft.speedboat.rpc.PreVoteResponse;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.rpc.LockOpRequest;
import cn.itcraft.speedboat.rpc.LockOpResponse;
import cn.itcraft.speedboat.lock.LockCommand;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import cn.itcraft.speedboat.serialize.SerializationException;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.raft.report.RaftNodeReport;
import cn.itcraft.speedboat.statemachine.StateMachine;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link RaftNode} 接口的唯一实现（Phase B API/Impl 分层）。
 *
 * <p>所有状态变更经 {@code RaftNodeExecutor} 投递到 raft 单线程串行执行；
 * 私有 {@code doHandleXxx} 方法仅由 {@code RaftNode} 接口方法门面
 * （同步语义）与内部回调（异步语义）分派使用。</p>
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
public class RaftNodeImpl implements RaftNode {

    private static final Logger logger = LoggerFactory.getLogger(RaftNodeImpl.class);

    // ==================== 持久性等级标注（对标 MicroRaft [PERSISTENT] 自文档化）====================
    // 【PERSISTENT】节点标识 / 机房（启动期不可变）
    private final String nodeId;
    private final String datacenter;
    private volatile NodeState currentState;
    // 【PERSISTENT】term / votedFor / leaderId（prior to visibility 经 raftStore.persistAndFlushTerm 落盘）
    private final Term term;
    private volatile String votedFor;
    // 【PERSISTENT】term 内已投票保护（link 为 term 条件一部分；真正持久化以 raftStore 覆盖）
    private volatile long votedForTerm;
    private volatile String leaderId;

    private final ElectionTimeout electionTimeout;
    private volatile long lastHeartbeatNanos;

    private final VoteWeightStrategy voteWeightStrategy;
    private final GroupStrategy groupStrategy;
    private TransportLayer transportLayer;

    private final List<String> peerIds;
    /** 每个 peer 的机房标识，用于权重策略计算。未配置时默认空字符串（视为同机房） */
    private final Map<String, String> peerDatacenters;
    private final Map<String, Boolean> votesReceived;

    private volatile boolean running;
    /**
     * Raft 单消费者执行器（Actor 模型）。
     *
     * <p>所有状态变更只在该执行器的唯一线程上发生；外部线程（Netty IO、
     * 用户调用）仅允许投递任务或读取 volatile 快照。因此算法本体内部
     * 不再使用 synchronized——并发正确性由"任务串行 + 任务间 happens-before"
     * 保证（MicroRaft executor 契约的本地化）。</p>
     */
    private RaftNodeExecutor executor;
    private Future<?> electionTimeoutFuture;
    private Future<?> heartbeatFuture;
    private Future<?> membershipChangeFuture;

    // 【PERSISTENT】日志主体（append & rebuild 均经 raftStore.persistLogEntries）
    private final List<LogEntry> log;
    /**
     * 日志索引 → 条目的 O(1) 映射。
     *
     * <p>raft 单线程独占写（append/rebuild/truncate 同步维护），
     * 外部线程仅做 get 读快照——替代原 {@code getEntryAt} 对链表
     * 的 O(n) 线性扫描（Phase B 结构性修复）。</p>
     */
    private final java.util.concurrent.ConcurrentHashMap<Long, LogEntry> logIndexMap = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile long commitIndex;
    private volatile long lastApplied;
    // 【NOT-PERSISTENT】nextIndex/matchIndex 属 leader 活性状态，重启后重新探测
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
    /**
     * 持久化存储（缺省 {@link cn.itcraft.speedboat.persistence.NopRaftStore} 内存模式）。
     * 接口前置：将来接入磁盘 WAL 时不需要改算法代码。
     */
    cn.itcraft.speedboat.persistence.RaftStore raftStore;
    /**
     * 状态报告监听器（Phase E 可观测性；可能为 null）。
     * 事件触发 + 周期兜底双通道发布（对标 MicroRaft RaftNodeReport 机制）。
     */
    private cn.itcraft.speedboat.raft.report.RaftNodeReportListener reportListener;
    /** 周期性状态快照发布间隔（毫秒），对标 MicroRaft raftNodeReportPublishPeriodSecs */
    private static final long REPORT_PERIOD_MILLIS = 10_000L;
    // ==================== Phase C：预投票 & check-quorum ====================
    /** 预投票探测中标记（raft 单线程读写） */
    private volatile boolean prevoting;
    /** 本轮预投票已收集的 peer（含自己） */
    private final java.util.HashSet<String> prevotesReceived = new java.util.HashSet<>();
    /** check-quorum：最近一次各 peer 心跳响应时间戳（nanos 时间基） */
    private final java.util.concurrent.ConcurrentHashMap<String, Long> lastResponseNanos = new java.util.concurrent.ConcurrentHashMap<>();
    /**
     * check-quorum 心跳新鲜窗口（毫秒）。
     *
     * <p>对标 MicroRaft leaderHeartbeatTimeoutPeriodSecs（默认 10s）：
     * 该窗口远大于选举超时/心跳周期——多数派侧会先选出更高任期 leader 
     * 并把孤岛 leader"打散"，check-quorum 仅作为长分区的兜底降级，
     * 而非瞬态抖动的第一反应，避免误杀合法 Leader。</p>
     */
    public static final long DEFAULT_QUORUM_CHECK_TIMEOUT_MILLIS = 5000;
    private long quorumCheckTimeoutMillis = DEFAULT_QUORUM_CHECK_TIMEOUT_MILLIS;

    RaftNodeImpl(RaftNode.Builder builder) {
        this.nodeId = builder.nodeId;
        this.datacenter = builder.datacenter != null ? builder.datacenter : "";
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
        this.peerDatacenters = new ConcurrentHashMap<>();
        this.votesReceived = new ConcurrentHashMap<>();
        this.lastHeartbeatNanos = System.nanoTime();
        this.log = new CopyOnWriteArrayList<>();
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        this.maxLogSize = builder.maxLogSize > 0 ? builder.maxLogSize : SpeedboatConsts.DEFAULT_MAX_LOG_SIZE;
        this.checkpointInterval = builder.checkpointInterval > 0 ? builder.checkpointInterval : DEFAULT_CHECKPOINT_INTERVAL;
        
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
        // Actor 执行器：默认单线程调度实现，可注入自定义实现（如 JCTools MPSC 版）
        this.executor = builder.executor != null ? builder.executor : new DefaultRaftNodeExecutor();
        // 持久化缺省 Nop：库级 Builder 保持"零副作用"基线（测试/嵌入式调用方显式选档）；
        // 应用门面（Speedboat）负责把生产默认装配为 mmap（跨进程重启挂回，选主安全性最全）。
        this.raftStore = builder.raftStore != null ? builder.raftStore : cn.itcraft.speedboat.persistence.NopRaftStore.getInstance();
        this.reportListener = builder.reportListener;
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        executor.start();

        // 持久化接口前置：启动时尝试恢复任期状态（NopRaftStore 时为 null，行为不变）
        cn.itcraft.speedboat.persistence.RaftTermRecord restored = raftStore.restoreTerm();
        if (restored != null) {
            term.updateIfHigher(restored.getTerm());
            votedFor = restored.getVotedFor();
            votedForTerm = restored.getVotedFor() != null ? restored.getTerm() : -1;
            logger.info("Node {} restored term {} votedFor {} from store {}",
                nodeId, restored.getTerm(), restored.getVotedFor(), raftStore.getClass().getName());
        }

        // P4：WAL 读回链（defect-20260918-01 根治）——重启先重建日志，再把 commitIndex 交给
        // Leader 心跳推进；applyCommittedEntries 将从 lastApplied 全序重放 → lockTable/epoch 确定
        List<LogEntry> restoredLogs = raftStore.restoredLogEntries();
        if (!restoredLogs.isEmpty() && log.isEmpty()) {
            for (LogEntry e : restoredLogs) {
                LogEntry mirror;
                if (e.getEntryType() == LogEntry.EntryType.MEMBER_CHANGE && e instanceof MemberChangeEntry) {
                    MemberChangeEntry mce = (MemberChangeEntry) e;
                    mirror = mce.getChangeType() == MemberChangeEntry.ChangeType.ADD
                        ? MemberChangeEntry.add(e.getIndex(), e.getTerm(), e.getLeaderId(), mce.getNodeId(), mce.getAddress())
                        : MemberChangeEntry.remove(e.getIndex(), e.getTerm(), e.getLeaderId(), mce.getNodeId());
                } else if (e.getEntryType() == LogEntry.EntryType.COMMAND) {
                    mirror = CommandLogEntry.create(e.getIndex(), e.getTerm(), e.getLeaderId(),
                        e.getData() == null ? new byte[0] : e.getData().clone());
                } else {
                    LogEntry plain = new LogEntry(e.getIndex(), e.getTerm(), e.getLeaderId());
                    plain.setEntryType(e.getEntryType());
                    plain.setData(e.getData() == null ? null : e.getData().clone());
                    mirror = plain;
                }
                logIndexMap.put(e.getIndex(), mirror);
                log.add(mirror);
            }
            logger.info("Node {} rebuilt log from store {} entries={}", nodeId, raftStore.getClass().getName(), log.size());
        }

        // 状态机检查点恢复（M2）：在 apply 重放之前，先落到 checkpoint 时刻的锁表；
        // 之后只重放 > lastApplied 的日志尾巴（epoch 租约等由检查点承载）
        String checkpointFile = raftStore.getCheckpointFile();
        if (checkpointFile != null && stateMachine != null) {
            try {
                stateMachine.restore(checkpointFile);
                long smApplied = stateMachine.getLastAppliedIndex();
                long lastLogIdx = getLastLogIndex();
                if (smApplied > 0 && smApplied <= lastLogIdx) {
                    this.lastApplied = smApplied;
                } else if (smApplied > 0) {
                    logger.warn("Node {} checkpoint appliedIndex {} beyond restored log ({}) — ignoring",
                        nodeId, smApplied, lastLogIdx);
                }
            } catch (Exception e) {
                logger.warn("Node {} state machine checkpoint restore failed: {}", nodeId, e.toString());
            }
        }

        if (transportLayer != null) {
            // 入站消息全异步化：请求投递到 raft 单线程（SC），
            // 应答经 CompletableFuture 由传输层异步回写（IO 线程不再互等）
            transportLayer.setRequestVoteHandler(request ->
                onRaftThreadAsync(() -> doHandleRequestVote(request)));
            transportLayer.setHeartbeatHandler(request ->
                onRaftThreadAsync(() -> doHandleHeartbeat(request)));
            transportLayer.setAppendEntriesHandler(request ->
                onRaftThreadAsync(() -> doHandleAppendEntries(request)));
            transportLayer.setPreVoteHandler(request ->
                onRaftThreadAsync(() -> doHandlePreVoteRequest(request)));
            // 命名锁转发协议：Leader 侧裁决点（打点授权+propose）；非 Leader 侧直拒
            transportLayer.setLockOpHandler(request ->
                onRaftThreadAsync(() -> doHandleLockOp(request)));
        }

        resetElectionTimeout();

        // 修复历史遗留：构造期 scheduler 尚未创建导致成员检测从未真正启动，
        // 现改为 start() 统一启动所有定时任务
        if (membershipConfig.isMembershipChangeEnabled()) {
            startMembershipChangeDetection();
        }

        // Phase E 周期兜底快照：每 10s 由 raft 线程发布一次状态视图
        executor.scheduleAtFixedRate(
            () -> publishReport(RaftNodeReport.ReportReason.PERIODIC),
            REPORT_PERIOD_MILLIS, REPORT_PERIOD_MILLIS);

        logger.info("Node {} started as {} on raft thread [{}]", nodeId, currentState, executor.isRaftThread());
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
        // 优雅停机：先落一次检查点（若 store 支持）并 flush WAL 尾部，崩溃恢复失去"最顺手"锚点
        try {
            String checkpointFile = raftStore.getCheckpointFile();
            if (checkpointFile != null && stateMachine != null && lastApplied > lastCheckpointApplied) {
                stateMachine.snapshot(checkpointFile);
                lastCheckpointApplied = lastApplied;
                raftStore.markCheckpoint(lastApplied);
            }
            raftStore.flush();
        } catch (Exception e) {
            logger.warn("Node {} shutdown checkpoint/flush failed: {}", nodeId, e.toString());
        }
        if (executor != null) {
            executor.shutdown();
        }
        logger.info("Node {} shutdown", nodeId);
    }

    /**
     * 将外部同步调用收敛到 raft 线程的兼容门面。
     *
     * <p>若当前已处于 raft 线程则直接内联执行（内部调用链全部如此）；
     * 否则投递任务并阻塞等待结果（有限时长，超时视为节点忙）。
     * 等待在调用方线程上进行，raft 线程永远不会被它阻塞。</p>
     *
     * @param task raft 线程上执行的任务
     * @param <T>  返回类型
     * @return 任务结果
     */
    private <T> T onRaftThread(java.util.concurrent.Callable<T> task) {
        try {
            if (executor.isRaftThread()) {
                return task.call();
            }
            return executor.submit(task).get(SpeedboatConsts.SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // 停机后被继续调用属预期场景，保留具体异常类型供调用方（propose 等）做退化
            throw e;
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalStateException("Raft task failed", cause);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Raft executor busy, task timed out", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting raft thread", e);
        } catch (Exception e) {
            throw new IllegalStateException("Raft task failed", e);
        }
    }

    /**
     * raft 线程异步执行任务，结果写入 CompletableFuture（以异常完成亦然）。
     *
     * <p>这是入站消息处理器与执行器之间的标准桥接：调用线程（发送方侧）
     * 拿到 Future 即返回，绝不阻塞等待接收方 raft 线程，避免 mock 集群
     * 场景的跨节点 raft 线程互等死锁。</p>
     */
    private <T> CompletableFuture<T> onRaftThreadAsync(java.util.concurrent.Callable<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                future.complete(task.call());
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    public void transitionTo(NodeState newState) {
        onRaftThread(() -> {
            doTransitionTo(newState);
            return null;
        });
    }

    private void doTransitionTo(NodeState newState) {
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
        
        publishReport(RaftNodeReport.ReportReason.ROLE_CHANGE);
        logger.info("Node {} transitioned from {} to {}", nodeId, oldState, newState);
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        return onRaftThread(() -> doHandleRequestVote(request));
    }

    private RequestVoteResponse doHandleRequestVote(RequestVoteRequest request) {
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

        // Phase C leader stickiness：现任 leader 心跳仍健康时不被同任期请求动摇
        // （更高任期请求在前面已处理为实现 FOLLOWER+term 对齐，正常放行）
        boolean leaderFresh = isLeaderHeartbeatFresh();
        boolean canVote = (votedFor == null || votedFor.equals(request.getCandidateId()))
            && votedForTerm != term.getCurrent()
            && !(leaderFresh && request.getTerm() < term.getCurrent());
        
        if (canVote && request.getTerm() >= term.getCurrent()) {
            votedFor = request.getCandidateId();
            votedForTerm = term.getCurrent();
            persistTermState(term.getCurrent(), votedFor, leaderId);
            resetElectionTimeout();
            logger.info("Node {} voted for {} in term {}", nodeId, request.getCandidateId(), request.getTerm());
            return new RequestVoteResponse(term.getCurrent(), true);
        }
        
        logger.info("Node {} rejected vote for {} (votedFor: {}, currentTerm: {})", 
            nodeId, request.getCandidateId(), votedFor, term.getCurrent());
        return new RequestVoteResponse(term.getCurrent(), false);
    }

    public HeartbeatResponse handleHeartbeat(HeartbeatRequest request) {
        return onRaftThread(() -> doHandleHeartbeat(request));
    }

    private HeartbeatResponse doHandleHeartbeat(HeartbeatRequest request) {
        AppendEntriesRequest appendRequest = AppendEntriesRequest.heartbeat(
            request.getTerm(), request.getLeaderId(), 0, 0, 0
        );
        AppendEntriesResponse response = doHandleAppendEntries(appendRequest);
        return new HeartbeatResponse(response.getTerm(), response.isSuccess());
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        return onRaftThread(() -> doHandleAppendEntries(request));
    }

    private AppendEntriesResponse doHandleAppendEntries(AppendEntriesRequest request) {
        if (request.getTerm() < term.getCurrent()) {
            logger.info("Rejecting AppendEntries from {} with lower term {}", 
                request.getLeaderId(), request.getTerm());
            return new AppendEntriesResponse(term.getCurrent(), false, getLastLogIndex());
        }

        if (request.getTerm() > term.getCurrent()) {
            term.updateIfHigher(request.getTerm());
            votedFor = null;
            votedForTerm = -1;
            persistTermState(term.getCurrent(), votedFor, leaderId);
        }

        if (currentState != NodeState.FOLLOWER) {
            transitionTo(NodeState.FOLLOWER);
        }

        leaderId = request.getLeaderId();
        resetElectionTimeout();
        lastHeartbeatNanos = System.nanoTime();

        logger.debug("Follower {} received appendEntries from leader {}, entries={}, prevLogIndex={}, leaderCommit={}", 
            nodeId, request.getLeaderId(), request.getEntries().size(), request.getPrevLogIndex(), request.getLeaderCommit());

        if (request.getPrevLogIndex() > 0) {
            if (getLastLogIndex() < request.getPrevLogIndex()) {
                logger.info("Follower {} log too short: {} < prevLogIndex {}", 
                    nodeId, getLastLogIndex(), request.getPrevLogIndex());
                // 返回 lastLogIndex，Leader 据此设置 nextIndex = matchIndex + 1，逐步回退
                return new AppendEntriesResponse(term.getCurrent(), false, getLastLogIndex());
            }

            LogEntry prevEntry = getEntryAt(request.getPrevLogIndex());
            if (prevEntry == null || prevEntry.getTerm() != request.getPrevLogTerm()) {
                logger.info("Follower {} log term mismatch at index {}, localTerm={}, expectedTerm={}",
                    nodeId, request.getPrevLogIndex(),
                    prevEntry != null ? prevEntry.getTerm() : "null",
                    request.getPrevLogTerm());
                // 返回 lastLogIndex，Leader 逐步回退 nextIndex 直到找到一致点
                // 这是标准 Raft 的做法，保证收敛；优化的快速回退需要额外协议支持
                return new AppendEntriesResponse(term.getCurrent(), false, getLastLogIndex());
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
            // 重建后统一重打索引（幂等，容量与 log list 一致）
            logIndexMap.clear();
            for (LogEntry existing : log) {
                logIndexMap.put(existing.getIndex(), existing);
            }
            raftStore.persistLogEntries(newLog);
            truncateLogIfNeeded();

            logger.debug("Follower {} appended {} entries, log size now {}", 
                nodeId, request.getEntries().size(), log.size());
        }

        if (request.getLeaderCommit() > commitIndex) {
            long oldCommitIndex = commitIndex;
            commitIndex = Math.min(request.getLeaderCommit(), getLastLogIndex());
            applyCommittedEntries();
            logger.debug("Node {} commitIndex updated: {} -> {}, lastApplied={}", 
                nodeId, oldCommitIndex, commitIndex, lastApplied);
        }

        return new AppendEntriesResponse(term.getCurrent(), true, getLastLogIndex());
    }

    public void startElection() {
        onRaftThread(() -> {
            doStartElection();
            return null;
        });
    }

    private void doStartElection() {
        if (currentState != NodeState.FOLLOWER && currentState != NodeState.CANDIDATE) {
            return;
        }
        
        doTransitionTo(NodeState.CANDIDATE);
        persistTermState(term.getCurrent(), votedFor, leaderId);
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

    /**
     * 预投票探测（Phase C，对标 MicroRaft pre-vote）。
     *
     * <p>不递增真实任期地询问多数派"若我发起 term+1 正式选举你愿意投票吗"；
     * 探测超时仍未转正式选举则直接进入正式选举（自复位语义），
     * 保证活性不回退：链路畅通时预票一次到达即选主，失败最多多等一轮超时。</p>
     */
    private void doStartPreVote() {
        if (!running || transportLayer == null) {
            return;
        }
        if (currentState != NodeState.FOLLOWER) {
            return;
        }
        // 已有探测在途则不重复发起（防任务重入）
        if (prevoting) {
            return;
        }
        // 自适应预投票：本集群尚未产生任何任期（term==0 且无 leader）时跳过探测，
        // 直接进入正式选举——保证首主选择在单轮选举窗口内完成（与基线兼容）
        if (term.getCurrent() == 0 && leaderId == null) {
            doStartElection();
            return;
        }

        prevoting = true;
        prevotesReceived.clear();
        prevotesReceived.add(nodeId);
        long probeTerm = term.getCurrent() + 1;
        logger.info("Node {} starting pre-vote for term {}", nodeId, probeTerm);

        if (peerIds.isEmpty()) {
            prevoting = false;
            doStartElection();
            return;
        }

        long lastLogIndex = getLastLogIndex();
        long lastLogTerm = getLastLogTerm();
        final long capturedProbeTerm = probeTerm;
        for (String peerId : peerIds) {
            PreVoteRequest request = new PreVoteRequest(probeTerm, nodeId, lastLogIndex, lastLogTerm);
            transportLayer.sendPreVote(peerId, request)
                .thenAccept(response -> executor.execute(
                    () -> doHandlePreVoteResponse(peerId, response, capturedProbeTerm)));
        }

        // 探测超时：仍未转正式选举（多数派拒绝/失联）→ 兜底进入正式选举保证活性
        executor.schedule(() -> {
            if (prevoting) {
                prevoting = false;
                logger.info("Node {} pre-vote timeout, fallback to real election", nodeId);
                doStartElection();
            }
        }, electionTimeout.getNext());
    }

    private void doHandlePreVoteResponse(String peerId, PreVoteResponse response, long probeTerm) {
        if (!prevoting) {
            return;
        }
        if (response.isVoteGranted()) {
            prevotesReceived.add(peerId);
            long grantedWeight = 1;
            for (String grantedPeer : prevotesReceived) {
                if (grantedPeer.equals(nodeId)) {
                    continue;
                }
                if (voteWeightStrategy != null) {
                    VoteContext context = VoteContext.forDatacenter(grantedPeer, getPeerDatacenter(grantedPeer), probeTerm);
                    grantedWeight += 1 + voteWeightStrategy.calculateAdditionalWeight(context);
                } else {
                    grantedWeight += 1;
                }
            }
            if (grantedWeight >= calculateRequiredWeight()) {
                prevoting = false;
                logger.info("Node {} pre-vote quorum reached ({}>={}), promoting to real election",
                    nodeId, grantedWeight, calculateRequiredWeight());
                doStartElection();
            }
            return;
        }

        // 明确拒绝且应答任期高于本地任期：仅中止本轮探测并降级，
        // 不做 term 爬升（拒绝响应的 term 属接收方视角，盲目收敛会在假性冲突下
        // 造成 term 无限爬升；任期推进均依赖正式选举/心跳路径）
        // 占位失败（term=0）属"无响应"语义——忽略，靠探测超时兜底进入正式选举
        if (response.getTerm() > term.getCurrent()) {
            doTransitionTo(NodeState.FOLLOWER);
            prevoting = false;
        }
    }


    private long probeTerm() {
        return term.getCurrent() + 1;
    }

    /**
     * 接收方处理预投票探测（raft 单线程内执行）。
     *
     * <p>三条拒绝规则：</p>
     * <ol>
     *   <li>请求探测 term <= 本地当前任期（陈旧探测）；</li>
     *   <li>现任 leader 心跳新鲜（sticky）：健康 leader 存活时不接受任何任期重选；</li>
     *   <li>候选者日志不新于本地（选举安全校验）。</li>
     * </ol>
     */
    private PreVoteResponse doHandlePreVoteRequest(PreVoteRequest request) {
        long localTerm = term.getCurrent();
        if (request.getTerm() <= localTerm) {
            logger.debug("Node {} rejects pre-vote from {} (stale probe term {} <= {})",
                nodeId, request.getCandidateId(), request.getTerm(), localTerm);
            return new PreVoteResponse(request.getRequestId(), localTerm, false);
        }

        // sticky：自身已是 Leader（现任即自己），或现任 leader 心跳仍新鲜则拒绝
        // （Leader 的 lastHeartbeatNanos 不由自身心跳刷新，必须显式判定角色）
        if (currentState == NodeState.LEADER || isLeaderHeartbeatFresh()) {
            logger.info("Node {} rejects pre-vote from {} (self-leader or live leader {})",
                nodeId, request.getCandidateId(), leaderId);
            return new PreVoteResponse(request.getRequestId(), localTerm, false);
        }

        // 日志安全校验：候选者日志必须不旧于本地
        if (request.getLastLogTerm() < getLastLogTerm()
            || (request.getLastLogTerm() == getLastLogTerm() && request.getLastLogIndex() < getLastLogIndex())) {
            logger.info("Node {} rejects pre-vote from {} (log stale: candidate index/term {}<{} vs local {}/{}), ",
                nodeId, request.getCandidateId(),
                request.getLastLogIndex(), request.getLastLogTerm(),
                getLastLogIndex(), getLastLogTerm());
            return new PreVoteResponse(request.getRequestId(), localTerm, false);
        }

        return new PreVoteResponse(request.getRequestId(), localTerm, true);
    }

    /** leader 心跳新鲜阈值：取选举超时上限，避免误判抖动 */
    private long heartbeatFreshThresholdMs() {
        return electionTimeout.getMaxMs();
    }

    private long elapsedLeaderHeartbeatMs() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastHeartbeatNanos);
    }

    /** 现任 leader 心跳是否新鲜（check-quorum 与 sticky 共用） */
    private boolean isLeaderHeartbeatFresh() {
        return leaderId != null
            && elapsedLeaderHeartbeatMs() < heartbeatFreshThresholdMs();
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
                .thenAccept(response -> executor.execute(() -> {
                    if (response.isVoteGranted() && currentState == NodeState.CANDIDATE) {
                        votesReceived.put(peerId, true);
                        checkElectionResult(calculateReceivedVoteWeight(), requiredWeight);
                    } else if (response.getTerm() > term.getCurrent()) {
                        term.updateIfHigher(response.getTerm());
                        doTransitionTo(NodeState.FOLLOWER);
                    }
                }));
        }

        executor.schedule(() -> {
            if (currentState == NodeState.CANDIDATE) {
                logger.info("Node {} election timeout, starting new election", nodeId);
                doStartElection();
            }
        }, electionTimeout.getNext());
    }

    private void checkElectionResult(int currentWeight, int requiredWeight) {
        if (currentState != NodeState.CANDIDATE) {
            return;
        }
        
        logger.info("Node {} has weight {}, needs {}", nodeId, currentWeight, requiredWeight);
        
        if (currentWeight >= requiredWeight) {
            becomeLeader();
        }
    }

    public void becomeLeader() {
        onRaftThread(() -> {
            doBecomeLeader();
            return null;
        });
    }

    private void doBecomeLeader() {
        if (currentState != NodeState.CANDIDATE) {
            return;
        }
        
        transitionTo(NodeState.LEADER);
        leaderId = nodeId;
        
        appendLeaderEntry();
        
        // Become leader时，设置matchIndex：自己的matchIndex为logSize，peer的matchIndex为0
        long logSize = getLastLogIndex();
        long now = System.nanoTime();
        for (String peerId : peerIds) {
            nextIndex.put(peerId, logSize + 1);
            matchIndex.put(peerId, 0L);
            lastResponseNanos.put(peerId, now);
        }
        matchIndex.put(nodeId, logSize);
        
        // Become leader时，立即提交自己提出的entry（Raft协议要求）
        commitIndex = logSize;
        applyCommittedEntries();
        
        sendAppendEntries();
        // 心跳由 doTransitionTo(LEADER) 分支统一武装，避免重复调度泄漏
        
        logger.info("Node {} became leader for term {}, commitIndex set to {}", nodeId, term.getCurrent(), commitIndex);
    }

    private void appendLeaderEntry() {
        long newIndex = getLastLogIndex() + 1;
        LogEntry entry = new LogEntry(newIndex, term.getCurrent(), nodeId);
        log.add(entry);
        logIndexMap.put(newIndex, entry);
        raftStore.persistLogEntries(java.util.Collections.singletonList(entry));
        truncateLogIfNeeded();
        logger.info("Leader {} appended entry at index {} term {}", nodeId, newIndex, term.getCurrent());
    }

    public void sendHeartbeat() {
        // Phase C check-quorum：多数派心跳响应超时则主动降级（防分区脑裂窗口）
        doCheckQuorumMaybeDemote();
        sendAppendEntries();
    }

    /**
     * check-quorum（对标 MicroRaft quorumResponseTimestamp 主动降级）。
     *
     * <p>leader 在自身心跳节拍上检查"心跳新鲜"的法定人数权重：
     * 若新鲜响应的权重（自身+各 peer 在阈值内有无响应）无法构成多数派，
     * 主动降级为 follower——孤岛 leader 不再对外产生日志/锁等影响。</p>
     *
     * <p>新鲜阈值 = 3 × 心跳间隔（需覆盖一个完整选举窗口；
     * 解群后多数派侧很快选出新 leader 并以更高 term 打奔本节点）。</p>
     */
    private void doCheckQuorumMaybeDemote() {
        if (currentState != NodeState.LEADER) {
            return;
        }
        long thresholdNanos = TimeUnit.MILLISECONDS.toNanos(quorumCheckTimeoutMillis);
        long now = System.nanoTime();
        long freshWeight = 1;  // 自己代表新鲜的 leader 响应
        for (String peerId : peerIds) {
            Long last = lastResponseNanos.get(peerId);
            if (last != null && (now - last) <= thresholdNanos) {
                freshWeight += 1;
                if (voteWeightStrategy != null) {
                    VoteContext context = VoteContext.forDatacenter(peerId, getPeerDatacenter(peerId), term.getCurrent());
                    freshWeight += voteWeightStrategy.calculateAdditionalWeight(context);
                }
            }
        }
        long required = calculateRequiredWeight();
        if (freshWeight < required) {
            logger.warn("Node {} check-quorum FAILED (freshWeight={} < required={}), demoting to follower",
                nodeId, freshWeight, required);
            doTransitionTo(NodeState.FOLLOWER);
            leaderId = null;
        }
    }

    public void sendAppendEntries() {
        if (currentState != NodeState.LEADER || transportLayer == null) {
            logger.info("Node {} not leader ({}) or transportLayer null ({}), cannot send append entries", 
                nodeId, currentState == NodeState.LEADER, transportLayer != null);
            return;
        }
        
        logger.debug("Leader {} sending append entries to {} peers, logSize={}, commitIndex={}", 
            nodeId, peerIds.size(), log.size(), commitIndex);
        for (String peerId : peerIds) {
            sendAppendEntriesToPeer(peerId);
        }
        
        logger.debug("Leader {} sent append entries to {} peers", nodeId, peerIds.size());
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
        
        logger.debug("Leader {} sending to peer {}: nextIdx={}, logSize={}, entriesToSend={}, commitIndex={}", 
            nodeId, peerId, nextIdx, log.size(), entriesToSend.size(), commitIndex);
        
        AppendEntriesRequest request = new AppendEntriesRequest(
            term.getCurrent(), nodeId, prevLogIndex, prevLogTerm, entriesToSend, commitIndex
        );
        
        logger.debug("Leader {} calling transportLayer.sendAppendEntries to peer {}, transportLayer={}", nodeId, peerId, transportLayer.getClass().getName());
        transportLayer.sendAppendEntries(peerId, request)
            .thenAccept(response -> executor.execute(
                () -> doHandleAppendEntriesResponse(peerId, response)));
    }

    private void doHandleAppendEntriesResponse(String peerId, AppendEntriesResponse response) {
        logger.debug("Leader {} received appendEntries response from {}: success={}, matchIndex={}", 
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
            lastResponseNanos.put(peerId, System.nanoTime());
            matchIndex.put(peerId, response.getMatchIndex());
            nextIndex.put(peerId, response.getMatchIndex() + 1);
            logger.debug("Leader {} matchIndex for {} updated to {}, matchIndex={}", 
                nodeId, peerId, response.getMatchIndex(), matchIndex);
            advanceCommitIndex();
        } else {
            // 失配回退（Raft §5.3）：hint=matchIndex+1 为下界；每轮至少回退 1。
            // 仅取 hint 会在“同索引不同 term”的分割日志（两任 leader 写同一 index）下
            // 恒等于当前 nextIndex，prevLog 探测死循环、日志永不收敛。
            long current = nextIndex.getOrDefault(peerId, 1L);
            long hint = response.getMatchIndex() + 1;
            long newNextIndex = Math.max(1, Math.min(hint, current - 1));
            nextIndex.put(peerId, newNextIndex);
            logger.debug("Leader {} decrementing nextIndex for {} to {} (hint={}, current={})",
                nodeId, peerId, newNextIndex, hint, current);
        }
    }

    private void advanceCommitIndex() {
        logger.debug("Leader {} advanceCommitIndex: lastLogIndex={}, commitIndex={}", 
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
                            VoteContext context = VoteContext.forDatacenter(peerId, getPeerDatacenter(peerId), term.getCurrent());
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
                    logger.debug("Leader {} cannot advance commitIndex to {}, matchedWeight={}, totalWeight={}, matchIndex={}", 
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
                logger.debug("Node {} applied entry at index {}: type={}", 
                    nodeId, lastApplied, entry.getEntryType());
            }
        }
        maybeCheckpoint();
    }

    private static final int DEFAULT_CHECKPOINT_INTERVAL = 1024;
    /** 状态机检查点触发阈值（applied 增量；Builder.checkpointInterval / raft.checkpoint.interval 可配） */
    private final int checkpointInterval;
    /** 触发水位：最近一次检查点覆盖的 appliedIndex */
    private long lastCheckpointApplied = 0L;

    /**
     * 检查点判定：仅当 raftStore 提供检查点路径（MmapRaftStore）且间隔达标时落盘。
     * raft 单线程调用；同步写（频率可配，缺省 1024 才一次），阻塞可接受。
     *
     * <p>成功次序铁律：先检查点落盘 + flush，再 {@code markCheckpoint} 上报水位——
     * 水位一旦上报即允许存储层压实回收 WAL 前缀，顺序颠倒会导致回收后仍无检查点可恢复。</p>
     */
    private void maybeCheckpoint() {
        if (stateMachine == null) {
            return;
        }
        String checkpointFile = raftStore.getCheckpointFile();
        if (checkpointFile == null) {
            return;
        }
        if (lastApplied - lastCheckpointApplied < checkpointInterval) {
            return;
        }
        try {
            stateMachine.snapshot(checkpointFile);
            raftStore.flush();
            lastCheckpointApplied = lastApplied;
            raftStore.markCheckpoint(lastApplied);
        } catch (Exception e) {
            logger.warn("Node {} checkpoint failed: {}", nodeId, e.toString());
        }
    }

    /**
     * 内存日志裁剪（maxLogSize 生效路径，P4-M4 修正）。
     *
     * <p><b>历史缺陷</b>：旧逻辑"遇到已提交条目即 break"，提交推进后永不裁剪、
     * maxLogSize 对已提交区域形同虚设（日志无限增长）。</p>
     *
     * <p><b>两条安全裁剪路径（合并保留）</b>：</p>
     * <ol>
     *   <li><b>未提交积压</b>（含 follower）：index &gt; commitIndex 的条目被裁剪是安全的
     *       —— 未提交条目由 Leader 按 nextIndex 重发收敛；</li>
     *   <li><b>已提交前缀</b>（仅 Leader）：要求所有 peer 的 matchIndex 已覆盖该前缀
     *       （或单节点），此时该前缀不可能再被任何 peer 需要重发（本框架无 INSTALL_SNAPSHOT）。</li>
     * </ol>
     */
    private void truncateLogIfNeeded() {
        if (log.size() <= maxLogSize) {
            return;
        }
        long safeCommittedFloor = resyncSafeFloor();
        int allowedRemove = log.size() - maxLogSize;
        int removeCount = 0;
        for (LogEntry entry : log) {
            if (removeCount >= allowedRemove) {
                break;
            }
            boolean uncommitted = entry.getIndex() > commitIndex;
            boolean safeCommitted = safeCommittedFloor >= 0 && entry.getIndex() <= safeCommittedFloor;
            if (!uncommitted && !safeCommitted) {
                break;
            }
            removeCount++;
        }
        if (removeCount <= 0) {
            return;
        }
        for (int i = 0; i < removeCount; i++) {
            LogEntry oldest = log.get(i);
            logIndexMap.remove(oldest.getIndex());
        }
        log.subList(0, removeCount).clear();
        logger.info("Node {} trimmed {} log entries (maxLogSize={}, safeCommittedFloor={}, logSize={})",
            nodeId, removeCount, maxLogSize, safeCommittedFloor, log.size());
    }

    /**
     * 可安全裁剪的日志上界：所有 peer 都已复制的最大连续前缀（≤ commitIndex）。
     * 返回 -1 表示当前不可裁剪（follower / 有 peer 匹配未知）。
     */
    private long resyncSafeFloor() {
        if (peerIds.isEmpty()) {
            return commitIndex;
        }
        if (!isLeader()) {
            return -1;
        }
        long floor = Long.MAX_VALUE;
        for (String peer : peerIds) {
            Long m = matchIndex.get(peer);
            if (m == null) {
                return -1;
            }
            floor = Math.min(floor, m);
        }
        return Math.min(floor, commitIndex);
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

    /** 成员全集（含自身，observer 视角应见完整成员列表） */
    private List<String> clusterMemberIds() {
        List<String> all = new ArrayList<>(peerIds);
        if (!all.contains(nodeId)) {
            all.add(nodeId);
        }
        return all;
    }

    /**
     * 构建并发布一次状态快照（raft 单线程内收集，快照值不可变）。
     * 无 listener 时为 no-op；监听器异常只告警不影响算法。
     */
    private void publishReport(RaftNodeReport.ReportReason reason) {
        if (reportListener == null) {
            return;
        }
        try {
            RaftNodeReport report = new RaftNodeReport(
                nodeId, currentState, term.getCurrent(), votedFor, leaderId,
                commitIndex, lastApplied, (int) getLastLogIndex(),
                clusterMemberIds(), new java.util.HashMap<>(matchIndex));
            reportListener.onReport(report);
        } catch (Exception e) {
            logger.warn("Report listener failed: {}", e.toString(), e);
        }
    }

    public void resetElectionTimeout() {
        if (executor == null) {
            return;
        }

        if (electionTimeoutFuture != null) {
            electionTimeoutFuture.cancel(false);
        }

        electionTimeout.reset();
        long timeout = electionTimeout.getNext();

        electionTimeoutFuture = executor.schedule(
            new cn.itcraft.speedboat.raft.task.ElectionTimeoutTask(() -> runElectionTimeout(timeout)), timeout);
    }

    /**
     * 选举超时到期后的判定（raft 单线程内执行）：
     * 心跳真超时则发起选举，否则重新武装下一次检测（self-rescheduling）。
     */
    /**
     * 任期状态持久化（term/votedFor/leader 级别原子落盘）。
     *
     * <p>调用时机参照 MicroRaft 的"先持久化后可见"原则：
     * 任期被提升/投票授予后、内存可见性发布前调用。</p>
     */
    private void persistTermState(long termVal, String votedForVal, String leaderVal) {
        try {
            raftStore.persistAndFlushTerm(termVal, votedForVal, leaderVal);
        } catch (Exception e) {
            throw new IllegalStateException(
                String.format("Node %s persist term state failed on store %s", nodeId, raftStore.getClass().getName()), e);
        }
    }

    private void runElectionTimeout(long timeoutMs) {
        if (running && currentState != NodeState.LEADER) {
            long elapsedNanos = System.nanoTime() - lastHeartbeatNanos;
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
            if (elapsedMs >= timeoutMs) {
                // Phase C：先经预投票探测，避免被分区恢复节点以暴涨 term 打奔现行任
                logger.info("Node {} election timeout (elapsed={}ms), starting pre-vote", nodeId, elapsedMs);
                doStartPreVote();
            } else {
                resetElectionTimeout();
            }
        }
    }

    private void startHeartbeat() {
        if (!running) {
            return;
        }
        // 幂等武装：先取消既有心跳再调度，防止候选/登基双路径重复调度产生
        // "孤儿心跳"（旧 Future 被覆盖引用后永远无法取消，stepdown 后仍持续发包）
        cancelHeartbeat();

        long heartbeatInterval = groupStrategy != null && groupStrategy.getHeartbeatInterval() > 0
            ? groupStrategy.getHeartbeatInterval() : 50;

        heartbeatFuture = executor.scheduleAtFixedRate(
            new cn.itcraft.speedboat.raft.task.HeartbeatTask(this::sendHeartbeat),
            0,
            heartbeatInterval
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
            VoteContext context = VoteContext.forDatacenter(request.getCandidateId(), datacenter, request.getTerm());
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
            VoteContext context = VoteContext.forDatacenter(nodeId, datacenter, term.getCurrent());
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
                    VoteContext context = VoteContext.forDatacenter(peerId, getPeerDatacenter(peerId), term.getCurrent());
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
                VoteContext context = VoteContext.forDatacenter(peerId, getPeerDatacenter(peerId), term.getCurrent());
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

    /**
     * 设置指定 peer 的机房标识。
     *
     * <p>权重策略（如 {@code DatacenterVoteWeightStrategy}）依赖此信息计算跨机房权重。
     * 未设置的 peer 默认视为空字符串（与本节点同机房）。</p>
     *
     * @param peerId     peer 节点 ID
     * @param datacenter peer 所在机房标识
     */
    public void setPeerDatacenter(String peerId, String datacenter) {
        peerDatacenters.put(peerId, datacenter != null ? datacenter : "");
    }

    /**
     * 获取指定 peer 的机房标识。
     *
     * @param peerId peer 节点 ID
     * @return peer 的机房标识，未设置时返回空字符串
     */
    String getPeerDatacenter(String peerId) {
        return peerDatacenters.getOrDefault(peerId, "");
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

    /**
     * 提交一条命令到 Raft 日志。
     *
     * <p>调用者应使用返回的日志索引配合 {@code waitForApply} 确认命令已被状态机应用。
     * 返回 -1 表示当前节点不是 Leader 或数据为空，命令未写入日志。</p>
     *
     * @param data 序列化后的命令数据
     * @return 命令在日志中的索引（≥1），或 -1 表示失败
     */
    public long propose(byte[] data) {
        try {
            return onRaftThread(() -> doPropose(data));
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // 节点已 shutdown、executor 已终止：propose 返回失败占位（-1）
            logger.warn("Node {} shutdown, propose rejected", nodeId);
            return -1;
        }
    }

    private long doPropose(byte[] data) {
        if (!isLeader()) {
            logger.warn("Only leader can propose commands");
            return -1;
        }

        if (data == null || data.length == 0) {
            logger.warn("Cannot propose empty data");
            return -1;
        }

        long newIndex = getLastLogIndex() + 1;
        CommandLogEntry entry = CommandLogEntry.create(newIndex, term.getCurrent(), nodeId, data);
        log.add(entry);
        logIndexMap.put(newIndex, entry);
        raftStore.persistLogEntries(java.util.Collections.singletonList(entry));
        truncateLogIfNeeded();
        
        logger.debug("Leader {} added entry at index {}, log size now {}", nodeId, newIndex, log.size());

        sendAppendEntries();

        logger.debug("Leader {} proposed command at index {}", nodeId, newIndex);
        return newIndex;
    }

    private long getLastLogIndex() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).getIndex();
    }

    /**
     * 锁操作转发裁决点（Leader 收到非 Leader 成员的锁命令时在 raft 线程执行）。
     *
     * <p>语义：只裁决"是否收录提案并回执日志索引"，不做互斥判定——
     * 互斥判定在状态机 apply（全序）发生，所有副本按同一日志序得出同一结论；
     * Leader 收录前仅做 <b>快速失败预检</b>（预检由申请方可选完成，此处仅校验提案本身）。</p>
     *
     * <p>授权打点：Leader 在 propose 前以自身时钟写入 grantTimestampMs，
     * 随命令复制后全网到期点一致（多持有者租约语义的基石）。</p>
     */
    private LockOpResponse doHandleLockOp(LockOpRequest request) {
        LockOpResponse rejected = new LockOpResponse(request.getRequestId(), false, -1);

        if (!isLeader()) {
            logger.debug("Node {} not leader, reject lock op: requestId={}", nodeId, request.getRequestId());
            return rejected;
        }

        byte[] payload = request.getCommand();
        if (payload == null || payload.length == 0) {
            logger.warn("Node {} empty lock command payload: requestId={}", nodeId, request.getRequestId());
            return rejected;
        }

        try {
            LockCommand command = new ProtostuffSerializer().deserialize(payload, LockCommand.class);
            if (command == null || command.getLockName() == null || command.getNodeId() == null
                || command.getCommandType() == null) {
                logger.warn("Node {} malformed lock command: requestId={}", nodeId, request.getRequestId());
                return rejected;
            }

            // Leader 授权打点（提案内容重编排：原 requestId/lockName/申请者身份原样保留）
            LockCommand stamped = new LockCommand(
                command.getLockName(), command.getNodeId(), command.getCommandType(),
                command.getRequestId(), command.getLeaseMs(), System.currentTimeMillis());

            long entryIndex = doPropose(new ProtostuffSerializer().serialize(stamped));
            logger.info("Node {} accepted lock op forwarding: requestId={}, lockName={}, entryIndex={}",
                nodeId, request.getRequestId(), command.getLockName(), entryIndex);
            return new LockOpResponse(request.getRequestId(), entryIndex > 0, entryIndex);
        } catch (SerializationException e) {
            logger.error("Node {} failed to decode lock command: requestId={}",
                nodeId, request.getRequestId(), e);
            return rejected;
        }
    }

    @Override
    public CompletableFuture<LockOpResponse> forwardLockOp(LockOpRequest request) {
        if (transportLayer == null) {
            return CompletableFuture.completedFuture(new LockOpResponse(request.getRequestId(), false, -1));
        }
        String leader = getLeaderId();
        if (leader == null || leader.equals(nodeId)) {
            // 无已知 Leader（选举中）或已恰好是 Leader：未来以"未收录"完成，调用方按需重试
            return CompletableFuture.completedFuture(new LockOpResponse(request.getRequestId(), false, -1));
        }
        return transportLayer.sendLockOp(leader, request);
    }

    private long getLastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();
    }

    private LogEntry getEntryAt(long index) {
        return logIndexMap.get(index);
    }

    /**
     * 启动成员变更检测任务
     */
    private void startMembershipChangeDetection() {
        if (!running || !membershipConfig.isMembershipChangeEnabled()) {
            return;
        }

        long checkInterval = membershipConfig.getHealthCheckInterval();
        membershipChangeFuture = executor.scheduleAtFixedRate(
            new cn.itcraft.speedboat.raft.task.MemberCheckTask(this::checkMembershipChanges),
            checkInterval,
            checkInterval
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
        return onRaftThread(() -> doProposeAddMember(newPeerId));
    }

    private boolean doProposeAddMember(String newPeerId) {
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
        logIndexMap.put(entry.getIndex(), entry);
        
        // 复制到所有节点
        replicateMemberChange(entry);
        
        logger.info("Leader {} proposed to add member {}", nodeId, newPeerId);
        return true;
    }
    
    /**
     * 提出移除成员
     */
    public boolean proposeRemoveMember(String peerId) {
        return onRaftThread(() -> doProposeRemoveMember(peerId));
    }

    private boolean doProposeRemoveMember(String peerId) {
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
        logIndexMap.put(entry.getIndex(), entry);
        
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

}