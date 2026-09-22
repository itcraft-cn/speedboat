package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.config.MembershipConfig;
import cn.itcraft.speedboat.config.SpeedboatConsts;
import cn.itcraft.speedboat.raft.executor.RaftNodeExecutor;
import cn.itcraft.speedboat.persistence.RaftStore;
import cn.itcraft.speedboat.raft.report.RaftNodeReportListener;
import cn.itcraft.speedboat.raft.RaftNode.Builder;
import cn.itcraft.speedboat.statemachine.StateMachine;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.strategy.membership.ChangeValidationStrategy;
import cn.itcraft.speedboat.strategy.membership.HealthCheckStrategy;
import cn.itcraft.speedboat.strategy.membership.RegistryStrategy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import cn.itcraft.speedboat.transport.TransportLayer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

/**
 * Raft 节点共享状态承载（内部协作器，非公开 API）。
 *
 * <p>样式对标 tiny-rules {@code RuleMgrState}：final 类 + 集中编排全部可变 /
 * 不可变节点状态，供 {@link RoleMachine}、{@code ElectionCoordinator}、
 * {@code ReplicationPump} 等功能组件单一渠道读写。组件之间不互相持有引用，
 * 一律经本上下文取协作对象（单向依赖，杜绝环）。</p>
 *
 * <p>并发契约：可变状态仅由 raft 单线程（{@code executor} 的消费线程）写入，
 * 外部线程只允许读 volatile 快照——沿用 Actor 纪律（任务串行 +
 * 任务间 happens-before）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class NodeContext {

    // ==================== 持久性等级标注（对标 MicroRaft [PERSISTENT] 自文档化）====================

    /** 【PERSISTENT】节点标识（启动期不可变） */
    final String nodeId;
    /** 【PERSISTENT】机房标识（启动期不可变；空串表示未知/同机房） */
    final String datacenter;
    /** 当前角色（volatile 快照供外部弱一致读） */
    volatile NodeState currentState;

    /** 【PERSISTENT】term / votedFor / leaderId（prior to visibility 经 raftStore.persistAndFlushTerm 落盘） */
    final Term term;
    /** 当前任期已投出的候选者（volatile 快照） */
    volatile String votedFor;
    /** 【PERSISTENT】term 内已投票保护（link 为 term 条件一部分；真正持久化以 raftStore 覆盖） */
    volatile long votedForTerm;
    /** 当前已知 Leader（volatile 快照） */
    volatile String leaderId;

    /** 选举超时随机化区间（复用同一实例重置取值） */
    final ElectionTimeout electionTimeout;
    /** 最近一次收到 leader 心跳的 nanos 时间戳（sticky/check-quorum 共用） */
    volatile long lastHeartbeatNanos;

    /** 投票权重策略（可空：null 视为均等策略） */
    final VoteWeightStrategy voteWeightStrategy;
    /** 分组策略（决定心跳节拍等集群拓扑行为） */
    final GroupStrategy groupStrategy;
    /** 传输层（启动时布线；可空便于单测） */
    TransportLayer transportLayer;

    /** peer 节点全集（成员变更可扩展；raft 单线程独占写） */
    final List<String> peerIds;
    /** 每个 peer 的机房标识，用于权重策略计算。未配置时默认空字符串（视为同机房） */
    final Map<String, String> peerDatacenters;
    /** 本 term 已收到的投票（含自己；Candidate 阶段有效） */
    final Map<String, Boolean> votesReceived;

    /** 运行标记（volatile；start/shutdown 切换） */
    volatile boolean running;

    /**
     * 选民/复制追踪状态 + 等待器注册表。
     * Raft 单消费者执行器（Actor 模型）。
     *
     * <p>所有状态变更只在该执行器的唯一线程上发生；外部线程（Netty IO、
     * 用户调用）仅允许投递任务或读取 volatile 快照。因此算法本体内部
     * 不再使用 synchronized——并发正确性由"任务串行 + 任务间 happens-before"
     * 保证（MicroRaft executor 契约的本地化）。</p>
     */
    final RaftNodeExecutor executor;
    /** 选举超时定时 Future（重置即先 cancel 后 schedule） */
    private Future<?> electionTimeoutFuture;
    /** 心跳周期 Future（幂等武装：先取消再调度） */
    private Future<?> heartbeatFuture;
    /** 成员健康检测周期 Future */
    private Future<?> membershipChangeFuture;

    // ==================== 日志仓（Phase 结构手术：行为归 RaftLogStore，状态归于此）====================

    /** 【PERSISTENT】日志主体（append & rebuild 均经 raftStore.persistLogEntries） */
    final List<LogEntry> log;

    /** 日志容量上限（裁剪由 RaftLogStore 负责） */
    final int maxLogSize;
    /** commitIndex（volatile 快照） */
    volatile long commitIndex;
    /** lastApplied（volatile 快照） */
    volatile long lastApplied;
    /** 【NOT-PERSISTENT】nextIndex 属 leader 活性状态，重启后重新探测 */
    final Map<String, Long> nextIndex;
    /** 【NOT-PERSISTENT】matchIndex 属 leader 活性状态，重启后重新探测 */
    final Map<String, Long> matchIndex;

    // ==================== 成员变更子系统状态 ====================

    /** 成员变更配置 */
    final MembershipConfig membershipConfig;
    /** 成员健康检测策略 */
    final HealthCheckStrategy healthCheckStrategy;
    /** 注册中心策略 */
    final RegistryStrategy registryStrategy;
    /** 成员变更校验策略 */
    final ChangeValidationStrategy changeValidationStrategy;
    /** 各 peer 故障记录（连续失败计数 + 提议移除判定） */
    final ConcurrentHashMap<String, FailureRecord> failureRecords = new ConcurrentHashMap<>();

    /** 业务状态机（锁表/epoch 等 apply 宿主；可空便于无状态机测试） */
    private StateMachine stateMachine;

    /**
     * 持久化存储（缺省 NopRaftStore 内存模式）。
     * 接口前置：将来接入磁盘 WAL 时不需要改算法代码。
     */
    final RaftStore raftStore;

    /**
     * 状态报告监听器（Phase E 可观测性；可能为 null）。
     * 事件触发 + 周期兜底双通道发布（对标 MicroRaft RaftNodeReport 机制）。
     */
    final RaftNodeReportListener reportListener;
    /** 周期性状态快照发布间隔（毫秒），对标 MicroRaft raftNodeReportPublishPeriodSecs */
    static final long REPORT_PERIOD_MILLIS = 10_000L;

    // ==================== Phase C：预投票 & check-quorum ====================

    /** 预投票探测中标记（raft 单线程读写） */
    volatile boolean prevoting;
    /** 本轮预投票已收集的 peer（含自己） */
    final HashSet<String> prevotesReceived = new HashSet<>();
    /** check-quorum：最近一次各 peer 心跳响应时间戳（nanos 时间基） */
    final ConcurrentHashMap<String, Long> lastResponseNanos = new ConcurrentHashMap<>();

    /**
     * check-quorum 心跳新鲜窗口（毫秒）。
     *
     * <p>对标 MicroRaft leaderHeartbeatTimeoutPeriodSecs（默认 10s）：
     * 该窗口远大于选举超时/心跳周期——多数派侧会先选出更高任期 leader
     * 并把孤岛 leader"打散"，check-quorum 仅作为长分区的兜底降级，
     * 而非瞬态抖动的第一反应，避免误杀合法 Leader。</p>
     */
    static final long DEFAULT_QUORUM_CHECK_TIMEOUT_MILLIS = 5000;
    /** check-quorum 新鲜窗口（由心跳 watchdog 消费） */
    final long quorumCheckTimeoutMillis;

    // ==================== 状态机检查点 ====================

    /** 状态机检查点触发阈值（applied 增量；Builder.checkpointInterval / raft.checkpoint.interval 可配） */
    final int checkpointInterval;
    /** 触发水位：最近一次检查点覆盖的 appliedIndex */
    long lastCheckpointApplied = 0L;

    NodeContext(Builder builder) {
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
        this.checkpointInterval = builder.checkpointInterval > 0 ? builder.checkpointInterval : Checkpointer.DEFAULT_CHECKPOINT_INTERVAL;

        // 成员变更初始化
        this.membershipConfig = builder.membershipConfig != null ? builder.membershipConfig : new MembershipConfig();
        this.healthCheckStrategy = builder.healthCheckStrategy != null ? builder.healthCheckStrategy
            : new cn.itcraft.speedboat.strategy.membership.impl.PassiveHealthCheckStrategy();
        this.registryStrategy = builder.registryStrategy != null ? builder.registryStrategy
            : new cn.itcraft.speedboat.strategy.membership.impl.NoOpRegistryStrategy();
        this.changeValidationStrategy = builder.changeValidationStrategy != null ? builder.changeValidationStrategy
            : new cn.itcraft.speedboat.strategy.membership.impl.DefaultChangeValidationStrategy();
        this.stateMachine = builder.stateMachine;
        // Actor 执行器：默认单线程调度实现，可注入自定义实现（如 JCTools MPSC 版）
        this.executor = builder.executor != null ? builder.executor : new cn.itcraft.speedboat.raft.executor.DefaultRaftNodeExecutor();
        // 持久化缺省 Nop：库级 Builder 保持"零副作用"基线（测试/嵌入式调用方显式选档）；
        // 应用门面（Speedboat）负责把生产默认装配为 mmap（跨进程重启挂回，选主安全性最全）。
        this.raftStore = builder.raftStore != null ? builder.raftStore : cn.itcraft.speedboat.persistence.NopRaftStore.getInstance();
        this.reportListener = builder.reportListener;
        this.quorumCheckTimeoutMillis = DEFAULT_QUORUM_CHECK_TIMEOUT_MILLIS;
    }

    StateMachine getStateMachine() {
        return stateMachine;
    }

    void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    Future<?> getElectionTimeoutFuture() {
        return electionTimeoutFuture;
    }

    void setElectionTimeoutFuture(Future<?> future) {
        this.electionTimeoutFuture = future;
    }

    Future<?> getHeartbeatFuture() {
        return heartbeatFuture;
    }

    void setHeartbeatFuture(Future<?> future) {
        this.heartbeatFuture = future;
    }

    Future<?> getMembershipChangeFuture() {
        return membershipChangeFuture;
    }

    void setMembershipChangeFuture(Future<?> future) {
        this.membershipChangeFuture = future;
    }
}
