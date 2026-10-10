package cn.itcraft.speedboat.raft;
import cn.itcraft.speedboat.config.MembershipConfig;
import cn.itcraft.speedboat.config.SpeedboatConsts;
import cn.itcraft.speedboat.persistence.NopRaftStore;
import cn.itcraft.speedboat.persistence.PriorityStore;
import cn.itcraft.speedboat.persistence.RaftStore;
import cn.itcraft.speedboat.raft.RaftNode.Builder;
import cn.itcraft.speedboat.raft.executor.DefaultRaftNodeExecutor;
import cn.itcraft.speedboat.raft.executor.RaftNodeExecutor;
import cn.itcraft.speedboat.raft.report.RaftNodeReportListener;
import cn.itcraft.speedboat.statemachine.StateMachine;
import cn.itcraft.speedboat.strategy.consistency.ConsistencyPolicy;
import cn.itcraft.speedboat.strategy.consistency.CpConsistencyPolicy;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.strategy.leadership.LeadershipPolicy;
import cn.itcraft.speedboat.strategy.membership.ChangeValidationStrategy;
import cn.itcraft.speedboat.strategy.membership.HealthCheckStrategy;
import cn.itcraft.speedboat.strategy.membership.RegistryStrategy;
import cn.itcraft.speedboat.strategy.membership.impl.DefaultChangeValidationStrategy;
import cn.itcraft.speedboat.strategy.membership.impl.NoOpRegistryStrategy;
import cn.itcraft.speedboat.strategy.membership.impl.PassiveHealthCheckStrategy;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable;
import cn.itcraft.speedboat.strategy.voteweight.PriorityCodec;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import cn.itcraft.speedboat.transport.TransportLayer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger logger = LoggerFactory.getLogger(NodeContext.class);

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

    /**
     * 最近一次看到<b>对侧机房</b> peer 存活的 nanos 时间戳（AP 降级判定的 leader 侧信号源）。
     *
     * <p>由 {@code HeartbeatWatchdog.markResponse} 在对侧机房 peer 应答时刷新；
     * follower 侧改用 {@link #lastHeartbeatNanos}（父组 Leader 恒在对侧机房）。初始化为建上下文时刻，
     * 保证启动初期对侧尚未应答时不会被误判为"已静默达阈值"。</p>
     */
    volatile long lastOppositeSeenNanos;

    /** 投票权重策略（可空：null 视为均等策略） */
    final VoteWeightStrategy voteWeightStrategy;
    /**
     * 机房优先级表（阶段五人工升级 API 用；可空）。
     *
     * <p>仅跨机房级联父组注入。与投票策略共享<b>同一实例</b>——策略每次权重计算读其最新快照，
     * 本上下文供优先级变更应用（ApplyEngine 分派 PRIORITY_CHANGE）与 RPC 传播读取。</p>
     */
    final DatacenterPriorityTable priorityTable;
    /**
     * 父侧独立优先级持久化（阶段五；可空——子组/单机房无持久化）。
     * 与 {@code RaftStore} 分离，不动其接口与契约。
     */
    final PriorityStore priorityStore;
    /**
     * 一致性策略（CP/AP）。可空时在构造器兜底为 CP 单例，
     * 保证单机房与既有调用路径行为逐字节不变。
     */
    final ConsistencyPolicy consistencyPolicy;
    /**
     * 领袖优先级策略（三机房拓扑语义；缺省 peer 零行为变更）。
     * 父组 PreVote sticky 判定据此决定是否为权重严格更高的候选者让位。
     */
    final LeadershipPolicy leadershipPolicy;
    /** 分组策略（决定心跳节拍等集群拓扑行为） */
    final GroupStrategy groupStrategy;
    /** 传输层（启动时布线；可空便于单测） */
    TransportLayer transportLayer;

    /** peer 节点全集（成员变更可扩展；raft 单线程独占写） */
    /**
     * 父/子组成员表（跨机房级联时的 raft 线程内 add/remove；报告002 H2 修复）。
     * 用 CopyOnWriteArrayList 兜住 raft 单线程写与外部（观测/检视）线程
     * {@code getPeerIds()} 复制的并发边界——COW 迭代是弱一致安全快照。
     */
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

    /**
     * 【PERSISTENT】日志主体（append & rebuild 均经 raftStore.persistLogEntries）。
     *
     * <p>读写均在 raft 单线程内（Actor 纪律；RaftNodeReport 构造亦同步于此），
     * 无需 CopyOnWriteArrayList——外部点评修复：COW 的每次 append 都全量拷贝，
     * 高频提案下呈 O(n²)。无锁数组即正确形态。</p>
     */
    final List<LogEntry> log;

    /** 日志容量上限（裁剪由 RaftLogStore 负责） */
    final int maxLogSize;
    /** commitIndex（volatile 快照） */
    volatile long commitIndex;
    /**
     * lastApplied（volatile 快照）。
     *
     * <p>两套水位口径注释（外部点评修复）：raft 层本值覆盖<b>全部条目类型</b>；
     * 状态机内部的 lastAppliedIndex（LockStateMachine 仅在 COMMAND 分支推进）可能
     * 落后于本值（差值 = 已 apply 的非 COMMAND 条目数）。检查点快照以状态机口径
     * 写出（lockTable/epoch 全量承载真实状态），(smApplied, 本值] 区间条目在重启
     * 重放上由 {@link ApplyEngine} 的 missing-entry 静默跳过兜底——该跳过只能
     * 作为防御而非正确性来源；状态本身不依赖它（快照内容自足）。</p>
     */
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

    // ==================== 跨机房级联：代表席位门控 ====================

    /**
     * 是否持有"代表席位"（volatile 快照，外部线程可写）。
     *
     * <p>跨机房级联模式下，父组（跨机房层）的投票成员是<b>各机房的子组 Leader</b>。
     * 一个进程内只有一个父组 RaftNode 实例，但它<b>只有在本进程当选子组 Leader 时
     * 才有资格</b>代表本机房参与父组选举——否则该机房就有多份"代表"参与投票，
     * 父组成员数会随子组切换而漂移，多数派判定失去意义。</p>
     *
     * <p><b>门控语义：</b>席位未持有时，节点不发起选举、拒绝父组投票请求；
     * 若此刻已是 LEADER 则主动退位为 FOLLOWER（避免已失去代表资格却仍对外发心跳）。
     * 单机房模式下恒为 {@code true}，不影响既有行为。</p>
     *
     * <p>由 {@code Speedboat} 门面在子组角色变化时通过
     * {@code RaftNode.setSeatHeld(boolean)} 写入。</p>
     */
    volatile boolean seatHeld;

    NodeContext(Builder builder) {
        this.nodeId = builder.nodeId;
        this.datacenter = builder.datacenter != null ? builder.datacenter : "";
        this.currentState = NodeState.FOLLOWER;
        // 缺省持有席位：单机房/普通集群场景无需感知本标记，行为与引入前完全一致
        this.seatHeld = true;
        this.term = new Term();
        this.votedFor = null;
        this.votedForTerm = -1;
        this.leaderId = null;
        this.electionTimeout = builder.electionTimeout;
        this.voteWeightStrategy = builder.voteWeightStrategy;
        // 机房优先级表：与投票策略共享同一实例（父组布线；子组/单机房为 null，行为不变）
        this.priorityTable = builder.priorityTable;
        this.priorityStore = builder.priorityStore;
        // 一致性策略缺省 CP：未显式注入时走强一致唯一性，单机房/既有路径行为逐字节不变
        this.consistencyPolicy = builder.consistencyPolicy != null
            ? builder.consistencyPolicy
            : CpConsistencyPolicy.getInstance();
        // 领袖优先级策略缺省 peer：零行为变更（对等先到先得，sticky 照旧拦截）
        this.leadershipPolicy = builder.leadershipPolicy != null
            ? builder.leadershipPolicy
            : LeadershipPolicy.defaultPolicy();
        this.groupStrategy = builder.groupStrategy;
        this.transportLayer = builder.transportLayer;
        this.peerIds = builder.peerIds != null
            ? new java.util.concurrent.CopyOnWriteArrayList<>(builder.peerIds)
            : new java.util.concurrent.CopyOnWriteArrayList<>();
        this.peerDatacenters = new ConcurrentHashMap<>();
        this.votesReceived = new ConcurrentHashMap<>();
        this.lastHeartbeatNanos = System.nanoTime();
        this.lastOppositeSeenNanos = this.lastHeartbeatNanos;
        this.log = new ArrayList<>();
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        this.maxLogSize = builder.maxLogSize > 0 ? builder.maxLogSize : SpeedboatConsts.DEFAULT_MAX_LOG_SIZE;
        this.checkpointInterval = builder.checkpointInterval > 0 ? builder.checkpointInterval : Checkpointer.DEFAULT_CHECKPOINT_INTERVAL;

        // 成员变更初始化
        this.membershipConfig = builder.membershipConfig != null ? builder.membershipConfig : new MembershipConfig();
        this.healthCheckStrategy = builder.healthCheckStrategy != null ? builder.healthCheckStrategy
            : new PassiveHealthCheckStrategy();
        this.registryStrategy = builder.registryStrategy != null ? builder.registryStrategy
            : new NoOpRegistryStrategy();
        this.changeValidationStrategy = builder.changeValidationStrategy != null ? builder.changeValidationStrategy
            : new DefaultChangeValidationStrategy();
        this.stateMachine = builder.stateMachine;
        // Actor 执行器：默认单线程调度实现，可注入自定义实现（如 JCTools MPSC 版）
        this.executor = builder.executor != null ? builder.executor : new DefaultRaftNodeExecutor();
        // 持久化缺省 Nop：库级 Builder 保持"零副作用"基线（测试/嵌入式调用方显式选档）；
        // 应用门面（Speedboat）负责把生产默认装配为 mmap（跨进程重启挂回，选主安全性最全）。
        this.raftStore = builder.raftStore != null ? builder.raftStore : NopRaftStore.getInstance();
        this.reportListener = builder.reportListener;
        this.quorumCheckTimeoutMillis = DEFAULT_QUORUM_CHECK_TIMEOUT_MILLIS;
    }

    StateMachine getStateMachine() {
        return stateMachine;
    }

    void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    /**
     * 依远端携带的优先级快照收敛本节点优先级表（阶段五传播收敛公共入口）。
     *
     * <p>心跳（AppendEntries）与投票（RequestVote）两条传播路径共用本方法：仅当远端
     * epoch 高于本表当前 epoch 才采纳，构成"只升不降"——旧快照（含提升前的低优先级）
     * 永远无法回灌。无优先级表（子组/单机房）或远端未携带（epoch=0/空串）时 no-op，
     * 旧版本节点发送的 null/0 字段据此忽略，协议向后兼容。</p>
     *
     * <p>并发契约：仅限 raft 单线程调用（随入站消息处理触发）。</p>
     *
     * @param remoteEpoch   远端携带的 epoch
     * @param remoteWeights 远端携带的权重表编码（"dc=w,dc=w"）；可为 null
     */
    void adoptPrioritySnapshot(long remoteEpoch, String remoteWeights) {
        if (priorityTable == null || remoteEpoch <= 0
            || remoteWeights == null || remoteWeights.isEmpty()) {
            return;
        }
        Map<String, Integer> decoded = decodePriorityCached(remoteWeights);
        if (decoded.isEmpty()) {
            return;
        }
        // 来源推断：与配置派生表一致则视为 CONFIG，否则视为 PROMOTED
        DatacenterPriorityTable.Source source =
            decoded.equals(priorityTable.configWeights())
                ? DatacenterPriorityTable.Source.CONFIG
                : DatacenterPriorityTable.Source.PROMOTED;
        if (priorityTable.convergeIfNewer(remoteEpoch, decoded, source)) {
            logger.info("Node {} converged priority table from remote snapshot: epoch={}, weights={}",
                nodeId, remoteEpoch, decoded);
            if (priorityStore != null) {
                priorityStore.save(priorityTable.current());
            }
        }
    }

    /**
     * 依传播快照收敛本节点优先级表·解码缓存（review L8）。
     *
     * <p>epoch>0 后同一条心跳/投票每轮重复携带同一编码串（直至.term 跳变），
     * 缓存"编码串 → 解码结果"避免每 RPC split/parse。以编码串为缓存 key：
     * 换表后编码串改变自然失效，正确性与缓存互不耦合。仅 raft 单线程读写。</p>
     */
    private String cachedPriorityEncoded;
    private Map<String, Integer> cachedPriorityDecoded;

    private Map<String, Integer> decodePriorityCached(String encoded) {
        if (encoded.equals(cachedPriorityEncoded) && cachedPriorityDecoded != null) {
            return cachedPriorityDecoded;
        }
        Map<String, Integer> decoded =
            PriorityCodec.decode(encoded);
        cachedPriorityEncoded = encoded;
        cachedPriorityDecoded = decoded;
        return decoded;
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
