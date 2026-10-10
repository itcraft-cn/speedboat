package cn.itcraft.speedboat.raft;
import cn.itcraft.speedboat.config.MembershipConfig;
import cn.itcraft.speedboat.config.SpeedboatConsts;
import cn.itcraft.speedboat.persistence.PriorityStore;
import cn.itcraft.speedboat.persistence.RaftStore;
import cn.itcraft.speedboat.raft.executor.RaftNodeExecutor;
import cn.itcraft.speedboat.raft.report.RaftNodeReportListener;
import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.rpc.LockOpRequest;
import cn.itcraft.speedboat.rpc.LockOpResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.statemachine.StateMachine;
import cn.itcraft.speedboat.strategy.consistency.ConsistencyPolicy;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.strategy.leadership.LeadershipPolicy;
import cn.itcraft.speedboat.strategy.membership.ChangeValidationStrategy;
import cn.itcraft.speedboat.strategy.membership.HealthCheckStrategy;
import cn.itcraft.speedboat.strategy.membership.RegistryStrategy;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import cn.itcraft.speedboat.transport.TransportLayer;
import java.util.List;
/**
 * Raft 共识算法节点的公共 API 契约（Phase B API/Impl 分层）。
 *
 * <p>本接口是库对外暴露的唯一操作面（对标 MicroRaft API/Impl 分层思想）：
 * 使用方只 import 接口；具体实现由同包的 {@code RaftNodeImpl} 承担，
 * 通过 {@link #builder()} 一行构建。</p>
 *
 * <p><b>线程模型契约（铁律）：</b></p>
 * <ol>
 *   <li>所有状态变更（角色/term/日志/成员/commitIndex）只在 raft 单线程上发生，
 *       由 {@code RaftNodeExecutor} 保证任务串行与 happens-before；</li>
 *   <li>调用方可任意线程：同步方法（handle* / propose / transitionTo / 存取器）
 *       内部统一收敛到 raft 线程——已在 raft 线程则内联，否则投递并阻塞等待
 *       （有限时长，raft 线程本身永不被阻塞）；</li>
 *   <li>读取类方法（isLeader/getTerm/...）为 volatile 快照读，无阻塞。</li>
 * </ol>
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
 * <p>使用示例：</p>
 * <pre>{@code
 * // 单机房扁平模式
 * RaftNode node = new RaftNode.Builder()
 *     .nodeId("node-1")
 *     .peerIds(Arrays.asList("node-2", "node-3"))
 *     .electionTimeout(new ElectionTimeout(150, 300))
 *     .groupStrategy(new DefaultGroupStrategy())
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
 * @see RaftNodeImpl
 * @see RaftGroup
 * @since 1.1.0
 */
public interface RaftNode {

    /**
     * 创建 Builder 的静态入口（一行启动门面）。
     */
    static RaftNode.Builder builder() {
        return new RaftNode.Builder();
    }

    /** 启动节点：建立传输 handler、启动选举超时与成员检测等定时任务。幂等。 */
    void start();

    /** 优雅停机：取消定时任务并关闭执行器。幂等。 */
    void shutdown();

    /**
     * 角色状态转换（含 FOLLOWER/CANDIDATE/LEADER 附属动作）。
     *
     * @throws IllegalStateException 非法角色转换时
     */
    void transitionTo(NodeState newState);

    /** 投票请求处理（计算内部在 raft 线程串行；同步门面阻塞返回）。 */
    RequestVoteResponse handleRequestVote(RequestVoteRequest request);

    /** 心跳请求处理（内部折叠为空 AppendEntries 语义）。 */
    HeartbeatResponse handleHeartbeat(HeartbeatRequest request);

    /** 日志复制请求处理（校验日志匹配、冲突截断、commit 推进）。 */
    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request);

    /** 发起选举（仅 FOLLOWER/CANDIDATE 有效；已位于 raft 线程则立即执行）。 */
    void startElection();

    /** 登基为 Leader（仅 CANDIDATE 有效；内部在 raft 线程执行附属动作）。 */
    void becomeLeader();

    /** Leader 心跳 tick：折叠为向所有 peer 发送 AppendEntries。 */
    void sendHeartbeat();

    /** 向所有 peer 发送 AppendEntries（Leader 语义）。 */
    void sendAppendEntries();

    /** 选举超时任务重置（按 ElectionTimeout 随机化下一次 dispatch）。 */
    void resetElectionTimeout();

    /**
     * 设置本节点是否持有"代表席位"（跨机房级联父组专用）。
     *
     * <p>跨机房级联模式下，父组的投票成员是各机房的<b>子组 Leader</b>。一个进程内
     * 只有一个父组 RaftNode 实例，但它只有在本进程当选子组 Leader 时才代表本机房参与
     * 父组选举——否则该机房会有多份"代表"同时投票，父组成员数随子组切换漂移，
     * 多数派判定即失去意义。</p>
     *
     * <p><b>语义：</b></p>
     * <ul>
     *   <li>{@code false} → 不发起选举、拒绝投票请求；若此刻已是 LEADER 则退位为 FOLLOWER；</li>
     *   <li>{@code true}  → 恢复正常选举与投票参与。</li>
     * </ul>
     *
     * <p>单机房模式恒为 {@code true}（缺省值），引入本方法不改变既有行为。</p>
     *
     * @param seatHeld 是否持有代表席位
     */
    void setSeatHeld(boolean seatHeld);

    /** 本节点是否持有代表席位（跨机房级联父组用；单机房场景恒为 true）。 */
    boolean isSeatHeld();

    /**
     * 计算某条投票请求对应候选者的总权重（base + 策略加权）。
     */
    int calculateVoteWeight(RequestVoteRequest request);

    /** 是否为主节点（Leader 角色）。 */
    boolean isMain();

    /** 是否 Leader 角色。 */
    boolean isLeader();

    /** 获取当前 raft 分组信息（Phase 3 已实现占位）。 */
    RaftGroup getGroup();

    NodeState getCurrentState();

    Term getTerm();

    String getVotedFor();

    void setVotedFor(String votedFor);

    String getLeaderId();

    String getNodeId();

    long getCommitIndex();

    long getLastApplied();

    List<LogEntry> getLogEntries();

    List<LeaderRecord> getLeaderHistory();

    List<String> getPeerIds();

    void setPeerDatacenter(String peerId, String datacenter);

    MembershipConfig getMembershipConfig();

    HealthCheckStrategy getHealthCheckStrategy();

    RegistryStrategy getRegistryStrategy();

    ChangeValidationStrategy getChangeValidationStrategy();

    java.util.concurrent.ConcurrentHashMap<String, FailureRecord> getFailureRecords();

    StateMachine getStateMachine();

    void setStateMachine(StateMachine stateMachine);

    /**
     * 提交一条命令到 Raft 日志。
     *
     * <p>调用者应使用返回的日志索引配合 {@code waitForApply} 确认命令已被状态机应用。
     * 返回 -1 表示当前节点不是 Leader 或数据为空，命令未写入日志。</p>
     *
     * @param data 序列化后的命令数据
     * @return 命令在日志中的索引（≥1），或 -1 表示失败
     */
    long propose(byte[] data);

    /** 提出添加成员（Leader 语义；失败返回 false）。 */
    boolean proposeAddMember(String newPeerId);

    /** 提出移除成员（不能移除自身；失败返回 false）。 */
    boolean proposeRemoveMember(String peerId);

    /**
     * 人工提升<b>本机房</b>在父组中的优先级（阶段五人工升级 API；raft 线程内执行）。
     *
     * <p>仅跨机房级联父组有效（单机房/子组 {@code priorityTable} 为 null 时返回 false）。
     * 接受后：本机房权重抬升为 {@code Σ(其他机房权重) + 1}（对任意机房数恒使自投即成主且结构上排除双主）、
     * term 按 {@code termLeap} 跃升（主机房旧任期追不上）、随选举登基为父组 Leader、
     * 提出 {@code PRIORITY_CHANGE} 日志条目复制到可达 peer、并写入父侧独立持久化。</p>
     *
     * <p><b>前置校验归门面</b>（连通性闸门、operator 非空、目标机房为本机房、审计）；
     * 本方法只做 raft 线程内的状态变更，不再复查网络。</p>
     *
     * @param operator 操作者（审计必填）
     * @param termLeap term 跃升步长（须 &gt; 0）
     * @param reason   变更原因（审计）
     * @return true 表示已在父组内完成提升并登基为 Leader
     */
    boolean promoteDatacenter(String operator, long termLeap, String reason);

    /**
     * 回退到配置派生的默认优先级表（阶段五；raft 线程内执行）。
     *
     * <p>以配置权重为新快照、epoch 递增、来源置 CONFIG，并走同一条复制+持久化+心跳传播路径。
     * 回退后主机房凭更高权重可在<b>下一次</b>选举夺回，但不自动发生——需主机房代表自行发起选举。</p>
     *
     * @param operator 操作者（审计必填）
     * @param reason   回退原因（审计）
     * @return true 表示已完成回退并复制
     */
    boolean restoreDefaultPriorities(String operator, String reason);

    /**
     * 当前优先级快照（epoch / 权重表 / 来源）。
     *
     * @return 当前快照；单机房/子组（无优先级表）返回 null
     */
    DatacenterPriorityTable.Snapshot getPrioritySnapshot();

    /**
     * 锁操作转发（命名锁设计）：非 Leader 成员把序列化的 LockCommand
     * 经本方法送达当前 Leader propose。
     *
     * <p>调用线程任意（传输层全异步）；响应仅含"提案是否收录"（ok+entryIndex），
     * 权威判定由发起方等本地 apply 后读取。无 Leader 时未来以 ok=false 完成。</p>
     */
    java.util.concurrent.CompletableFuture<LockOpResponse> forwardLockOp(
        LockOpRequest request);

    /**
     * Raft 节点构建器（保持既有 {@code new RaftNode.Builder()} 调用兼容）。
     */
    class Builder {
        String nodeId;
        String datacenter;
        List<String> peerIds;
        ElectionTimeout electionTimeout;
        VoteWeightStrategy voteWeightStrategy;
        DatacenterPriorityTable priorityTable;
        PriorityStore priorityStore;
        ConsistencyPolicy consistencyPolicy;
        LeadershipPolicy leadershipPolicy;
        GroupStrategy groupStrategy;
        TransportLayer transportLayer;
        RaftNodeExecutor executor;
        int maxLogSize;
        int checkpointInterval;
        MembershipConfig membershipConfig;
        HealthCheckStrategy healthCheckStrategy;
        RegistryStrategy registryStrategy;
        ChangeValidationStrategy changeValidationStrategy;
        StateMachine stateMachine;
        RaftStore raftStore;
        RaftNodeReportListener reportListener;

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder datacenter(String datacenter) {
            this.datacenter = datacenter;
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

        /**
         * 注入机房优先级表（阶段五人工升级 API 用）。
         *
         * <p>与 {@link #voteWeightStrategy(VoteWeightStrategy)} 共享<b>同一实例</b>——策略每次
         * 权重计算读其最新快照；本表供 PRIORITY_CHANGE 应用分派与 RPC 传播读取。仅跨机房
         * 级联父组注入；子组与单机房路径不注入即为 null，行为逐字节不变。</p>
         *
         * @param priorityTable 机房优先级表
         * @return builder 自身
         */
        public Builder priorityTable(DatacenterPriorityTable priorityTable) {
            this.priorityTable = priorityTable;
            return this;
        }

        /**
         * 注入父侧独立优先级持久化（阶段五；与 {@code RaftStore} 分离，不动其接口）。
         *
         * <p>仅跨机房级联父组注入；子组与单机房路径不注入即为 null。PRIORITY_CHANGE 条目
         * 落表后写入该 store，父组启动时以其为初值覆盖配置派生表（重启不丢提升状态）。</p>
         *
         * @param priorityStore 父侧优先级持久化
         * @return builder 自身
         */
        public Builder priorityStore(PriorityStore priorityStore) {
            this.priorityStore = priorityStore;
            return this;
        }

        /**
         * 注入一致性策略（CP/AP）。缺省 CP（强一致唯一性）。
         *
         * <p>仅跨机房级联父组需要显式注入 AP；单机房与子组路径不注入即为 CP，
         * 行为与引入本策略前逐字节等价。</p>
         *
         * @param consistencyPolicy 一致性策略
         * @return builder 自身
         */
        public Builder consistencyPolicy(ConsistencyPolicy consistencyPolicy) {
            this.consistencyPolicy = consistencyPolicy;
            return this;
        }

        /**
         * 注入领袖优先级策略（三机房拓扑语义；缺省 peer 零行为变更）。
         *
         * <p>仅跨机房级联父组需要显式注入 dominant（"同城双备 + 异地灾备"必须指定
         * 主机房）；单机房与子组路径不注入即为 peer，sticky 行为与引入前逐字节等价。</p>
         *
         * @param leadershipPolicy 领袖优先级策略
         * @return builder 自身
         */
        public Builder leadershipPolicy(
            LeadershipPolicy leadershipPolicy) {
            this.leadershipPolicy = leadershipPolicy;
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

        /**
         * 注入自定义 raft 单消费者执行器（Actor 模型核心，可替换为
         * JCTools MPSC 队列实现）。缺省为单线程 Scheduled 调度器。
         */
        public Builder executor(RaftNodeExecutor executor) {
            this.executor = executor;
            return this;
        }

        public Builder maxLogSize(int maxLogSize) {
            this.maxLogSize = maxLogSize;
            return this;
        }

        /**
         * 状态机检查点触发阈值（applied 增量；缺省 1024）。
         * 仅对提供检查点路径的 RaftStore（Mmap 档）生效。
         */
        public Builder checkpointInterval(int checkpointInterval) {
            this.checkpointInterval = checkpointInterval;
            return this;
        }

        public Builder membershipConfig(MembershipConfig membershipConfig) {
            this.membershipConfig = membershipConfig;
            return this;
        }

        public Builder healthCheckStrategy(HealthCheckStrategy healthCheckStrategy) {
            this.healthCheckStrategy = healthCheckStrategy;
            return this;
        }

        public Builder registryStrategy(RegistryStrategy registryStrategy) {
            this.registryStrategy = registryStrategy;
            return this;
        }

        public Builder changeValidationStrategy(ChangeValidationStrategy changeValidationStrategy) {
            this.changeValidationStrategy = changeValidationStrategy;
            return this;
        }

        public Builder stateMachine(StateMachine stateMachine) {
            this.stateMachine = stateMachine;
            return this;
        }

        /**
         * 注入持久化存储（缺省 NopRaftStore 内存模式）。
         * 接口前置：生产 WAL / SQLite 引擎按 persistence/RaftStore 契约接入。
         */
        public Builder raftStore(RaftStore raftStore) {
            this.raftStore = raftStore;
            return this;
        }

        /**
         * 注册状态报告监听器（Phase E 可观测性）。
         * 核心算法零依赖 metrics 库：Micrometer 等通过本接口外挂接入。
         */
        public Builder reportListener(RaftNodeReportListener listener) {
            this.reportListener = listener;
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
            return new RaftNodeImpl(this);
        }
    }
}
