package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.config.SpeedboatConsts;
import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.raft.executor.RaftNodeExecutor;
import cn.itcraft.speedboat.statemachine.StateMachine;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
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

    cn.itcraft.speedboat.config.MembershipConfig getMembershipConfig();

    cn.itcraft.speedboat.strategy.membership.HealthCheckStrategy getHealthCheckStrategy();

    cn.itcraft.speedboat.strategy.membership.RegistryStrategy getRegistryStrategy();

    cn.itcraft.speedboat.strategy.membership.ChangeValidationStrategy getChangeValidationStrategy();

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
     * Raft 节点构建器（保持既有 {@code new RaftNode.Builder()} 调用兼容）。
     */
    class Builder {
        String nodeId;
        String datacenter;
        List<String> peerIds;
        ElectionTimeout electionTimeout;
        VoteWeightStrategy voteWeightStrategy;
        GroupStrategy groupStrategy;
        TransportLayer transportLayer;
        RaftNodeExecutor executor;
        int maxLogSize;
        cn.itcraft.speedboat.config.MembershipConfig membershipConfig;
        cn.itcraft.speedboat.strategy.membership.HealthCheckStrategy healthCheckStrategy;
        cn.itcraft.speedboat.strategy.membership.RegistryStrategy registryStrategy;
        cn.itcraft.speedboat.strategy.membership.ChangeValidationStrategy changeValidationStrategy;
        StateMachine stateMachine;
        cn.itcraft.speedboat.persistence.RaftStore raftStore;

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

        /**
         * 注入持久化存储（缺省 NopRaftStore 内存模式）。
         * 接口前置：生产 WAL / SQLite 引擎按 persistence/RaftStore 契约接入。
         */
        public Builder raftStore(cn.itcraft.speedboat.persistence.RaftStore raftStore) {
            this.raftStore = raftStore;
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
