package cn.itcraft.speedboat.raft;
import cn.itcraft.speedboat.config.MembershipConfig;
import cn.itcraft.speedboat.config.SpeedboatConsts;
import cn.itcraft.speedboat.raft.executor.RaftNodeExecutor;
import cn.itcraft.speedboat.raft.report.RaftNodeReport;
import cn.itcraft.speedboat.raft.task.HeartbeatTask;
import cn.itcraft.speedboat.raft.task.MemberCheckTask;
import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.rpc.LockOpRequest;
import cn.itcraft.speedboat.rpc.LockOpResponse;
import cn.itcraft.speedboat.rpc.PreVoteRequest;
import cn.itcraft.speedboat.rpc.PreVoteResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.statemachine.StateMachine;
import cn.itcraft.speedboat.strategy.consistency.ConsistencyPolicy;
import cn.itcraft.speedboat.strategy.membership.ChangeValidationStrategy;
import cn.itcraft.speedboat.strategy.membership.HealthCheckStrategy;
import cn.itcraft.speedboat.strategy.membership.RegistryStrategy;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * {@link RaftNode} 接口的唯一实现（门面协调器；结构手术重构版）。
 *
 * <p>本类不再承载算法本体——只做三件事：</p>
 * <ol>
 *   <li><b>布线</b>：构造期装配 13 个单一职责组件（见下表）；</li>
 *   <li><b>Actor 纪律收敛</b>：{@code onRaftThread / onRaftThreadAsync} 桥接，
 *       保证全部状态变更在 raft 单线程发生；</li>
 *   <li><b>公开 API 委派</b>： {@link RaftNode} 契约逐字不变，每个方法单行转发。</li>
 * </ol>
 *
 * <p>功能组件分工（vote"一个功能独立形成一个类"）：</p>
 * <table>
 *   <tr><th>类</th><th>唯一职责</th></tr>
 *   <tr><td>NodeContext</td><td>共享可变状态承载（tiny-rules RuleMgrState 样式）</td></tr>
 *   <tr><td>RoleMachine</td><td>角色迁移与入口副作用</td></tr>
 *   <tr><td>QuorumCalculator</td><td>权重法定多数纯函数</td></tr>
 *   <tr><td>RaftLogStore</td><td>日志仓：存储/裁剪/WAL 桥接/term 落盘</td></tr>
 *   <tr><td>Checkpointer</td><td>状态机检查点与恢复链</td></tr>
 *   <tr><td>ElectionCoordinator</td><td>选举主链（RequestVote/PreVote/超时/登基）</td></tr>
 *   <tr><td>HeartbeatWatchdog</td><td>check-quorum 降级判定</td></tr>
 *   <tr><td>ReplicationPump</td><td>出站复制泵与应答处理</td></tr>
 *   <tr><td>InboundAppendHandler</td><td>入站日志复制受理</td></tr>
 *   <tr><td>ApplyEngine</td><td>全序重放与 lastApplied 推进</td></tr>
 *   <tr><td>MembershipManager</td><td>成员变更检测/提案/应用</td></tr>
 *   <tr><td>CommandProposer</td><td>用户命令提案</td></tr>
 *   <tr><td>LockOpGateway</td><td>锁操作裁决/转发</td></tr>
 *   <tr><td>ReportPublisher</td><td>状态快照发布（项目 {@code raft/report} 事件通道）</td></tr>
 * </table>
 *
 * <p>节点状态机与核心算法语义（选举/复制/成员变更）、配置项说明与使用示例：见
 * {@link RaftNode}、{@code docs/deconstruct/*}。本类注释存档原实现的行为细节，
 * 组件内部保留原有注释（搬迁随行原则）。</p>
 *
 * @author speedboat
 * @see RaftNode
 * @see NodeContext
 * @since 1.0.0
 */
public class RaftNodeImpl implements RaftNode {

    private static final Logger logger = LoggerFactory.getLogger(RaftNodeImpl.class);

    /** 共享（可变）节点状态 */
    private final NodeContext ctx;
    /** 权重法定多数计算器 */
    private final QuorumCalculator quorum;
    /** Raft 日志仓（存储/裁剪/持久化桥接） */
    private final RaftLogStore logStore;
    /** 状态机检查点 */
    private final Checkpointer checkpointer;
    /** 全序重放引擎 */
    private final ApplyEngine applyEngine;
    /** 角色状态机 */
    private final RoleMachine roles;
    /** 选举协调器（RequestVote/PreVote/超时/登基） */
    private final ElectionCoordinator election;
    /** 心跳看门狗（check-quorum 主动降级） */
    private final HeartbeatWatchdog watchdog;
    /** 出站复制泵 */
    private final ReplicationPump pump;
    /** 入站日志复制受理 */
    private final InboundAppendHandler inboundAppend;
    /** 成员变更管理器 */
    private final MembershipManager membership;
    /** 命令提案器 */
    private final CommandProposer proposer;
    /** 锁操作网关 */
    private final LockOpGateway lockOps;
    /** 状态报告发布器 */
    private final ReportPublisher reporter;

    RaftNodeImpl(RaftNode.Builder builder) {
        this.ctx = new NodeContext(builder);
        this.quorum = new QuorumCalculator(ctx);
        this.logStore = new RaftLogStore(ctx, quorum);
        this.checkpointer = new Checkpointer(ctx);
        this.applyEngine = new ApplyEngine(ctx, checkpointer);
        this.roles = new RoleMachine(ctx, new RoleHooksInner());
        // 生命周期挂点同源布线（上策：Follower/Candidate/Leader 三态类进入副作用经此回收）
        roles.bind(new RoleLifecycleAdapter());
        this.watchdog = new HeartbeatWatchdog(ctx, quorum, roles);
        this.pump = new ReplicationPump(ctx, roles, quorum, logStore, watchdog, applyEngine);
        this.inboundAppend = new InboundAppendHandler(ctx, roles, logStore, applyEngine, this::resetElectionTimeout);
        this.membership = new MembershipManager(ctx, logStore, pump);
        this.proposer = new CommandProposer(ctx, logStore, pump::sendAppendEntries);
        this.lockOps = new LockOpGateway(ctx, proposer);
        this.reporter = new ReportPublisher(ctx);
        this.roles.bind(reporter);
        this.applyEngine.bind(logStore);
        this.applyEngine.bind(membership);
        // 阶段五：PRIORITY_CHANGE apply 落表后写父侧独立持久化（子组/单机房为 null，no-op）
        this.applyEngine.bindPriorityStore(ctx.priorityStore);
        // ActorActor 纪律：ElectionCoordinator 依赖 ApplyEngine（经 ApplyBridge 收敛，
        // 由门面两段布线避免构造顺序环）
        this.election = new ElectionCoordinator(ctx, roles, quorum, logStore, pump::sendAppendEntries);
        this.election.bind(applyEngine::applyCommittedEntries);
    }

    // ==================== 生命周期编排 ====================

    public void start() {
        if (ctx.running) {
            return;
        }
        ctx.running = true;
        ctx.executor.start();

        // 恢复链第一段：任期状态（先持久化后可见契约的恢复端）
        logStore.restoreTermIfPresent();

        // 恢复链第二段：WAL 读回链（defect-20260918-01 根治）——重启先重建日志，
        // 再把 commitIndex 交给 Leader 心跳推进；applyCommittedEntries 将从
        // lastApplied 全序重放 → lockTable/epoch 确定
        List<LogEntry> restoredLogs = ctx.raftStore.restoredLogEntries();
        if (!restoredLogs.isEmpty() && ctx.log.isEmpty()) {
            logStore.mirrorFromStore(restoredLogs);
        }

        // 恢复链第三段：状态机检查点——先落到 checkpoint 时刻的锁表，
        // 之后只重放 > lastApplied 的日志尾巴（epoch 租约等由检查点承载）
        long smApplied = checkpointer.restoreStateCheckpoint(ctx.getStateMachine());
        if (smApplied > 0) {
            long lastLogIdx = logStore.getLastLogIndex();
            if (smApplied <= lastLogIdx) {
                ctx.lastApplied = smApplied;
            } else {
                logger.warn("Node {} checkpoint appliedIndex {} beyond restored log ({}) — ignoring",
                    ctx.nodeId, smApplied, lastLogIdx);
            }
        }

        if (ctx.transportLayer != null) {
            // 入站消息全异步化：请求经 RpcBridge 投递到 raft 单线程（SC），
            // 应答经 CompletableFuture 由传输层异步回写（IO 线程不再互等）；
            // 每 RPC 一个 Handler 类（上策形态），算法本体在其目标组件。
            RpcBridge bridge = this::onRaftThreadAsync;
            new RequestVoteRpcHandler(ctx.transportLayer, election, bridge).register();
            new PreVoteRpcHandler(ctx.transportLayer, election, bridge).register();
            new AppendEntriesRpcHandler(ctx.transportLayer, inboundAppend, bridge).register();
            // 命名锁转发协议：Leader 侧裁决点（打点授权+propose）；非 Leader 侧直拒
            new LockOpRpcHandler(ctx.transportLayer, lockOps, bridge).register();
        }

        resetElectionTimeout();

        // 修复历史遗留：构造期 scheduler 尚未创建导致成员检测从未真正启动，
        // 现改为 start() 统一启动所有定时任务
        if (ctx.membershipConfig.isMembershipChangeEnabled()) {
            startMembershipChangeDetection();
        }

        // Phase E 周期兜底快照：每 10s 由 raft 线程发布一次状态视图
        ctx.executor.scheduleAtFixedRate(
            () -> reporter.publish(RaftNodeReport.ReportReason.PERIODIC),
            NodeContext.REPORT_PERIOD_MILLIS, NodeContext.REPORT_PERIOD_MILLIS);

        logger.info("Node {} started as {} on raft thread [{}]", ctx.nodeId, ctx.currentState, ctx.executor.isRaftThread());
    }

    public void shutdown() {
        if (!ctx.running) {
            return;
        }
        ctx.running = false;

        if (ctx.getElectionTimeoutFuture() != null) {
            ctx.getElectionTimeoutFuture().cancel(false);
        }
        if (ctx.getHeartbeatFuture() != null) {
            ctx.getHeartbeatFuture().cancel(false);
        }
        if (ctx.getMembershipChangeFuture() != null) {
            ctx.getMembershipChangeFuture().cancel(false);
        }
        // 优雅停机：先落一次检查点（若 store 支持）并 flush WAL 尾部，崩溃恢复失去"最顺手"锚点
        checkpointer.snapshotOnShutdown(ctx.getStateMachine());
        if (ctx.executor != null) {
            ctx.executor.shutdown();
        }
        logger.info("Node {} shutdown", ctx.nodeId);
    }

    // ==================== Actor 桥接 ====================

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
            if (ctx.executor.isRaftThread()) {
                return task.call();
            }
            return ctx.executor.submit(task).get(SpeedboatConsts.SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
        ctx.executor.execute(() -> {
            try {
                future.complete(task.call());
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    // ==================== 角色与公开 API 委派 ====================

    public void transitionTo(NodeState newState) {
        onRaftThread(() -> {
            roles.doTransitionTo(newState);
            return null;
        });
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        return onRaftThread(() -> election.doHandleRequestVote(request));
    }

    public HeartbeatResponse handleHeartbeat(HeartbeatRequest request) {
        return onRaftThread(() -> inboundAppend.handleHeartbeat(request));
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        return onRaftThread(() -> inboundAppend.handleAppendEntries(request));
    }

    public void startElection() {
        onRaftThread(() -> {
            election.doStartElection();
            return null;
        });
    }

    public void becomeLeader() {
        onRaftThread(() -> {
            election.doBecomeLeader();
            return null;
        });
    }

    public void sendHeartbeat() {
        pump.sendHeartbeat();
    }

    public void sendAppendEntries() {
        pump.sendAppendEntries();
    }

    public void resetElectionTimeout() {
        election.resetElectionTimeout();
    }

    @Override
    public void setSeatHeld(boolean seatHeld) {
        boolean previous = ctx.seatHeld;
        ctx.seatHeld = seatHeld;

        if (previous == seatHeld) {
            return;
        }

        if (!seatHeld) {
            // 失去代表资格：若此刻仍是本组 Leader 必须立即退位，
            // 否则会以"非代表"身份继续对外发心跳/复制日志，对外呈现一个假 Leader。
            // （跨机房父组下这正是"杀光主机房后备机房误判自己是全局主"的隐患）
            onRaftThread(() -> {
                if (!ctx.seatHeld && ctx.currentState == NodeState.LEADER) {
                    logger.info("Node {} lost representative seat, stepping down from LEADER", ctx.nodeId);
                    roles.doTransitionTo(NodeState.FOLLOWER);
                }
                return null;
            });
            logger.info("Node {} representative seat revoked", ctx.nodeId);
        } else {
            logger.info("Node {} representative seat granted", ctx.nodeId);
        }
    }

    @Override
    public boolean isSeatHeld() {
        return ctx.seatHeld;
    }

    public boolean isMain() {
        return roles.isMain();
    }

    public boolean isLeader() {
        return roles.isLeader();
    }

    public NodeState getCurrentState() {
        return ctx.currentState;
    }

    public Term getTerm() {
        return ctx.term;
    }

    public String getVotedFor() {
        return ctx.votedFor;
    }

    public void setVotedFor(String votedFor) {
        ctx.votedFor = votedFor;
    }

    public String getLeaderId() {
        return ctx.leaderId;
    }

    public String getNodeId() {
        return ctx.nodeId;
    }

    public long getCommitIndex() {
        return ctx.commitIndex;
    }

    public long getLastApplied() {
        return ctx.lastApplied;
    }

    public List<LogEntry> getLogEntries() {
        return Collections.unmodifiableList(new ArrayList<>(ctx.log));
    }

    public List<LeaderRecord> getLeaderHistory() {
        List<LeaderRecord> history = new ArrayList<>();
        for (LogEntry entry : ctx.log) {
            history.add(new LeaderRecord(entry.getTerm(), entry.getLeaderId()));
        }
        return history;
    }

    public List<String> getPeerIds() {
        return quorum.peerIds();
    }

    /**
     * 兼容保留：RaftGroup 门面引用。
     *
     * <p><b>注意：</b>返回 {@code null} 是刻意的设计结论，而非待完成的占位。
     * {@link RaftGroup} 是<b>单进程模拟</b>的组管理器（组内多个 RaftNode 共享进程、
     * 不接受 transport 参数），无法承载多进程部署下的跨机房级联。
     *
     * <p>跨机房级联的实际实现路径是：每个进程持有<b>两套</b>独立的
     * {@link RaftNode} —— 子组（机房内）与父组（跨机房）—— 各自绑定独立端口、
     * 独立 term 与独立持久化目录，由 {@code Speedboat} 门面编排。
     * 因此本方法不返回任何组实例。</p>
     *
     * @return 恒为 {@code null}
     */
    @Override
    public RaftGroup getGroup() {
        return null;
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
        quorum.setPeerDatacenter(peerId, datacenter);
    }

    public MembershipConfig getMembershipConfig() {
        return ctx.membershipConfig;
    }

    public HealthCheckStrategy getHealthCheckStrategy() {
        return ctx.healthCheckStrategy;
    }

    public RegistryStrategy getRegistryStrategy() {
        return ctx.registryStrategy;
    }

    public ChangeValidationStrategy getChangeValidationStrategy() {
        return ctx.changeValidationStrategy;
    }

    public java.util.concurrent.ConcurrentHashMap<String, FailureRecord> getFailureRecords() {
        return ctx.failureRecords;
    }

    public StateMachine getStateMachine() {
        return ctx.getStateMachine();
    }

    public void setStateMachine(StateMachine stateMachine) {
        ctx.setStateMachine(stateMachine);
    }

    /**
     * 提交一条命令到 Raft 日志（调用方经返回索引确认应用；-1 表示非 Leader/停机）。
     */
    public long propose(byte[] data) {
        try {
            return onRaftThread(() -> proposer.propose(data));
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // 节点已 shutdown、executor 已终止：propose 返回失败占位（-1）
            logger.warn("Node {} shutdown, propose rejected", ctx.nodeId);
            return -1;
        }
    }

    public boolean proposeAddMember(String newPeerId) {
        return onRaftThread(() -> membership.proposeAddMember(newPeerId));
    }

    public boolean proposeRemoveMember(String peerId) {
        return onRaftThread(() -> membership.proposeRemoveMember(peerId));
    }

    // ==================== 人工升级 API（阶段五运维兜底） ====================

    /**
     * 人工提升本机房在父组的优先级（阶段五；仅跨机房父组有意义）。
     *
     * <p>执行序（全部收敛到 raft 单线程内，见设计文档 §6）：</p>
     * <ol>
     *   <li><b>换表</b>：本机房权重 = Σ(其他机房权重) + 1，epoch+1，来源 PROMOTED；</li>
     *   <li><b>term 跃升</b>：默认 +{@code termLeap}（配置 promote.term.leap，缺省 100），
     *       使对侧旧任期在愈合后必然退让；</li>
     *   <li><b>登基</b>：自投（selfWeight ≥ requiredWeight）免广播直接成为 Leader；</li>
     *   <li><b>复制</b>：append + replicate 一条 PRIORITY_CHANGE 条目，自身权重已达标故
     *       本地即可推进提交；</li>
     *   <li><b>持久化</b>：写父侧独立文件（PriorityStore），重启不丢。</li>
     * </ol>
     *
     * <p><b>前置闸门不在本方法</b>——"对侧机房父组端点全部不可达"由 Speedboat 门面探测
     * （门面持有对侧端点集合）。本方法只做 raft 线程内状态变更，不复查网络。</p>
     *
     * <p><b>AP 模式闸门在本方法</b>：人工切主只对 CP 有意义，AP 下直接拒绝（仅留 WARN），
     * 不改任何状态。门面已先行忽略并打审计日志，此处是<b>纵深防御</b>——状态变更的真正
     * 咽喉点须自带防线，防止未来新增调用方绕过门面。</p>
     *
     * @param operator 操作者（审计必填；由门面校验非空）
     * @param termLeap term 跃升幅度（&gt;0；门面已按配置缺省补齐）
     * @param reason   变更原因（审计）
     * @return true 表示提升已在 raft 线程内生效并复制出去
     */
    @Override
    public boolean promoteDatacenter(String operator, long termLeap, String reason) {
        if (manualSwitchIgnoredInApMode()) {
            logger.warn("Node {} manual promote ignored: consistency policy is AP "
                + "(manual datacenter switch applies to CP only), operator={}, reason={}",
                ctx.nodeId, operator, reason);
            return false;
        }
        if (ctx.priorityTable == null) {
            logger.warn("Node {} promote rejected: no priority table (not a cross-datacenter parent group)",
                ctx.nodeId);
            return false;
        }
        if (!ctx.seatHeld) {
            logger.warn("Node {} promote rejected: not the datacenter representative (no seat)",
                ctx.nodeId);
            return false;
        }
        return onRaftThread(() -> doPromoteDatacenter(operator, termLeap, reason));
    }

    /**
     * 人工回退优先级表到配置派生值（阶段五；与提升走同一条复制+持久化+心跳传播路径）。
     *
     * <p>回退<b>不跃升 term、不夺主</b>——它只把本机房权重降回配置值。回退后主机房凭
     * 更高权重可在下一次选举夺回，但<b>不自动</b>发生：需主机房代表自身发起选举，
     * 或在 CP 下由 check-quorum 降级后自然触发。切勿把回退自动化。</p>
     *
     * <p>本机房已是父组 Leader 时才执行换表（否则拒绝：回退需要以 Leader 身份复制出去，
     * 否则对侧无法经心跳收到新表）。与提升一致，网络闸门由门面把守。</p>
     *
     * <p>AP 模式下与提升同样被忽略（纵深防御，见 {@link #promoteDatacenter}）。</p>
     *
     * @param operator 操作者（审计必填）
     * @param reason   变更原因（审计）
     * @return true 表示回退已生效并复制出去
     */
    @Override
    public boolean restoreDefaultPriorities(String operator, String reason) {
        if (manualSwitchIgnoredInApMode()) {
            logger.warn("Node {} manual restore ignored: consistency policy is AP "
                + "(manual datacenter switch applies to CP only), operator={}, reason={}",
                ctx.nodeId, operator, reason);
            return false;
        }
        if (ctx.priorityTable == null) {
            logger.warn("Node {} restore rejected: no priority table (not a cross-datacenter parent group)",
                ctx.nodeId);
            return false;
        }
        if (!ctx.seatHeld) {
            logger.warn("Node {} restore rejected: not the datacenter representative (no seat)",
                ctx.nodeId);
            return false;
        }
        if (ctx.currentState != NodeState.LEADER) {
            logger.warn("Node {} restore rejected: not parent leader (state={}); restore must run as leader "
                + "so the reverted table replicates outward", ctx.nodeId, ctx.currentState);
            return false;
        }
        return onRaftThread(() -> doRestoreDefaultPriorities(operator, reason));
    }

    /**
     * 当前父组优先级快照（epoch + 权重表 + 来源）；单机房/子组返回 null。
     * 任意线程可读（快照本身不可变）。
     */
    @Override
    public DatacenterPriorityTable.Snapshot getPrioritySnapshot() {
        DatacenterPriorityTable table = ctx.priorityTable;
        return table == null ? null : table.current();
    }

    /**
     * 人工切主是否应被忽略（仅 AP 模式忽略；CP 与未注入策略按缺省 CP 放行）。
     *
     * <p>直接读 {@code ctx.consistencyPolicy}——与选举/心跳消费的是同一实例，
     * 保证"模式判定"与"降级接管行为"不会口径分裂。</p>
     */
    private boolean manualSwitchIgnoredInApMode() {
        ConsistencyPolicy policy = ctx.consistencyPolicy;
        return policy != null
            && ConsistencyPolicy.MODE_AP
                .equals(policy.mode());
    }

    /**
     * 提升编排本体（raft 单线程内）。
     *
     * <p>权重数学：本机房取 {@code Σ(其他机房权重) + 1}，对任意机房数恒保证
     * {@code selfWeight ≥ requiredWeight} 自投即成主、对侧合计永远差一票——
     * 双主在结构上不可能（见设计文档 §5 与 computePromotedWeights javadoc）。</p>
     */
    private boolean doPromoteDatacenter(String operator, long termLeap, String reason) {
        DatacenterPriorityTable table = ctx.priorityTable;
        DatacenterPriorityTable.Snapshot before = table.current();
        long termBefore = ctx.term.getCurrent();

        // 1) 换表：本机房权重 = Σ(其他机房权重) + 1，epoch+1（任意机房数恒自投即成主，见其 javadoc）
        java.util.Map<String, Integer> newWeights = computePromotedWeights(before.weights());
        long newEpoch = before.epoch() + 1;
        table.replace(newEpoch, newWeights,
            DatacenterPriorityTable.Source.PROMOTED);

        // 2) term 跃升（先于登基：使对侧旧任期在愈合后必然退让）
        if (termLeap > 0) {
            ctx.term.updateIfHigher(ctx.term.getCurrent() + termLeap);
            logStore.persistTermState(ctx.term.getCurrent(), ctx.votedFor, ctx.leaderId);
        }

        // 3) 登基：已是 Leader（如重复调用）则跳过；否则 startElection 自投成主
        if (ctx.currentState != NodeState.LEADER) {
            election.doStartElection();
            if (ctx.currentState != NodeState.LEADER) {
                logger.error("Node {} failed to become leader after promote; state={}, epoch={} weights={}",
                    ctx.nodeId, ctx.currentState, newEpoch, newWeights);
                return false;
            }
        }

        // 4) append + replicate 一条 PRIORITY_CHANGE（自身权重已 ≥ required，本地即可推进提交）
        PriorityChangeEntry entry = PriorityChangeEntry.create(
            logStore.getLastLogIndex() + 1, ctx.term.getCurrent(), ctx.nodeId,
            newEpoch, newWeights, PriorityChangeEntry.SOURCE_PROMOTED, ctx.datacenter, operator, reason);
        logStore.appendEntry(entry);
        pump.advanceCommitIndex();
        pump.sendAppendEntries();

        // 5) 持久化（独立文件；失败仅 WARN，不影响已生效的内存表与复制）
        if (ctx.priorityStore != null) {
            ctx.priorityStore.save(table.current());
        }

        logger.warn("Node {} promoted datacenter {} epoch {} (was {}): term {} -> {}, weights {} -> {}, "
                + "operator={}, reason={}",
            ctx.nodeId, ctx.datacenter, newEpoch, before.epoch(), termBefore, ctx.term.getCurrent(),
            before.weights(), newWeights, operator, reason);
        return true;
    }

    /**
     * 回退编排本体（raft 单线程内）：以配置派生表为新快照、epoch+1、来源 CONFIG，
     * 走与提升相同的复制 + 持久化 + 心跳传播路径。
     */
    private boolean doRestoreDefaultPriorities(String operator, String reason) {
        DatacenterPriorityTable table = ctx.priorityTable;
        DatacenterPriorityTable.Snapshot before = table.current();

        java.util.Map<String, Integer> configWeights = table.configWeights();
        long newEpoch = before.epoch() + 1;
        table.replace(newEpoch, configWeights,
            DatacenterPriorityTable.Source.CONFIG);

        PriorityChangeEntry entry = PriorityChangeEntry.create(
            logStore.getLastLogIndex() + 1, ctx.term.getCurrent(), ctx.nodeId,
            newEpoch, configWeights, PriorityChangeEntry.SOURCE_CONFIG, ctx.datacenter, operator, reason);
        logStore.appendEntry(entry);
        // 回退后本机房权重可能低于 required，故不强推 commit；随心跳复制由多数派自然提交
        pump.sendAppendEntries();

        if (ctx.priorityStore != null) {
            ctx.priorityStore.save(table.current());
        }

        logger.warn("Node {} restored default priorities for datacenter {}: epoch {} (was {}), "
                + "weights {} -> {}, operator={}, reason={}",
            ctx.nodeId, ctx.datacenter, newEpoch, before.epoch(), before.weights(), configWeights,
            operator, reason);
        return true;
    }

    /**
     * 计算提升后的权重表：本机房 = Σ(其他机房权重) + 1，其余机房保持原值。
     *
     * <p>双机房下该公式与旧式同为"自投即成主"，仅快照值由 max+2 收窄为 Σ+1
     * （更贴近最小充分权重，避免配置值被历史提升层层抬高）。</p>
     */
    private java.util.Map<String, Integer> computePromotedWeights(
        java.util.Map<String, Integer> currentWeights) {
        java.util.Map<String, Integer> next = new java.util.LinkedHashMap<String, Integer>();
        if (currentWeights != null) {
            next.putAll(currentWeights);
        }
        int sumOther = 0;
        for (java.util.Map.Entry<String, Integer> e : next.entrySet()) {
            if (!e.getKey().equals(ctx.datacenter) && e.getValue() != null && e.getValue() > 0) {
                sumOther += e.getValue();
            }
        }
        next.put(ctx.datacenter, sumOther + 1);
        return next;
    }

    public java.util.concurrent.CompletableFuture<LockOpResponse> forwardLockOp(LockOpRequest request) {
        return lockOps.forwardLockOp(request);
    }

    /**
     * 锁操作 Leader 侧裁决入口（包内契约：与传输层 handler 同语义）。
     * 经 {@code onRaftThread} 收敛后交 {@code LockOpGateway.doHandleLockOp} 裁决。
     */
    LockOpResponse handleLockOp(LockOpRequest request) {
        return onRaftThread(() -> lockOps.doHandleLockOp(request));
    }

    // ==================== 定时任务武装（心跳节拍归门面） ====================

    /** 投票权重计算（RequestVote 受理方视角；纯函数归 QuorumCalculator，委派保留） */
    @Override
    public int calculateVoteWeight(RequestVoteRequest request) {
        return quorum.calculateVoteWeight(request);
    }

    /**
     * Leader 心跳武装（幂等：先取消既有心跳，防止双路径重复调度产生孤儿心跳）。
     * 入口副作用由 {@link RoleMachine.RoleHooks} 触发。
     */
    private void startHeartbeat() {
        if (!ctx.running) {
            return;
        }
        // 幂等武装：先取消既有心跳再调度，防止候选/登基双路径重复调度产生
        // "孤儿心跳"（旧 Future 被覆盖引用后永远无法取消，stepdown 后仍持续发包）
        cancelHeartbeat();

        long heartbeatInterval = ctx.groupStrategy != null && ctx.groupStrategy.getHeartbeatInterval() > 0
            ? ctx.groupStrategy.getHeartbeatInterval() : 50;

        ctx.setHeartbeatFuture(ctx.executor.scheduleAtFixedRate(
            new HeartbeatTask(this::sendHeartbeat),
            0,
            heartbeatInterval
        ));
    }

    private void cancelHeartbeat() {
        if (ctx.getHeartbeatFuture() != null) {
            ctx.getHeartbeatFuture().cancel(false);
            ctx.setHeartbeatFuture(null);
        }
    }

    private void startMembershipChangeDetection() {
        if (!ctx.running || !ctx.membershipConfig.isMembershipChangeEnabled()) {
            return;
        }

        long checkInterval = ctx.membershipConfig.getHealthCheckInterval();
        ctx.setMembershipChangeFuture(ctx.executor.scheduleAtFixedRate(
            new MemberCheckTask(membership::checkMembershipChanges),
            checkInterval,
            checkInterval
        ));
    }

    /** 停机时取消成员变更检测（shutdown 统一走 Future 取消，当前保留方法防过早 API 收口） */
    @SuppressWarnings("unused")
    private void stopMembershipChangeDetection() {
        if (ctx.getMembershipChangeFuture() != null) {
            ctx.getMembershipChangeFuture().cancel(false);
            ctx.setMembershipChangeFuture(null);
        }
    }

    /** 角色入口副作用（CANDIDATE 清票 + term 自增；LEADER 心跳武装；FOLLOWER 心跳撤销 + 超时重置）
     *  ——上策形态下此 hooks 仅作 lifecycle 未挂载前的兼容兜底（单测场景） */
    private class RoleHooksInner implements RoleMachine.RoleHooks {

        @Override
        public void onEnterLeader() {
            startHeartbeat();
        }

        @Override
        public void onEnterFollower() {
            cancelHeartbeat();
            resetElectionTimeout();
        }

        @Override
        public void onEnterCandidate() {
            ctx.votesReceived.clear();
            ctx.votedFor = ctx.nodeId;
            ctx.term.increment();
        }
    }

    /** 角色生命周期挂点（上策形态：显式接口实现，行为与原 hooks 逐字等价）
     *  注意：内部类方法与外部类同名，必须显式 {@code RaftNodeImpl.} 限定避免自递归 */
    private class RoleLifecycleAdapter implements RoleLifecycle {

        @Override
        public void startHeartbeat() {
            RaftNodeImpl.this.startHeartbeat();
        }

        @Override
        public void cancelHeartbeat() {
            RaftNodeImpl.this.cancelHeartbeat();
        }

        @Override
        public void resetElectionTimeout() {
            RaftNodeImpl.this.resetElectionTimeout();
        }
    }
}
