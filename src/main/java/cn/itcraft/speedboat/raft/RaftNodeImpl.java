package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.config.SpeedboatConsts;
import cn.itcraft.speedboat.raft.executor.RaftNodeExecutor;
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
import cn.itcraft.speedboat.raft.report.RaftNodeReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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

    public cn.itcraft.speedboat.config.MembershipConfig getMembershipConfig() {
        return ctx.membershipConfig;
    }

    public cn.itcraft.speedboat.strategy.membership.HealthCheckStrategy getHealthCheckStrategy() {
        return ctx.healthCheckStrategy;
    }

    public cn.itcraft.speedboat.strategy.membership.RegistryStrategy getRegistryStrategy() {
        return ctx.registryStrategy;
    }

    public cn.itcraft.speedboat.strategy.membership.ChangeValidationStrategy getChangeValidationStrategy() {
        return ctx.changeValidationStrategy;
    }

    public java.util.concurrent.ConcurrentHashMap<String, FailureRecord> getFailureRecords() {
        return ctx.failureRecords;
    }

    public cn.itcraft.speedboat.statemachine.StateMachine getStateMachine() {
        return ctx.getStateMachine();
    }

    public void setStateMachine(cn.itcraft.speedboat.statemachine.StateMachine stateMachine) {
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
            new cn.itcraft.speedboat.raft.task.HeartbeatTask(this::sendHeartbeat),
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
            new cn.itcraft.speedboat.raft.task.MemberCheckTask(membership::checkMembershipChanges),
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
