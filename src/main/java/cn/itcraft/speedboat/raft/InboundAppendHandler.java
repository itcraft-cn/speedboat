package cn.itcraft.speedboat.raft;
import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.strategy.leadership.LeadershipPolicy;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 入站日志复制处理器（内部协作器，非公开 API）。
 *
 * <p>职责唯一：Follower 侧的 AppendEntries/Heartbeat 受理——任期校验、
 * prevLogIndex/prevLogTerm 一致性检查、冲突覆写、leaderCommit 推进。</p>
 *
 * <p>职责边界：物理追加/持久化归 {@link RaftLogStore}；全序重放归
 * {@code ApplyEngine}（经 {@code applyFlow} 挂点，避免依赖环）。</p>
 *
 * <p>并发契约：仅限 raft 单线程调用。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class InboundAppendHandler {

    private static final Logger logger = LoggerFactory.getLogger(InboundAppendHandler.class);

    private final NodeContext ctx;
    private final RoleMachine roles;
    private final RaftLogStore logStore;
    private final ApplyEngine applyEngine;
    /** 选举超时重武装（由 ElectionCoordinator 提供；保持 resetElectionTimeout 单实现） */
    private final Runnable resetElectionTimeout;

    InboundAppendHandler(NodeContext ctx, RoleMachine roles, RaftLogStore logStore, ApplyEngine applyEngine,
                         Runnable resetElectionTimeout) {
        this.ctx = ctx;
        this.roles = roles;
        this.logStore = logStore;
        this.applyEngine = applyEngine;
        this.resetElectionTimeout = resetElectionTimeout;
    }

    /**
     * Heartbeat 即空负载 AppendEntries 的语义终映（保持原实现委托关系）。
     */
    HeartbeatResponse handleHeartbeat(HeartbeatRequest request) {
        AppendEntriesRequest appendRequest = AppendEntriesRequest.heartbeat(
            request.getTerm(), request.getLeaderId(), 0, 0, 0
        );
        AppendEntriesResponse response = handleAppendEntries(appendRequest);
        return new HeartbeatResponse(response.getTerm(), response.isSuccess());
    }

    /**
     * AppendEntries 受理主链（raft 单线程内）：
     * <ol>
     *   <li>低 term 拒绝（回已方 lastLogIndex 供 leader 回退）；</li>
     *   <li>高 term 对齐 + 清票 + term 落盘；</li>
     *   <li>非 FOLLOWER 角色收敛 + 记录 leader + 重置选举窗口；</li>
     *   <li>prevLog 一致性检查（不满足→拒绝，返回 lastLogIndex 供回退）；</li>
     *   <li>冲突覆写 + WAL 持久化 + 容量裁剪；</li>
     *   <li>leaderCommit 推进 → 全序重放。</li>
     * </ol>
     */
    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        if (request.getTerm() < ctx.term.getCurrent()) {
            logger.info("Rejecting AppendEntries from {} with lower term {}",
                request.getLeaderId(), request.getTerm());
            return new AppendEntriesResponse(ctx.term.getCurrent(), false, logStore.getLastLogIndex());
        }

        if (request.getTerm() > ctx.term.getCurrent()) {
            ctx.term.updateIfHigher(request.getTerm());
            ctx.votedFor = null;
            ctx.votedForTerm = -1;
            logStore.persistTermState(ctx.term.getCurrent(), ctx.votedFor, ctx.leaderId);
        }

        if (ctx.currentState != NodeState.FOLLOWER) {
            roles.doTransitionTo(NodeState.FOLLOWER);
        }

        ctx.leaderId = request.getLeaderId();
        ctx.lastHeartbeatNanos = System.nanoTime();
        // 心跳冻结选举定时器（Raft 原生）与 dominant 夺主权例外（策略化）：
        // 三机房 dominant 下，若本机房权重严格高于现任 Leader 机房，本节点持有
        // "夺主探测权"——父组选举定时器不被在位心跳 reset（否则在位主永远健康、
        // 选举定时器永不到期，sticky 让位虽生效却无发起时机），定时器照常到期、
        // doStartPreVote 以 probe term 反复探测直至接管完成。
        // peer（缺省）：恒冻结，与引入策略前逐字节等价。
        LeadershipPolicy leadership = ctx.leadershipPolicy;
        // 现任 Leader 机房：peer 登记查询（非本机；本机即 Leader 的分支在下方 selfIsLeaderDc 处理）
        String leaderDc = ctx.peerDatacenters.get(request.getLeaderId());
        boolean selfIsLeaderDc = ctx.nodeId.equals(request.getLeaderId());
        if (selfIsLeaderDc
            || leadership.freezeHeartbeatElectionTimer(leaderDc, ctx.datacenter)) {
            resetElectionTimeout.run();
        }

        // 阶段五：心跳携带的优先级快照先行收敛（epoch 更高才采纳，与日志一致性检查无关），
        // 使提升后的优先级表能随父组心跳传播到全网。
        ctx.adoptPrioritySnapshot(request.getPriorityEpoch(), request.getPriorityWeights());

        logger.debug("Follower {} received appendEntries from leader {}, entries={}, prevLogIndex={}, leaderCommit={}",
            ctx.nodeId, request.getLeaderId(), request.getEntries().size(), request.getPrevLogIndex(), request.getLeaderCommit());

        if (request.getPrevLogIndex() > 0) {
            if (logStore.getLastLogIndex() < request.getPrevLogIndex()) {
                logger.info("Follower {} log too short: {} < prevLogIndex {}",
                    ctx.nodeId, logStore.getLastLogIndex(), request.getPrevLogIndex());
                // 返回 expectedNextIndex 快速回退提示：首条日志 +1（日志太短 → leader
                // 应从缺口的头部整段重发，替代原"返回 lastLogIndex 逐条回退"的慢路径；
                // leader 侧仍按 ≤ current-1 钳制保证安全）
                return new AppendEntriesResponse(ctx.term.getCurrent(), false, logStore.getLastLogIndex(),
                    logStore.getFirstLogIndex() + 1);
            }

            LogEntry prevEntry = logStore.getEntryAt(request.getPrevLogIndex());
            if (prevEntry == null || prevEntry.getTerm() != request.getPrevLogTerm()) {
                logger.info("Follower {} log term mismatch at index {}, localTerm={}, expectedTerm={}",
                    ctx.nodeId, request.getPrevLogIndex(),
                    prevEntry != null ? prevEntry.getTerm() : "null",
                    request.getPrevLogTerm());
                // 快速回退：计算本地冲突任期区间的起始 index 供 leader 一步对齐；
                // 这是标准 Raft 逐条回退的优化（SOFAJRaft/Ratis 同款协议扩展）
                return new AppendEntriesResponse(ctx.term.getCurrent(), false, logStore.getLastLogIndex(),
                    logStore.conflictNextIndex(request.getPrevLogIndex(), request.getPrevLogTerm()));
            }
        }

        if (!request.getEntries().isEmpty()) {
            logStore.applyFollowerAppend(request);
        }

        if (request.getLeaderCommit() > ctx.commitIndex) {
            long oldCommitIndex = ctx.commitIndex;
            applyEngine.advanceToCommit(Math.min(request.getLeaderCommit(), logStore.getLastLogIndex()));
            logger.debug("Node {} commitIndex updated: {} -> {}, lastApplied={}",
                ctx.nodeId, oldCommitIndex, ctx.commitIndex, ctx.lastApplied);
        }

        return new AppendEntriesResponse(ctx.term.getCurrent(), true, logStore.getLastLogIndex());
    }

    /** 快照供门面 getLogEntries（不可变视图转发） */
    List<LogEntry> snapshotLogEntries() {
        return logStore.getLogEntries();
    }
}