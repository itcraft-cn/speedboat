package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable;
import cn.itcraft.speedboat.strategy.voteweight.PriorityCodec;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 复制泵（内部协作器，非公开 API）。
 *
 * <p>职责唯一：Leader 侧出站复制——心跳节拍、按 peer nextIndex 构造
 * AppendEntriesRequest、应答处理（matchIndex/nextIndex 维护 + 失配回退 hint）、
 * commitIndex 推进判定。</p>
 *
 * <p>分工边界：check-quorum 降级判定归 {@link HeartbeatWatchdog
 * 一致性受理归 {@code InboundAppendHandler}；全序重放归 {@code ApplyEngine}。</p>
 *
 * <p>并发契约：出站方法允许 raft 单线程触发；应答经 executor 搬回 raft 线程。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class ReplicationPump {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationPump.class);

    private final NodeContext ctx;
    private final RoleMachine roles;
    private final QuorumCalculator quorum;
    private final RaftLogStore logStore;
    private final HeartbeatWatchdog watchdog;
    private final ApplyEngine applyEngine;

    ReplicationPump(NodeContext ctx, RoleMachine roles, QuorumCalculator quorum,
                    RaftLogStore logStore, HeartbeatWatchdog watchdog, ApplyEngine applyEngine) {
        this.ctx = ctx;
        this.roles = roles;
        this.quorum = quorum;
        this.logStore = logStore;
        this.watchdog = watchdog;
        this.applyEngine = applyEngine;
    }

    /**
     * 心跳节拍（HeartbeatTask 宿主入口）：
     * Phase C check-quorum 先行——多数派心跳响应超时则主动降级（防分区脑裂窗口），随后广播。
     */
    void sendHeartbeat() {
        watchdog.checkQuorumMaybeDemote();
        sendAppendEntries();
    }

    /** 全员广播 AppendEntries（logSize/commitIndex 供排障快照） */
    void sendAppendEntries() {
        if (ctx.currentState != NodeState.LEADER || ctx.transportLayer == null) {
            // 报告002 M-3：非 Leader 时每次复制轮询都会经过本路径，INFO 高频刷屏降 DEBUG
            logger.debug("Node {} not leader ({}) or transportLayer null ({}), cannot send append entries",
                ctx.nodeId, ctx.currentState == NodeState.LEADER, ctx.transportLayer != null);
            return;
        }

        logger.debug("Leader {} sending append entries to {} peers, logSize={}, commitIndex={}",
            ctx.nodeId, ctx.peerIds.size(), logStore.getLastLogIndex(), ctx.commitIndex);
        for (String peerId : ctx.peerIds) {
            sendAppendEntriesToPeer(peerId);
        }

        logger.debug("Leader {} sent append entries to {} peers", ctx.nodeId, ctx.peerIds.size());
    }

    /**
     * 对单 peer 构造 AppendEntries（nextIndex→prevLog 探测头 + 增量 payload），
     * 应答经 executor 搬回 raft 线程处理。
     */
    private void sendAppendEntriesToPeer(String peerId) {
        long nextIdx = ctx.nextIndex.getOrDefault(peerId, 1L);
        long prevLogIndex = nextIdx - 1;
        long prevLogTerm = logStore.getEntryTerm(prevLogIndex);

        // 外部点评修复：按下标切除（日志 index 连续：log[i].getIndex() == i + 1），
        // 心跳对每 peer 都是 O(1) 视图 + O(尾段) 复制，替换原先的 O(log) 全遍历。
        List<LogEntry> entriesToSend = tailFrom(ctx.log, nextIdx);

        logger.debug("Leader {} sending to peer {}: nextIdx={}, logSize={}, entriesToSend={}, commitIndex={}",
            ctx.nodeId, peerId, nextIdx, ctx.log.size(), entriesToSend.size(), ctx.commitIndex);

        AppendEntriesRequest request = new AppendEntriesRequest(
            ctx.term.getCurrent(), ctx.nodeId, prevLogIndex, prevLogTerm, entriesToSend, ctx.commitIndex
        );
        // 阶段五：心跳/复制载体附带优先级快照（epoch + 权重表），接收方仅当 epoch 更高才采纳
        attachPrioritySnapshot(request);

        logger.debug("Leader {} calling transportLayer.sendAppendEntries to peer {}, transportLayer={}",
            ctx.nodeId, peerId, ctx.transportLayer.getClass().getName());
        ctx.transportLayer.sendAppendEntries(peerId, request)
            .thenAccept(response -> ctx.executor.execute(
                () -> doHandleAppendEntriesResponse(peerId, response)));
    }

    /**
     * 出站 AppendEntries 附带优先级快照（阶段五传播载体）。
     *
     * <p>无优先级表（子组/单机房）或快照 epoch 为 0（从未变更）时不附带，
     * 旧版本节点收到 null/0 字段据此忽略，协议向后兼容。</p>
     */
    private void attachPrioritySnapshot(AppendEntriesRequest request) {
        DatacenterPriorityTable table = ctx.priorityTable;
        if (table == null) {
            return;
        }
        DatacenterPriorityTable.Snapshot snapshot = table.current();
        if (snapshot.epoch() <= 0) {
            return;
        }
        request.setPrioritySnapshot(snapshot.epoch(),
            PriorityCodec.encode(snapshot.weights()));
    }

    /**
     * 应答处理（raft 单线程内）：
     * <ul>
     *   <li>更高 term → 对齐并降级；</li>
     *   <li>非 Leader 忽略；</li>
     *   <li>成功 → 刷新响应新鲜度 + matchIndex/nextIndex 前进 → 推进 commitIndex；</li>
     *   <li>失败 → 失配回退（Raft §5.3）：hint=matchIndex+1 为下界；每轮至少回退 1。
     *       仅取 hint 会在"同索引不同 term"的分割日志（两任 leader 写同一 index）下
     *       恒等于当前 nextIndex，prevLog 探测死循环、日志永不收敛。</li>
     * </ul>
     */
    void doHandleAppendEntriesResponse(String peerId, AppendEntriesResponse response) {
        logger.debug("Leader {} received appendEntries response from {}: success={}, matchIndex={}",
            ctx.nodeId, peerId, response.isSuccess(), response.getMatchIndex());

        if (response.getTerm() > ctx.term.getCurrent()) {
            ctx.term.updateIfHigher(response.getTerm());
            roles.doTransitionTo(NodeState.FOLLOWER);
            return;
        }

        if (ctx.currentState != NodeState.LEADER) {
            logger.info("Node {} is not leader anymore, current state={}", ctx.nodeId, ctx.currentState);
            return;
        }

        if (response.isSuccess()) {
            watchdog.markResponse(peerId);
            ctx.matchIndex.put(peerId, response.getMatchIndex());
            ctx.nextIndex.put(peerId, response.getMatchIndex() + 1);
            logger.debug("Leader {} matchIndex for {} updated to {}, matchIndex={}",
                ctx.nodeId, peerId, response.getMatchIndex(), ctx.matchIndex);
            advanceCommitIndex();
        } else {
            // 失配回退（Raft §5.3）：
            //   1) 优先取接收方的 expectedNextIndex 快速回退（SOFAJRaft/Ratis 同款扩展）；
            //   2) 无提示时退回 hint=matchIndex+1 为下界；每轮至少回退 1；
            //   3) 仅取 hint/expected 会在"同索引不同 term"的分割日志（两任 leader 写同一
            //      index）下恒等于当前 nextIndex、prevLog 探测死循环——故 end 强制钳制 ≤
            //      current - 1，保证严格递减、永不卡死。
            long current = ctx.nextIndex.getOrDefault(peerId, 1L);
            long expected = response.getExpectedNextIndex() > 0
                ? response.getExpectedNextIndex()
                : response.getMatchIndex() + 1;
            long newNextIndex = Math.max(1, Math.min(expected, current - 1));
            ctx.nextIndex.put(peerId, newNextIndex);
            logger.debug("Leader {} decrementing nextIndex for {} to {} (expected={}, hint={}, current={})",
                ctx.nodeId, peerId, newNextIndex, response.getExpectedNextIndex(), response.getMatchIndex() + 1, current);
        }
    }

    /**
     * commitIndex 推进（权重法定多数版，原 advanceCommitIndex 归一）：
     * 从 lastLogIndex 降序扫描，条目属当前任期且 matchedWeight ≥ requiredWeight 时
     * 推进并触发全序重放（一次推进即 break，保持原线性判定）。
     */
    void advanceCommitIndex() {
        logger.debug("Leader {} advanceCommitIndex: lastLogIndex={}, commitIndex={}",
            ctx.nodeId, logStore.getLastLogIndex(), ctx.commitIndex);
        for (long n = logStore.getLastLogIndex(); n > ctx.commitIndex; n--) {
            LogEntry entry = logStore.getEntryAt(n);
            if (entry != null && entry.getTerm() == ctx.term.getCurrent()) {
                long totalWeight = quorum.requiredWeight();
                long matchedWeight = quorum.matchedWeight(ctx.matchIndex, n, ctx.term.getCurrent());

                if (matchedWeight >= totalWeight) {
                    ctx.commitIndex = n;
                    applyEngine.applyCommittedEntries();
                    logger.info("Leader {} advanced commitIndex to {}, matchedWeight={}, totalWeight={}",
                        ctx.nodeId, ctx.commitIndex, matchedWeight, totalWeight);
                    break;
                } else {
                    logger.debug("Leader {} cannot advance commitIndex to {}, matchedWeight={}, totalWeight={}, matchIndex={}",
                        ctx.nodeId, n, matchedWeight, totalWeight, ctx.matchIndex);
                }
            }
        }
    }

    /** 成员变更广播所需的单 peer 定制复制（MemberChangeEntry 专用快速路径） */
    void replicateToPeerOverride(String peerId, AppendEntriesRequest request) {
        ctx.transportLayer.sendAppendEntries(peerId, request);
    }

    /** 指定索引条目任期查询（成员变更构造 prevLog 用） */
    long getEntryTerm(long index) {
        return logStore.getEntryTerm(index);
    }

    /**
     * 从日志取 {@code [nextIdx, 未端]} 的视图复本（`index=log 下标+1` 的连续映射）。
     * 防御下标偏移假设被破坏（修复/注入缺陷）时回退全遍历——行为与旧实现一致。
     */
    private static List<LogEntry> tailFrom(List<LogEntry> log, long nextIdx) {
        int size = log.size();
        int start = (int) nextIdx - 1;
        if (nextIdx <= 0 || start >= size) {
            return new java.util.ArrayList<>();
        }
        if (start >= 0 && log.get(start).getIndex() == nextIdx) {
            return new ArrayList<>(log.subList(start, size));
        }
        // 连续假设被破坏：回退全遍历（等价旧行为）
        List<LogEntry> entriesToSend = new ArrayList<>();
        for (LogEntry entry : log) {
            if (entry.getIndex() >= nextIdx) {
                entriesToSend.add(entry);
            }
        }
        return entriesToSend;
    }
}
