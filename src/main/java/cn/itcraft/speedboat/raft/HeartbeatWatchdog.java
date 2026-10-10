package cn.itcraft.speedboat.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * check-quorum 看门狗（内部协作器，非公开 API）。
 *
 * <p>职责唯一：leader 在自身心跳节拍上检查"心跳新鲜"的法定人数权重，
 * 不足以构成多数派时主动降级——孤岛 leader 不再对外产生日志/锁等影响。</p>
 *
 * <p>新鲜阈值语义沿用原实现：{@link NodeContext#DEFAULT_QUORUM_CHECK_TIMEOUT_MILLIS}
 * （5s），远大于选举超时/心跳周期——多数派侧会先选出更高任期 leader；本 watchdog
 * 仅作长分区兜底降级，避免瞬态抖动误杀合法 Leader。</p>
 *
 * <p>并发契约：仅限 raft 单线程调用（心跳任务路径）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class HeartbeatWatchdog {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatWatchdog.class);

    private final NodeContext ctx;
    private final QuorumCalculator quorum;
    private final RoleMachine roles;
    /** 新鲜响应权重判定所需的 peer 机房标签经 quorum 计算器提供 */

    HeartbeatWatchdog(NodeContext ctx, QuorumCalculator quorum, RoleMachine roles) {
        this.ctx = ctx;
        this.quorum = quorum;
        this.roles = roles;
    }

    /** 记录 peer 响应新鲜度量（由复制应答路径调用，Leader 侧有效） */
    void markResponse(String peerId) {
        ctx.lastResponseNanos.put(peerId, System.nanoTime());
        // AP 降级判定的 leader 侧信号源：对侧机房 peer 应答时刷新"最近见到对侧"时间戳。
        // CP 下该字段仅被写入、不参与任何判定，行为零影响。
        String peerDc = quorum.peerDatacenter(peerId);
        if (!peerDc.equals(ctx.datacenter)) {
            ctx.lastOppositeSeenNanos = System.nanoTime();
        }
    }

    /**
     * check-quorum（对标 MicroRaft quorumResponseTimestamp 主动降级）。
     *
     * <p>非 Leader 直接返回；多数派新鲜权重不足时降级 FOLLOWER 并清 leaderId。</p>
     */
    void checkQuorumMaybeDemote() {
        if (ctx.currentState != NodeState.LEADER) {
            return;
        }
        long thresholdNanos = TimeUnit.MILLISECONDS.toNanos(ctx.quorumCheckTimeoutMillis);
        long now = System.nanoTime();

        // 权重计算统一交回 QuorumCalculator，看门狗只负责"筛出哪些 peer 新鲜"。
        // 若看门狗自成一派按节点计数，会与选举侧（按权重、父组按机房聚合）口径分裂：
        // 不对称机房权重下主机房 self=2，杀光备机房后若仍按节点数累计
        // freshWeight < required，主机房父组 Leader 会被误降级，场景④退化为无主。
        java.util.Set<String> freshPeers = new java.util.HashSet<String>();
        for (String peerId : ctx.peerIds) {
            Long last = ctx.lastResponseNanos.get(peerId);
            if (last != null && (now - last) <= thresholdNanos) {
                freshPeers.add(peerId);
            }
        }

        // AP 降级判定（leader 侧）：先给策略一次评估机会。CP 恒 no-op 返回 false，
        // 此后 freshWeight/required 与引入前逐字节等价。AP 在对侧机房静默达阈值后进入
        // 降级态、把对侧剔除出分母，使本机房 self 重新够格——故必须在计算权重前评估。
        cn.itcraft.speedboat.strategy.consistency.ConsistencyPolicy policy = ctx.consistencyPolicy;
        if (policy != null && policy.allowDegradedTakeover()) {
            policy.evaluateDegraded(ctx.datacenter, quorum.oppositeDatacenter(),
                ctx.seatHeld, ctx.lastOppositeSeenNanos, now);
        }

        long freshWeight = quorum.freshWeight(freshPeers, ctx.term.getCurrent());
        long required = quorum.requiredWeight();
        if (freshWeight < required) {
            logger.warn("Node {} check-quorum FAILED (freshWeight={} < required={}), demoting to follower",
                ctx.nodeId, freshWeight, required);
            roles.doTransitionTo(NodeState.FOLLOWER);
            ctx.leaderId = null;
        }
    }
}
