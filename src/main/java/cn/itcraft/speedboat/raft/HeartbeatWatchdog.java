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
        long freshWeight = 1;  // 自己代表新鲜的 leader 响应
        for (String peerId : ctx.peerIds) {
            Long last = ctx.lastResponseNanos.get(peerId);
            if (last != null && (now - last) <= thresholdNanos) {
                freshWeight += 1;
                if (ctx.voteWeightStrategy != null) {
                    VoteContext context = VoteContext.forDatacenter(peerId, quorum.peerDatacenter(peerId), ctx.term.getCurrent());
                    freshWeight += ctx.voteWeightStrategy.calculateAdditionalWeight(context);
                }
            }
        }
        long required = quorum.requiredWeight();
        if (freshWeight < required) {
            logger.warn("Node {} check-quorum FAILED (freshWeight={} < required={}), demoting to follower",
                ctx.nodeId, freshWeight, required);
            roles.doTransitionTo(NodeState.FOLLOWER);
            ctx.leaderId = null;
        }
    }
}
