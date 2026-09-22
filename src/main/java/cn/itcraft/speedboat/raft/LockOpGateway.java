package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.lock.LockCommand;
import cn.itcraft.speedboat.rpc.LockOpRequest;
import cn.itcraft.speedboat.rpc.LockOpResponse;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import cn.itcraft.speedboat.serialize.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 锁操作网关（内部协作器，非公开 API）。
 *
 * <p>职责唯一：命名锁命令的转发协议两端——</p>
 * <ul>
 *   <li><b>Leader 裁决点</b>（{@link #doHandleLockOp}）：只裁决"是否收录提案并回执
 *       日志索引"，不做互斥判定——互斥判定在状态机 apply（全序）发生，所有副本按
 *       同一日志序得出同一结论；Leader 收录前仅做<b>快速失败预检</b>
 *       （预检由申请方可选完成，此处仅校验提案本身）。</li>
 *   <li><b>授权打点</b>：Leader 在 propose 前以自身时钟写入 grantTimestampMs，
 *       随命令复制后全网到期点一致（多持有者租约语义的基石）。</li>
 *   <li><b>Follower 转发</b>（{@link #forwardLockOp}）：无已知 Leader（选举中）
 *       或已恰好是 Leader（本机裁决）时以"未收录"完成，调用方按需重试。</li>
 * </ul>
 *
 * <p>并发契约：{@link #doHandleLockOp} 仅限 raft 单线程调用；
 * {@link #forwardLockOp} 线程安全（经 transport 异步）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class LockOpGateway {

    private static final Logger logger = LoggerFactory.getLogger(LockOpGateway.class);

    private final NodeContext ctx;
    private final CommandProposer proposer;
    /**
     * requestId → 上次裁决响应的幂等缓存（SOFAJRaft RequestMap 纪律）。
     *
     * <p>背景：申请方在"propose 受理成功但应答丢失"后重试，原实现会重复入日志
     * （重复提案 → 状态机 requestId 去重表兜底为 DENIED，语义可能看似"锁丢失"）。
     * 现网关侧先查缓存命中直接回放原 response，天然幂等。</p>
     *
     * <p>缓存范围（收敛的确定性结果）：</p>
     * <ul>
     *   <li>受理成功（entryIndex&gt;0）——回放成功结果；</li>
     *   <li>确定性拒绝（空 payload/畸形命令）——同参重试必然同拒；</li>
     *   <li>【不缓存】非 Leader 拒绝——条件态（选举后可能成功），重试要放行。</li>
     * </ul>
     *
     * <p>上界默认 4096（与 raft.max.log.size 同量级记忆），LRU 驱逐（BoundedCache）。</p>
     */
    private final cn.itcraft.speedboat.raft.util.BoundedCache<String, LockOpResponse> dedupCache =
        new cn.itcraft.speedboat.raft.util.BoundedCache<>(4096);

    LockOpGateway(NodeContext ctx, CommandProposer proposer) {
        this.ctx = ctx;
        this.proposer = proposer;
    }

    /**
     * 锁操作转发裁决点（Leader 收到非 Leader 成员的锁命令时在 raft 线程执行）。
     *
     * <p>语义分四段：</p>
     * <ol>
     *   <li>非 Leader → 拒绝（不缓存）；</li>
     *   <li>幂等缓存命中 → 直接回放历史裁决；</li>
     *   <li>确定性预检失败（空包/畸形）→ 拒绝【并缓存】；</li>
     *   <li>授权打点 → propose → 受理成功【并缓存】。</li>
     * </ol>
     */
    LockOpResponse doHandleLockOp(LockOpRequest request) {
        LockOpResponse rejected = new LockOpResponse(request.getRequestId(), false, -1);

        if (ctx.currentState != NodeState.LEADER) {
            logger.debug("Node {} not leader, reject lock op: requestId={}", ctx.nodeId, request.getRequestId());
            return rejected;
        }

        byte[] payload = request.getCommand();
        if (payload == null || payload.length == 0) {
            logger.warn("Node {} empty lock command payload: requestId={}", ctx.nodeId, request.getRequestId());
            return dedupCache.computeIfAbsent(
                dedupKey(request.getRequestId()), k -> new LockOpResponse(request.getRequestId(), false, -1));
        }

        try {
            LockCommand command = new ProtostuffSerializer().deserialize(payload, LockCommand.class);
            if (command == null || command.getLockName() == null || command.getNodeId() == null
                || command.getCommandType() == null) {
                logger.warn("Node {} malformed lock command: requestId={}", ctx.nodeId, request.getRequestId());
                return dedupCache.computeIfAbsent(
                    dedupKey(request.getRequestId()), k -> new LockOpResponse(request.getRequestId(), false, -1));
            }

            // 幂等缓存命中：回放受理结果（含原 entryIndex），不重复入日志
            LockOpResponse cached = dedupCache.get(dedupKey(request.getRequestId()));
            if (cached != null) {
                logger.debug("Node {} lock op dedup hit: requestId={}, entryIndex={}",
                    ctx.nodeId, request.getRequestId(), cached.getEntryIndex());
                return cached;
            }

            // Leader 授权打点（提案内容重编排：原 requestId/lockName/申请者身份原样保留）
            LockCommand stamped = new LockCommand(
                command.getLockName(), command.getNodeId(), command.getCommandType(),
                command.getRequestId(), command.getLeaseMs(), System.currentTimeMillis());

            long entryIndex = proposer.propose(new ProtostuffSerializer().serialize(stamped));
            LockOpResponse accepted = new LockOpResponse(request.getRequestId(), entryIndex > 0, entryIndex);
            if (entryIndex > 0) {
                dedupCache.put(dedupKey(request.getRequestId()), accepted);
            }
            logger.info("Node {} accepted lock op forwarding: requestId={}, lockName={}, entryIndex={}",
                ctx.nodeId, request.getRequestId(), command.getLockName(), entryIndex);
            return accepted;
        } catch (SerializationException e) {
            logger.error("Node {} failed to decode lock command: requestId={}",
                ctx.nodeId, request.getRequestId(), e);
            return rejected;
        }
    }

    /** 幂等键：requestId 已含申请方唯一标识，前缀 px 防跨协议混写 */
    private static String dedupKey(String requestId) {
        return "px:" + requestId;
    }

    /**
     * Follower 侧转发：目指已知 Leader；无 Leader/已是 Leader 时返回"未收录"完成。
     */
    java.util.concurrent.CompletableFuture<LockOpResponse> forwardLockOp(LockOpRequest request) {
        if (ctx.transportLayer == null) {
            return java.util.concurrent.CompletableFuture.completedFuture(new LockOpResponse(request.getRequestId(), false, -1));
        }
        String leader = ctx.leaderId;
        if (leader == null || leader.equals(ctx.nodeId)) {
            // 无已知 Leader（选举中）或已恰好是 Leader：未来以"未收录"完成，调用方按需重试
            return java.util.concurrent.CompletableFuture.completedFuture(new LockOpResponse(request.getRequestId(), false, -1));
        }
        return ctx.transportLayer.sendLockOp(leader, request);
    }
}
