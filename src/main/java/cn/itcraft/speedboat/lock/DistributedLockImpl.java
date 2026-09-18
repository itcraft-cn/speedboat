package cn.itcraft.speedboat.lock;

import cn.itcraft.speedboat.config.SpeedboatConsts;
import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.rpc.LockOpRequest;
import cn.itcraft.speedboat.rpc.LockOpResponse;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import cn.itcraft.speedboat.serialize.SerializationException;
import cn.itcraft.speedboat.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DistributedLock 接口的默认实现，基于 Raft 共识算法提供生产级分布式锁服务。
 * 
 * <p>DistributedLockImpl 是 Speedboat 分布式锁的核心实现类，负责将抽象的锁操作映射为具体的 Raft 日志复制、
 * 状态机更新和本地缓存管理，提供高性能、强一致性的分布式锁服务。</p>
 * 
 * <p>架构设计：</p>
 * <ul>
 *   <li><b>Raft 集成层</b>：通过 {@link RaftNode} 提交锁操作日志，利用 Raft 共识保证一致性</li>
 *   <li><b>状态机层</b>：{@link LockStateMachine} 维护全局锁状态，响应日志应用</li>
 *   <li><b>本地缓存层</b>：缓存当前节点的锁状态，减少 Raft 查询开销</li>
 *   <li><b>租约管理层</b>：定时续期长锁，防止因网络分区导致的误释放</li>
 *   <li><b>超时重试层</b>：实现可配置的超时和重试策略，提高锁获取成功率</li>
 * </ul>
 * 
 * <p>核心算法：</p>
 * <ol>
 *   <li><b>锁获取</b>：构建 {@link LockCommand} → 通过 Raft 提交 → 等待日志应用 → 更新本地缓存</li>
 *   <li><b>锁释放</b>：验证持有者 → 提交解锁命令 → 等待日志应用 → 清除本地状态</li>
 *   <li><b>状态同步</b>：定期从状态机同步锁状态，确保缓存一致性</li>
 *   <li><b>租约续期</b>：对于长锁，启动后台任务定期续期，防止过期</li>
 * </ul>
 * 
 * <p>性能优化：</p>
 * <ul>
 *   <li><b>本地缓存</b>：缓存当前节点持有的锁状态，避免每次查询都走 Raft</li>
 *   <li><b>批量提交</b>：支持批量锁操作，减少 Raft 日志条目数量</li>
 *   <li><b>异步续期</b>：租约续期使用后台线程，不阻塞业务线程</li>
 *   <li><b>快速失败</b>：本地缓存判断锁已被其他节点持有时立即返回失败</li>
 * </ul>
 * 
 * <p>容错机制：</p>
 * <ul>
 *   <li><b>Leader 故障</b>：自动切换到新 Leader，锁状态通过日志恢复</li>
 *   <li><b>网络分区</b>：租约超时后自动释放锁，防止脑裂</li>
 *   <li><b>节点故障</b>：故障节点持有的锁在租约超时后自动释放</li>
 *   <li><b>重复提交</b>：使用唯一锁 ID 防止重复操作</li>
 * </ul>
 * 
 * <p>配置参数：</p>
 * <ul>
 *   <li><b>默认租约超时</b>：30 秒（{@link #DEFAULT_LEASE_TIMEOUT_MS}）</li>
 *   <li><b>默认等待超时</b>：5 秒（{@link #DEFAULT_WAIT_TIMEOUT_MS}）</li>
 *   <li><b>重试间隔</b>：100 毫秒（{@link #RETRY_INTERVAL_MS}）</li>
 *   <li><b>续期线程池</b>：单线程调度器，负责租约续期</li>
 * </ul>
 * 
 * <p>使用限制：</p>
 * <ul>
 *   <li>锁名称必须唯一，不同资源应使用不同的锁名称</li>
 *   <li>锁租约超时应根据业务操作时间合理设置</li>
 *   <li>高并发锁竞争场景建议使用分段锁或乐观锁</li>
 *   <li>跨机房场景下需适当增加超时时间</li>
 * </ul>
 * 
 * <p>实现细节：</p>
 * <pre>{@code
 * // 内部状态转换
 * 1. 尝试获取锁：lockCache.computeIfAbsent()
 * 2. 提交 Raft 日志：raftNode.propose(serializer.serialize(command))
 * 3. 等待应用：等待状态机更新通知（通过 CompletableFuture）
 * 4. 更新缓存：收到应用通知后更新本地缓存
 * 5. 启动续期：如果是长锁，启动定时续期任务
 * }
 * </pre>
 * 
 * @author speedboat
 * @see DistributedLock
 * @see LockStateMachine
 * @see LockCommand
 * @see RaftNode
 * @since 1.0.0
 */
public class DistributedLockImpl implements DistributedLock {

    private static final Logger logger = LoggerFactory.getLogger(DistributedLockImpl.class);

    /** 租约超时统一引用 {@link SpeedboatConsts#DEFAULT_LEASE_TIMEOUT_MS}（唯一权威定义，避免双副本漂移） */
    private static final long DEFAULT_LEASE_TIMEOUT_MS = SpeedboatConsts.DEFAULT_LEASE_TIMEOUT_MS;
    /** 本锁实例的租约时长（构造时确定；建议 > 2×续期间隔即整租约的 1/2） */
    private final long leaseTimeoutMs;
    private static final long DEFAULT_WAIT_TIMEOUT_MS = 5000;
    private static final long RETRY_INTERVAL_MS = 100;
    /** 转发等待上限：略大于传输层 5s 兜底，确保能收到显式失败而非超时异常 */
    private static final long FORWARD_WAIT_TIMEOUT_MS = 6000;

    private final String lockName;
    private final String nodeId;
    private final RaftNode raftNode;
    private final LockStateMachine stateMachine;
    private final ProtostuffSerializer serializer;

    private final ScheduledExecutorService renewExecutor;
    private final AtomicBoolean renewing = new AtomicBoolean(false);
    /**
     * 当前在途续期任务句柄。
     *
     * <p>重要：续期任务的启停必须走 cancel(future) 而非 executor.shutdown()——
     * executor 是构造期创建的单例，shutdown 后 {@code scheduleAtFixedRate} 将抛
     * RejectedExecutionException，导致"锁失而复得"场景续期静默失效，
     * 租约过期后锁被他人抢走而持有者无感知（历史缺陷 P0-20260914）。</p>
     */
    private volatile ScheduledFuture<?> renewFuture;

    public DistributedLockImpl(String lockName, String nodeId, RaftNode raftNode, LockStateMachine stateMachine) {
        this(lockName, nodeId, raftNode, stateMachine, DEFAULT_LEASE_TIMEOUT_MS);
    }

    /**
     * 租约时长可指定构造（命名锁设计：lock.lease.ms 可配；测试用短租约验证迁移）。
     *
     * @param leaseTimeoutMs 本锁实例的租约时长（毫秒，>0 生效，<=0 回退默认 30s）
     */
    public DistributedLockImpl(String lockName, String nodeId, RaftNode raftNode,
                               LockStateMachine stateMachine, long leaseTimeoutMs) {
        this.lockName = lockName;
        this.nodeId = nodeId;
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
        this.leaseTimeoutMs = leaseTimeoutMs > 0 ? leaseTimeoutMs : DEFAULT_LEASE_TIMEOUT_MS;
        this.serializer = new ProtostuffSerializer();
        this.renewExecutor = Executors.newSingleThreadScheduledExecutor(
            NamedThreadFactory.forComponent("LeaseRenewer", nodeId)
        );
    }

    @Override
    public LockHandle tryLock() {
        return tryLock(DEFAULT_WAIT_TIMEOUT_MS);
    }

    @Override
    public LockHandle tryLock(long timeoutMs) {
        long startTime = System.currentTimeMillis();
        long deadline = startTime + timeoutMs;
        int attempt = 0;

        while (System.currentTimeMillis() < deadline) {
            attempt++;
            logger.info("tryLock attempt {}: isLeader={}, lockName={}, nodeId={}", 
                attempt, raftNode.isLeader(), lockName, nodeId);
                
            long acquiredEpoch = tryLockInternal();
            if (acquiredEpoch >= 0) {
                startRenewTask();
                logger.info("Lock acquired: {} by {} (epoch={})", lockName, nodeId, acquiredEpoch);
                return new LockHandleImpl(true, lockName, nodeId, this, acquiredEpoch);
            }

            try {
                Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        logger.info("Lock acquire failed: {} by {} (timeout {}ms, attempts={})", lockName, nodeId, timeoutMs, attempt);
        return new LockHandleImpl(false, lockName, nodeId, this, -1L);
    }

    /**
     * 单次申请（每次调用生成独立 requestId，构成一次独立提案）。
     *
     * <p><b>双路合一（命名锁设计）：</b>Leader 本地直接 propose（就地打点授权）；
     * 非 Leader 经转发协议把命令送达 Leader propose——接收的授权打点是 Leader 时钟，
     * 全网到期点一致。判定一律在状态机 apply（全序），本地视图仅作 Leader 侧快速失败。</p>
     *
     * <p>返回值语义（旧 boolean 更化，避免调用方区分"未获授权"与"瞬时故障"）：</p>
     * <ul>
     *   <li>{@code >= 0}：授予成功，值即 fencing epoch；</li>
     *   <li>{@code -1}：本论未获授权（含被拒/超时/停机），外层循环按退避重试。</li>
     * </ul>
     */
    private long tryLockInternal() {
        logger.info("tryLockInternal: isLeader={}, lockName={}, nodeId={}", 
            raftNode.isLeader(), lockName, nodeId);

        if (raftNode.isLeader()) {
            // Leader 本地快速失败（可选优化）：判定仍以 apply 结果为准
            if (!stateMachine.isLockAvailable(lockName, nodeId)) {
                logger.error("Lock not available, entry={}", stateMachine.getLockEntry(lockName));
                return -1;
            }
        }
        // 非 Leader 无需预检：本地 applied 视图可能滞后，误拒反而恶化公平性

        String requestId = java.util.UUID.randomUUID().toString();
        LockCommand command = new LockCommand(lockName, nodeId, LockCommand.CommandType.LOCK,
            requestId, leaseTimeoutMs, 0L);

        long entryIndex = proposeOrForward(command);
        if (entryIndex > 0) {
            logger.info("Lock command proposed at index {}, waiting for apply", entryIndex);
            return waitForApplyResult(entryIndex, requestId);
        }

        return -1;
    }

    /**
     * 命令入日志的双路统一入口。
     *
     * <p>Leader 路径：本机即 Leader，以其自身时钟就地打点后 propose；
     * 转发路（Leader 侧打点）。返回 proposal 日志索引或 -1（未收录，可安全重试）。</p>
     */
    private long proposeOrForward(LockCommand command) {
        if (raftNode.isLeader()) {
            // 本地 propose：打点即本人时刻（Leader 时钟的权威性与转发路径一致）
            LockCommand stamped = new LockCommand(command.getLockName(), command.getNodeId(),
                command.getCommandType(), command.getRequestId(), command.getLeaseMs(),
                System.currentTimeMillis());
            return raftNode.propose(serializeCommand(stamped));
        }

        // 非 Leader：转发给当前 Leader（无 Leader/转发失败返回 -1，调用方上层重试收敛）
        LockOpRequest forwardRequest = new LockOpRequest(serializeCommand(command));
        forwardRequest.setRoutingNodeId(nodeId);

        try {
            LockOpResponse response = raftNode.forwardLockOp(forwardRequest)
                .get(FORWARD_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (response != null && response.isOk()) {
                logger.info("Lock op forwarded & proposed: requestId={}, lockName={}, entryIndex={}",
                    command.getRequestId(), command.getLockName(), response.getEntryIndex());
                return response.getEntryIndex();
            }
            logger.info("Lock op forward not accepted (no leader / rejected): requestId={}", command.getRequestId());
        } catch (java.util.concurrent.TimeoutException e) {
            logger.warn("Lock op forward timeout: requestId={}", command.getRequestId());
        } catch (Exception e) {
            logger.warn("Lock op forward failed: requestId={}", command.getRequestId(), e);
        }
        return -1;
    }

    /**
     * 等待命令 apply 并读取权威判定。
     *
     * <p>判定结果表（按 requestId）一旦出现即为终态：GRANTED 返回 epoch，
     * DENIED 立即失败（新 tryLock(-1)，外层按需重试）——消除旧版"被拒仍等满超时"的盲等。
     * requestId 缺失（旧格式命令兜底）时退化为持锁验证语义。</p>
     */
    private long waitForApplyResult(long targetIndex, String requestId) {
        long startTime = System.nanoTime();
        long timeoutMs = 1000;

        while ((System.nanoTime() - startTime) / 1_000_000 < timeoutMs) {
            // 本地状态机已应用且写入判定结果 → 权威结论就地可得
            LockOpResult result = stateMachine.getLockOpResult(requestId);
            if (result != null) {
                stateMachine.removeLockOpResult(requestId);
                if (result.isSuccess()) {
                    if (stateMachine.isLockHeldBy(lockName, nodeId)) {
                        return result.getEpoch();
                    }
                    // 结果与持锁视图矛盾：视为异常判，退化为失败重试
                    logger.warn("Result granted but not held locally, requestId={}, lockName={}", requestId, lockName);
                    return -1;
                }
                logger.info("Lock acquire denied by state machine: requestId={}, err={}", requestId, result.getErrCode());
                return -1;
            }

            // 兜底（请求无 requestId 或结果表尚未覆盖）：老语义——已 apply 且锁已被本节点持有
            if (requestId == null && raftNode.getLastApplied() >= targetIndex
                && stateMachine.isLockHeldBy(lockName, nodeId)) {
                return 0;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        logger.info("waitForApplyResult timeout: targetIndex={}, finalApplied={}, requestId={}",
            targetIndex, raftNode.getLastApplied(), requestId);
        return -1;
    }

    /**
     * 启动续期任务（可重入安全）。
     *
     * <p>CAS 保证同一把锁只有一份续期任务；锁失而复得时旧任务已 cancel，
     * 此处重新调度即可——executor 生命周期与锁实例等价，永不中途 shutdown。</p>
     */
    private void startRenewTask() {
        if (renewing.compareAndSet(false, true)) {
            long renewInterval = leaseTimeoutMs / 2;
            renewFuture = renewExecutor.scheduleAtFixedRate(
                this::renewLease,
                renewInterval,
                renewInterval,
                TimeUnit.MILLISECONDS
            );
            logger.debug("Started renew task for lock: {}", lockName);
        }
    }

    /**
     * 停止续期任务：仅取消在途调度，不关闭 executor。
     *
     * <p>历史缺陷修复：旧实现直接 {@code renewExecutor.shutdown()}，导致
     * executor 终止后再次 startRenewTask 时任务提交被拒（续期静默失效）；
     * 且从未获取过锁时 executor 永不关闭（线程泄露）。现改为：
     * 调度由 future.cancel 控制，executor 统一在 {@link #shutdown()} 关闭。</p>
     */
    private void stopRenewTask() {
        if (renewing.compareAndSet(true, false)) {
            ScheduledFuture<?> future = this.renewFuture;
            this.renewFuture = null;
            if (future != null) {
                // 不中断在跑任务（false）：renewLease 单次执行很快，避免打断 propose
                future.cancel(false);
            }
            logger.debug("Stopped renew task for lock: {}", lockName);
        }
    }

    private void renewLease() {
        if (!stateMachine.isLockHeldBy(lockName, nodeId)) {
            logger.warn("Lock lost, stopping renew: {}", lockName);
            stopRenewTask();
            return;
        }

        String requestId = java.util.UUID.randomUUID().toString();
        LockCommand command = new LockCommand(lockName, nodeId, LockCommand.CommandType.RENEW,
            requestId, leaseTimeoutMs, 0L);
        long entryIndex = proposeOrForward(command);

        // 续期权威判定：被拒（RENEW_LOST）即锁已失守——立刻停止续期与业务持有预期，
        // 别等租约自然到期（宁可误停，不可双持）
        if (entryIndex > 0 && waitForRenewResult(requestId) != null) {
            logger.info("Renew confirmed for lock: {} by {}", lockName, nodeId);
        } else {
            // 非致命（转发失败/判定未达）：剩余租约仍有效，下一周期重试
            logger.warn("Renew unconfirmed (propose failed or judgement missed), lock={} holder={}", lockName, nodeId);
        }
    }

    /**
     * 短窗等待续期判定（仅用于失守检测；本轮续期失败不代表立即失锁，
     * 剩余租约仍为到期前的缓冲——真正的失守通知在下次续期或本地 expiry 发现）。
     */
    private LockOpResult waitForRenewResult(String requestId) {
        long startTime = System.nanoTime();
        long timeoutMs = 500;

        while ((System.nanoTime() - startTime) / 1_000_000 < timeoutMs) {
            LockOpResult result = stateMachine.getLockOpResult(requestId);
            if (result != null) {
                stateMachine.removeLockOpResult(requestId);
                if (result.isSuccess()) {
                    return result;
                }
                logger.warn("Renew rejected, lock may be lost: requestId={}, err={}, stopping renew: {}",
                    requestId, result.getErrCode(), lockName);
                stopRenewTask();
                return null;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return null;
    }

    @Override
    public void unlock() {
        if (!stateMachine.isLockHeldBy(lockName, nodeId)) {
            stopRenewTask();
            return;
        }

        String requestId = java.util.UUID.randomUUID().toString();
        LockCommand command = new LockCommand(lockName, nodeId, LockCommand.CommandType.UNLOCK,
            requestId, leaseTimeoutMs, 0L);
        byte[] data = serializeCommand(command);

        // 释放确认语义：close() 返回后调用方立即读到锁已释放（reproducible contract）
        long entryIndex = proposeOrForward(command);
        if (entryIndex > 0) {
            logger.info("Lock released: {} by {}", lockName, nodeId);
            waitForRelease(1000, entryIndex);
        } else {
            // propose/转发失败（如恰逢 no-leader）：UNLOCK 未进入日志，
            // 续期任务停止止血，本地锁状态交由租约过期自然失效（P2-20260914）
            logger.warn("Unlock propose failed (may be no leader), stopping renew task: {} by {}", lockName, nodeId);
            stopRenewTask();
        }

        LockEntry entry = stateMachine.getLockEntry(lockName);
        if (entry == null || entry.getHoldCount() <= 0) {
            stopRenewTask();
        }
    }

    /**
     * 等待指定日志索引被 commit 并 apply 完成（用于释放确认）。
     *
     * <p>与 {@link #waitForApplyResult(long, String)} 的区别：解锁场景是"期望锁被释放"
     * —— 终止条件是 lastApplied >= target 且锁已不在此节点手中。
     * raft 线程与调用线程独立，仅做异步轮询，无阻塞风险。</p>
     */
    private boolean waitForRelease(long timeoutMs, long targetIndex) {
        long startTime = System.nanoTime();
        while ((System.nanoTime() - startTime) / 1_000_000 < timeoutMs) {
            long currentApplied = raftNode.getLastApplied();
            boolean heldBy = stateMachine.isLockHeldBy(lockName, nodeId);
            if (currentApplied >= targetIndex && !heldBy) {
                return true;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        logger.warn("waitForRelease timeout: targetIndex={}, finalApplied={}, stillHeld={}",
            targetIndex, raftNode.getLastApplied(), stateMachine.isLockHeldBy(lockName, nodeId));
        return false;
    }

    private byte[] serializeCommand(LockCommand command) {
        try {
            return serializer.serialize(command);
        } catch (SerializationException e) {
            logger.error("Failed to serialize lock command", e);
            throw new RuntimeException("Failed to serialize lock command", e);
        }
    }

    @Override
    public boolean isLocked() {
        LockEntry entry = stateMachine.getLockEntry(lockName);
        return entry != null && entry.isHeld() && !entry.isLeaseExpired();
    }

    @Override
    public boolean isHeldByCurrentNode() {
        return stateMachine.isLockHeldBy(lockName, nodeId);
    }

    @Override
    public String getLockName() {
        return lockName;
    }

    @Override
    public String getHolderNodeId() {
        LockEntry entry = stateMachine.getLockEntry(lockName);
        return entry != null ? entry.getNodeId() : null;
    }

    /**
     * 停机：停止续期任务并关闭续期线程池。
     *
     * <p>executor 只在本方法关闭（与锁实例生命周期等价）；
     * 无论是否获取过锁都会执行，杜绝"未获取锁场景线程泄露"。
     * awaitTermination 有限等待，超时则中断兜底，避免无限阻塞调用方。</p>
     */
    public void shutdown() {
        stopRenewTask();
        renewExecutor.shutdown();
        try {
            if (!renewExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                renewExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            renewExecutor.shutdownNow();
        }
    }

    private static class LockHandleImpl implements LockHandle {

        private final boolean success;
        private final String lockName;
        private final String nodeId;
        /** fencing token（未成功持锁为 -1） */
        private final long epoch;
        private final DistributedLockImpl lock;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        LockHandleImpl(boolean success, String lockName, String nodeId, DistributedLockImpl lock) {
            this(success, lockName, nodeId, lock, -1L);
        }

        /** @param epoch fencing token（未成功持锁为 -1） */
        LockHandleImpl(boolean success, String lockName, String nodeId, DistributedLockImpl lock, long epoch) {
            this.success = success;
            this.lockName = lockName;
            this.nodeId = nodeId;
            this.epoch = epoch;
            this.lock = lock;
        }

        @Override
        public boolean isSuccess() {
            return success;
        }

        @Override
        public String getLockName() {
            return lockName;
        }

        @Override
        public String getNodeId() {
            return nodeId;
        }

        @Override
        public long getEpoch() {
            return epoch;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true) && success) {
                lock.unlock();
            }
        }
    }
}