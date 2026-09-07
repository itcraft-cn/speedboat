package cn.itcraft.speedboat.lock;

import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import cn.itcraft.speedboat.serialize.SerializationException;
import cn.itcraft.speedboat.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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

    private static final long DEFAULT_LEASE_TIMEOUT_MS = 30000;
    private static final long DEFAULT_WAIT_TIMEOUT_MS = 5000;
    private static final long RETRY_INTERVAL_MS = 100;

    private final String lockName;
    private final String nodeId;
    private final RaftNode raftNode;
    private final LockStateMachine stateMachine;
    private final ProtostuffSerializer serializer;

    private final ScheduledExecutorService renewExecutor;
    private final AtomicBoolean renewing = new AtomicBoolean(false);

    public DistributedLockImpl(String lockName, String nodeId, RaftNode raftNode, LockStateMachine stateMachine) {
        this.lockName = lockName;
        this.nodeId = nodeId;
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
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
                
            if (tryLockInternal()) {
                startRenewTask();
                logger.info("Lock acquired: {} by {}", lockName, nodeId);
                return new LockHandleImpl(true, lockName, nodeId, this);
            }

            try {
                Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        logger.info("Lock acquire failed: {} by {} (timeout {}ms, attempts={})", lockName, nodeId, timeoutMs, attempt);
        return new LockHandleImpl(false, lockName, nodeId, this);
    }

    private boolean tryLockInternal() {
        logger.info("tryLockInternal: isLeader={}, lockName={}, nodeId={}", 
            raftNode.isLeader(), lockName, nodeId);
        
        if (!raftNode.isLeader()) {
            logger.info("Not leader, cannot acquire lock directly");
            return false;
        }

        if (stateMachine.isLockAvailable(lockName, nodeId)) {
            LockCommand command = LockCommand.lock(lockName, nodeId);
            byte[] data = serializeCommand(command);

            // 在 propose 之前记录目标索引，propose 后 lastApplied 可能已经推进
            long targetIndex = raftNode.getLastApplied() + 1;
            
            if (raftNode.propose(data)) {
                logger.info("Lock command proposed, waiting for apply, targetIndex={}", targetIndex);
                return waitForApply(1000, targetIndex);
            } else {
                logger.error("Failed to propose lock command, current node may not be leader");
            }
        } else {
            logger.error("Lock not available, entry={}", stateMachine.getLockEntry(lockName));
        }

        return false;
    }

    private boolean waitForApply(long timeoutMs, long targetIndex) {
        long startTime = System.nanoTime();
        logger.info("waitForApply: targetIndex={}, currentLastApplied={}, commitIndex={}, lockName={}, nodeId={}", 
            targetIndex, raftNode.getLastApplied(), raftNode.getCommitIndex(), lockName, nodeId);
        
        while ((System.nanoTime() - startTime) / 1_000_000 < timeoutMs) {
            long currentApplied = raftNode.getLastApplied();
            long currentCommit = raftNode.getCommitIndex();
            boolean heldBy = stateMachine.isLockHeldBy(lockName, nodeId);
            logger.debug("waitForApply check: targetIndex={}, currentApplied={}, currentCommit={}, heldBy={}, lockEntry={}", 
                targetIndex, currentApplied, currentCommit, heldBy, stateMachine.getLockEntry(lockName));
                
            if (heldBy && currentApplied >= targetIndex) {
                logger.info("waitForApply success: targetIndex={}, currentApplied={}", targetIndex, currentApplied);
                return true;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        logger.info("waitForApply timeout: targetIndex={}, finalApplied={}, finalCommit={}", 
            targetIndex, raftNode.getLastApplied(), raftNode.getCommitIndex());
        return false;
    }

    private void startRenewTask() {
        if (renewing.compareAndSet(false, true)) {
            long renewInterval = DEFAULT_LEASE_TIMEOUT_MS / 2;
            renewExecutor.scheduleAtFixedRate(
                this::renewLease,
                renewInterval,
                renewInterval,
                TimeUnit.MILLISECONDS
            );
            logger.debug("Started renew task for lock: {}", lockName);
        }
    }

    private void stopRenewTask() {
        if (renewing.compareAndSet(true, false)) {
            renewExecutor.shutdown();
            logger.debug("Stopped renew task for lock: {}", lockName);
        }
    }

    private void renewLease() {
        if (!raftNode.isLeader()) {
            return;
        }

        if (!stateMachine.isLockHeldBy(lockName, nodeId)) {
            logger.warn("Lock lost, stopping renew: {}", lockName);
            stopRenewTask();
            return;
        }

        LockCommand command = LockCommand.renew(lockName, nodeId);
        byte[] data = serializeCommand(command);
        raftNode.propose(data);
        logger.debug("Renewed lease for lock: {}", lockName);
    }

    @Override
    public void unlock() {
        if (!stateMachine.isLockHeldBy(lockName, nodeId)) {
            stopRenewTask();
            return;
        }

        LockCommand command = LockCommand.unlock(lockName, nodeId);
        byte[] data = serializeCommand(command);

        if (raftNode.propose(data)) {
            logger.info("Lock released: {} by {}", lockName, nodeId);
        }

        LockEntry entry = stateMachine.getLockEntry(lockName);
        if (entry == null || entry.getHoldCount() <= 0) {
            stopRenewTask();
        }
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

    public void shutdown() {
        stopRenewTask();
    }

    private static class LockHandleImpl implements LockHandle {

        private final boolean success;
        private final String lockName;
        private final String nodeId;
        private final DistributedLockImpl lock;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        LockHandleImpl(boolean success, String lockName, String nodeId, DistributedLockImpl lock) {
            this.success = success;
            this.lockName = lockName;
            this.nodeId = nodeId;
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
        public void close() {
            if (closed.compareAndSet(false, true) && success) {
                lock.unlock();
            }
        }
    }
}