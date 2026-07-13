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

        while (System.currentTimeMillis() < deadline) {
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

        logger.debug("Lock acquire failed: {} by {} (timeout {}ms)", lockName, nodeId, timeoutMs);
        return new LockHandleImpl(false, lockName, nodeId, this);
    }

    private boolean tryLockInternal() {
        if (!raftNode.isLeader()) {
            logger.debug("Not leader, cannot acquire lock directly");
            return false;
        }

        if (stateMachine.isLockAvailable(lockName, nodeId)) {
            LockCommand command = LockCommand.lock(lockName, nodeId);
            byte[] data = serializeCommand(command);

            if (raftNode.propose(data)) {
                return waitForApply(1000);
            }
        }

        return false;
    }

    private boolean waitForApply(long timeoutMs) {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            if (stateMachine.isLockHeldBy(lockName, nodeId)) {
                return true;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
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