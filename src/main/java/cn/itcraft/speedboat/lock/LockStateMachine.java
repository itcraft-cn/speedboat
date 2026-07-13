package cn.itcraft.speedboat.lock;

import cn.itcraft.speedboat.raft.LogEntry;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import cn.itcraft.speedboat.serialize.SerializationException;
import cn.itcraft.speedboat.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class LockStateMachine implements StateMachine {

    private static final Logger logger = LoggerFactory.getLogger(LockStateMachine.class);

    private static final long DEFAULT_LEASE_TIMEOUT_MS = 30000;

    private final ConcurrentHashMap<String, LockEntry> lockTable = new ConcurrentHashMap<>();
    private final ProtostuffSerializer serializer = new ProtostuffSerializer();
    private volatile long lastAppliedIndex = 0;

    @Override
    public void apply(LogEntry entry) {
        if (entry == null) {
            return;
        }

        if (entry.getEntryType() != LogEntry.EntryType.COMMAND) {
            return;
        }

        byte[] data = entry.getData();
        if (data == null || data.length == 0) {
            return;
        }

        try {
            LockCommand command = serializer.deserialize(data, LockCommand.class);
            if (command == null) {
                return;
            }

            applyCommand(command);
            lastAppliedIndex = entry.getIndex();

            logger.debug("Applied lock command: {}", command);
        } catch (SerializationException e) {
            logger.error("Failed to deserialize lock command at index {}", entry.getIndex(), e);
        }
    }

    private void applyCommand(LockCommand command) {
        String lockName = command.getLockName();
        String nodeId = command.getNodeId();

        LockEntry lockEntry = lockTable.computeIfAbsent(lockName, LockEntry::new);

        switch (command.getCommandType()) {
            case LOCK:
                lockEntry.tryAcquire(nodeId, DEFAULT_LEASE_TIMEOUT_MS);
                logger.info("Lock acquired: {} by {}", lockName, nodeId);
                break;

            case UNLOCK:
                lockEntry.release(nodeId);
                logger.info("Lock released: {} by {}", lockName, nodeId);
                break;

            case RENEW:
                lockEntry.renew(nodeId, DEFAULT_LEASE_TIMEOUT_MS);
                logger.debug("Lock renewed: {} by {}", lockName, nodeId);
                break;

            default:
                logger.warn("Unknown command type: {}", command.getCommandType());
        }
    }

    public boolean isLockHeldBy(String lockName, String nodeId) {
        LockEntry entry = lockTable.get(lockName);
        if (entry == null) {
            return false;
        }

        if (entry.isLeaseExpired()) {
            return false;
        }

        return entry.isHeldBy(nodeId);
    }

    public boolean isLockAvailable(String lockName, String nodeId) {
        LockEntry entry = lockTable.get(lockName);
        if (entry == null) {
            return true;
        }

        if (entry.isLeaseExpired()) {
            return true;
        }

        return entry.isHeldBy(nodeId);
    }

    public LockEntry getLockEntry(String lockName) {
        return lockTable.get(lockName);
    }

    public int getLockCount() {
        return lockTable.size();
    }

    @Override
    public void snapshot(String snapshotPath) {
        logger.info("Snapshot not implemented yet: {}", snapshotPath);
    }

    @Override
    public void restore(String snapshotPath) {
        logger.info("Restore not implemented yet: {}", snapshotPath);
    }

    @Override
    public long getLastAppliedIndex() {
        return lastAppliedIndex;
    }
}