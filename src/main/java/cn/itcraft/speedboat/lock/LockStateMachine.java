package cn.itcraft.speedboat.lock;

import cn.itcraft.speedboat.config.SpeedboatConsts;
import cn.itcraft.speedboat.raft.LogEntry;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import cn.itcraft.speedboat.serialize.SerializationException;
import cn.itcraft.speedboat.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * LockStateMachine 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class LockStateMachine implements StateMachine {

    private static final Logger logger = LoggerFactory.getLogger(LockStateMachine.class);

    /** 租约超时统一引用 {@link SpeedboatConsts#DEFAULT_LEASE_TIMEOUT_MS}（唯一权威定义，避免双副本漂移） */
    private static final long DEFAULT_LEASE_TIMEOUT_MS = SpeedboatConsts.DEFAULT_LEASE_TIMEOUT_MS;

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
                // 空命令视为坏 entry：隔离不应用，但必须推进 applied 索引，
                // 否则调用方 waitForApply 会永久卡死在该索引上（P1-20260914）
                lastAppliedIndex = entry.getIndex();
                return;
            }

            applyCommand(command);
            lastAppliedIndex = entry.getIndex();

            logger.debug("Applied lock command: {}", command);
        } catch (SerializationException e) {
            // 坏 entry 隔离：状态机语境下不能向上抛（会打死 raft 单线程），
            // 记录后同样推进索引跳过，保证 applied 推进的单调性
            lastAppliedIndex = entry.getIndex();
            logger.error("Failed to deserialize lock command at index {}, skipped entry, lastAppliedIndex advanced to {}",
                entry.getIndex(), lastAppliedIndex, e);
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

        // 锁未被任何人持有（已释放）时，也可用；或已被当前请求者持有时，可重入
        return !entry.isHeld() || entry.isHeldBy(nodeId);
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

    ConcurrentHashMap<String, LockEntry> getLockTable() {
        return lockTable;
    }
}