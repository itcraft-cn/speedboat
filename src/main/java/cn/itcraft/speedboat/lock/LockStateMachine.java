package cn.itcraft.speedboat.lock;

import cn.itcraft.speedboat.config.SpeedboatConsts;
import cn.itcraft.speedboat.raft.LogEntry;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import cn.itcraft.speedboat.serialize.SerializationException;
import cn.itcraft.speedboat.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 锁状态机： Raft 日志全序 apply 的锁表权威裁决点。
 *
 * <p><b>裁决铁律（2026-09-18 命名锁设计）：</b></p>
 * <ol>
 *   <li>互斥的唯一裁判在 {@link #applyCommand}——LOCK/RENEW/UNLOCK 命令
 *       按日志全序 apply，tryAcquire/renew/release 的返回值即命令结果，全网一致；</li>
 *   <li>判定结果写入 {@link #opResults}（按 requestId 索引），
 *       调用方（申请节点）apply 到位后读取，不再靠超时盲猜；</li>
 *   <li>{@code appliedRequestIds} 提供重放幂等：同一条命令的本节点内重复应用
 *       （理论不应发生，防御日志重放/测试注入）只记一次，重入计数不被重复累加；</li>
 *   <li>租约到期点采用命令内 Leader 打点（grantTimestampMs + leaseMs），
 *       全网一致，消除各副本本地打点漂移。</li>
 * </ol>
 *
 * @author speedboat
 * @since 1.0.0
 */
public class LockStateMachine implements StateMachine {

    private static final Logger logger = LoggerFactory.getLogger(LockStateMachine.class);

    /** 租约超时统一引用 {@link SpeedboatConsts#DEFAULT_LEASE_TIMEOUT_MS}（唯一权威定义，避免双副本漂移） */
    private static final long DEFAULT_LEASE_TIMEOUT_MS = SpeedboatConsts.DEFAULT_LEASE_TIMEOUT_MS;
    /**
     * 结果表/幂等表的"最近窗口"只保留 60 秒：调用方等待超时（默认 5s）
     * 远小于窗口，清理不会误伤在途请求；同时也让重启/回放后旧结果自然淘汰。
     */
    private static final long RESULT_WINDOW_MILLIS = 60_000;
    /** 结果表硬容量上限，超限立即按时间清理（当前节点 apply 频率远达不到该量级） */
    private static final int RESULT_TABLE_MAX = 4096;

    private final ConcurrentHashMap<String, LockEntry> lockTable = new ConcurrentHashMap<>();
    /** 命令判定结果表：requestId → 判定（GRANTED(epoch) / DENIED(原因)） */
    private final ConcurrentHashMap<String, LockOpResult> opResults = new ConcurrentHashMap<>();
    /**
     * 命令幂等去重表：requestId → 首次应用墙钟。
     * 仅在 raft 单线程 apply 路径访问（非防御共享，但用 ConcurrentHashMap 免起争议）。
     */
    private final ConcurrentHashMap<String, Long> appliedRequestIds = new ConcurrentHashMap<>();
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
        String requestId = command.getRequestId();

        // 幂等闸门：同一 requestId 只应用一次（防御重放/注入；正常路径每命令恰好 apply 一次）
        if (requestId != null) {
            if (appliedRequestIds.putIfAbsent(requestId, System.currentTimeMillis()) != null) {
                logger.debug("Duplicate lock command skipped (idempotent): requestId={}", requestId);
                return;
            }
        }

        LockEntry lockEntry = lockTable.computeIfAbsent(lockName, LockEntry::new);

        // 授权打点：命令内 Leader stamp（>0 全网一致）；0 兜底本地墙钟（兼容历史/测试）
        long grantPoint = command.getGrantTimestampMs() > 0 ? command.getGrantTimestampMs() : System.currentTimeMillis();
        // 租约时长：命令携带优先；未携带回退全局默认（兼容旧命令格式）
        long leaseMs = command.getLeaseMs() > 0 ? command.getLeaseMs() : DEFAULT_LEASE_TIMEOUT_MS;

        switch (command.getCommandType()) {
            case LOCK:
                boolean acquired = lockEntry.tryAcquire(nodeId, leaseMs, grantPoint);
                if (acquired) {
                    recordResult(requestId, LockOpResult.granted(lockEntry.getEpoch()));
                    logger.info("Lock acquired: {} by {} (epoch={})", lockName, nodeId, lockEntry.getEpoch());
                } else {
                    recordResult(requestId, LockOpResult.denied("DENIED_HELD_BY " + lockEntry.getNodeId()));
                    logger.info("Lock acquire denied: {} by {}, holder={}", lockName, nodeId, lockEntry.getNodeId());
                }
                break;

            case UNLOCK:
                boolean released = lockEntry.release(nodeId);
                if (released) {
                    recordResult(requestId, LockOpResult.granted(lockEntry.getEpoch()));
                    logger.info("Lock released: {} by {}", lockName, nodeId);
                } else {
                    recordResult(requestId, LockOpResult.denied("NOT_HOLDER"));
                    logger.info("Lock release denied (not holder): {} by {}", lockName, nodeId);
                }
                break;

            case RENEW:
                boolean renewed = lockEntry.renew(nodeId, leaseMs, grantPoint);
                if (renewed) {
                    recordResult(requestId, LockOpResult.granted(lockEntry.getEpoch()));
                    logger.debug("Lock renewed: {} by {}", lockName, nodeId);
                } else {
                    recordResult(requestId, LockOpResult.denied("RENEW_LOST"));
                    logger.warn("Lock renew rejected (lost holdership): {} by {}", lockName, nodeId);
                }
                break;

            default:
                logger.warn("Unknown command type: {}", command.getCommandType());
        }
        pruneTablesIfOverflow();
    }

    /** 幂等：requestId 为空（旧格式命令）时不记录，调用方退回"等到 apply 验持锁"（旧语义） */
    private void recordResult(String requestId, LockOpResult result) {
        if (requestId == null) {
            return;
        }
        opResults.put(requestId, result);
    }

    /** 读取判定结果（不消费；消费侧用 {@link #removeLockOpResult}） */
    public LockOpResult getLockOpResult(String requestId) {
        return requestId == null ? null : opResults.get(requestId);
    }

    /** 移除并返回本节点结果（调用方授予确认后清理，节点归属制：删除仅影响本节点副本） */
    public LockOpResult removeLockOpResult(String requestId) {
        return requestId == null ? null : opResults.remove(requestId);
    }

    /**
     * 惰性清理：表超过软容量时按时间窗口淘汰。
     * 在 raft apply 线程调用，占用极小（高频锁场景下每 4096 次 apply 一次 O(n)）。
     */
    private void pruneTablesIfOverflow() {
        if (opResults.size() < RESULT_TABLE_MAX && appliedRequestIds.size() < RESULT_TABLE_MAX) {
            return;
        }
        long now = System.currentTimeMillis();
        opResults.values().removeIf(r -> now - r.getStampMillis() > RESULT_WINDOW_MILLIS);
        appliedRequestIds.values().removeIf(stamp -> now - stamp > RESULT_WINDOW_MILLIS);
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
        if (snapshotPath == null || snapshotPath.isEmpty()) {
            return;
        }
        File tmp = new java.io.File(snapshotPath + ".tmp");
        File target = new java.io.File(snapshotPath);
        try {
            // 原子替换：temp 写成 + rename，防半写
            try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(tmp, "rw")) {
                java.nio.ByteBuffer buf = serializeState();
                raf.setLength(buf.remaining());
                java.nio.channels.FileChannel ch = raf.getChannel();
                buf.position(0);
                ch.position(0);
                ch.write(buf);
                ch.force(true);
            }
            java.nio.file.Files.deleteIfExists(target.toPath());
            if (!tmp.renameTo(target)) {
                logger.warn("snapshot rename failed: {} -> {}", tmp, target);
                return;
            }
            logger.info("LockStateMachine snapshot written: path={} appliedIndex={} locks={}",
                target.getAbsolutePath(), lastAppliedIndex, lockTable.size());
        } catch (IOException e) {
            logger.error("LockStateMachine snapshot failed: path={}", snapshotPath, e);
        }
    }

    @Override
    public void restore(String snapshotPath) {
        if (snapshotPath == null || snapshotPath.isEmpty()) {
            return;
        }
        File target = new java.io.File(snapshotPath);
        if (!target.exists()) {
            return;
        }
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(target, "r")) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate((int) raf.length());
            java.nio.channels.FileChannel ch = raf.getChannel();
            ch.position(0);
            ch.read(buf);
            buf.rewind();
            deserializeState(buf);
            logger.info("LockStateMachine restored: path={} appliedIndex={} entries={}",
                target.getAbsolutePath(), lastAppliedIndex, lockTable.size());
        } catch (IOException e) {
            logger.warn("LockStateMachine restore failed (start blank): path={}", target.getAbsolutePath(), e);
        }
    }

    /**
     * 序列化布局：crc32(4) 覆盖 payload；payload = magic(4)|count(4)|{name UTF | holderFlag 1 |
     * holder UTF | holdCount 4 | leaseExpireTime 8 | epoch 8}.. | lastAppliedIndex(8)。
     * 重启确定性一致（不依赖日志重放时序）。
     */
    private java.nio.ByteBuffer serializeState() throws IOException {
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        java.io.DataOutputStream out = new java.io.DataOutputStream(bos);
        out.writeInt(SNAPSHOT_MAGIC);
        out.writeInt(lockTable.size());
        for (Map.Entry<String, LockEntry> e : lockTable.entrySet()) {
            LockEntry le = e.getValue();
            out.writeUTF(e.getKey());
            String holder = le.getNodeId();
            out.writeBoolean(holder != null);
            out.writeUTF(holder == null ? "" : holder);
            out.writeInt(le.getHoldCount());
            out.writeLong(le.getLeaseExpireTime());
            out.writeLong(le.getEpoch());
        }
        out.writeLong(lastAppliedIndex);
        out.flush();
        byte[] payload = bos.toByteArray();

        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(payload);
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(payload.length + 4);
        buffer.putInt((int) crc.getValue());
        buffer.put(payload);
        buffer.flip();
        return buffer;
    }

    private void deserializeState(java.nio.ByteBuffer buf) throws IOException {
        int crc = buf.getInt();
        byte[] payload = new byte[buf.remaining()];
        buf.get(payload);
        java.util.zip.CRC32 c = new java.util.zip.CRC32();
        c.update(payload);
        if ((int) c.getValue() != crc) {
            throw new IOException("LockStateMachine snapshot CRC mismatch");
        }
        java.io.DataInputStream in = new java.io.DataInputStream(new java.io.ByteArrayInputStream(payload));
        if (in.readInt() != SNAPSHOT_MAGIC) {
            throw new IOException("LockStateMachine snapshot magic mismatch");
        }
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String lockName = in.readUTF();
            boolean hasHolder = in.readBoolean();
            String holder = hasHolder ? in.readUTF() : null;
            int holdCount = in.readInt();
            long lease = in.readLong();
            long epoch = in.readLong();
            LockEntry le = lockTable.computeIfAbsent(lockName, LockEntry::new);
            // 直接回填（绕过 tryAcquire：snapshot 即"当机时刻合法权益"）
            restoreEntry(le, holder, holdCount, lease, epoch);
        }
        lastAppliedIndex = in.readLong();
    }

    /** LockEntry 绕过判定直达的快照回填（synchronized 保单线程一致性） */
    private void restoreEntry(LockEntry le, String holder, int holdCount, long lease, long epoch) {
        le.restoreFromCheckpoint(holder, holdCount, lease, epoch);
    }

    private static final int SNAPSHOT_MAGIC = 0x4C4B4350; // "LKCP"

    @Override
    public long getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    ConcurrentHashMap<String, LockEntry> getLockTable() {
        return lockTable;
    }
}