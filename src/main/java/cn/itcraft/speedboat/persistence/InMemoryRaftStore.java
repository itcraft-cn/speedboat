package cn.itcraft.speedboat.persistence;

import cn.itcraft.speedboat.raft.LogEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 内存持久化实现（三档策略之 Mem：<b>显式档</b>——无盘沙箱/容器 ephemeral 场景）。
 *
 * <p><b>三档定位（2026-09-18 P4-M5 收敛：缺省已让位 mmap）：</b></p>
 * <ul>
 *   <li>{@link NopRaftStore}：测试基线/显式关闭；库级 Builder 缺省</li>
 *   <li><b>Mem（本类，显式选档 raft.persistence=mem）</b>：JVM 进程内可恢复，
 *       零磁盘开销；<b>跨进程重启丢 term/votedFor（重复投票风险）与锁 epoch</b>
 *       ——不支持"极稳定/重启保真"诉求</li>
 *   <li>{@link MmapRaftStore}（<b>生产缺省</b>）：跨进程重启可挂回</li>
 * </ul>
 *
 * <p>语义对齐 RaftStore 契约：persist 返回即 durable（内存层面）；
 * 支持 {@link #restoreTerm()} / {@link #restoredLogEntries()} 模拟节点重启恢复。</p>
 *
 * <p><b>线程模型：</b>persist* 由 raft 单线程调用（写），restore* 由任意线程读取
 * ——使用读写锁保护，读侧无阻塞。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class InMemoryRaftStore implements RaftStore {

    /** 新建实例（缺省装配口：每个 raft 节点一份私有状态，禁止单例） */
    public static InMemoryRaftStore createDefault() {
        return new InMemoryRaftStore();
    }

    // 【PERSISTENT】任期状态：term / votedFor / leaderId
    private long term;
    private String votedFor;
    private String leaderId;
    private boolean termPersisted;

    // 【PERSISTENT】日志（index 连续），按 index 升序
    private final TreeMap<Long, LogEntry> logEntries = new TreeMap<>();

    // 【PERSISTENT】快照分块（预留口，本版本不写）
    private final Map<Long, byte[]> snapshotChunks = new TreeMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public void persistAndFlushTerm(long term, String votedFor, String leaderId) {
        lock.writeLock().lock();
        try {
            this.term = term;
            this.votedFor = votedFor;
            this.leaderId = leaderId;
            this.termPersisted = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void persistLogEntries(List<LogEntry> entries) {
        if (entries == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            for (LogEntry entry : entries) {
                logEntries.put(entry.getIndex(), entry);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void truncateLogEntriesFrom(long fromIndex) {
        lock.writeLock().lock();
        try {
            logEntries.tailMap(fromIndex).clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void truncateLogEntriesUntil(long untilIndex) {
        lock.writeLock().lock();
        try {
            logEntries.headMap(untilIndex, true).clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void deleteSnapshotChunks() {
        lock.writeLock().lock();
        try {
            snapshotChunks.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void flush() {
        // 内存实现无需 fsync
    }

    @Override
    public RaftTermRecord restoreTerm() {
        lock.readLock().lock();
        try {
            if (!termPersisted) {
                return null;
            }
            return new RaftTermRecord(term, votedFor, leaderId);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<LogEntry> restoredLogEntries() {
        lock.readLock().lock();
        try {
            if (logEntries.isEmpty()) {
                return Collections.emptyList();
            }
            return Collections.unmodifiableList(new ArrayList<>(logEntries.values()));
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 测试专用：当前持久化视角的快照（索引与 list 一致）。 */
    public int logEntryCount() {
        lock.readLock().lock();
        try {
            return logEntries.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}
