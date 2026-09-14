package cn.itcraft.speedboat.persistence;

import cn.itcraft.speedboat.raft.LogEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 内存持久化实现（测试/验证专用）。
 *
 * <p>在 JVM 内维持 term/votedFor/leader 与日志的"可恢复"副本，
 * 语义对齐 RaftStore 契约：persist 返回即 durable（内存层面）；
 * 支持 {@link #restoreTerm()} / {@link #restoredLogEntries()} 模拟节点重启恢复。</p>
 *
 * <p><b>线程模型：</b>persist* 由 raft 单线程调用（写），restore* 由任意线程读取
 * ——使用读写锁保护，读侧无阻塞。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class InMemoryRaftStore implements RaftStore {

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
