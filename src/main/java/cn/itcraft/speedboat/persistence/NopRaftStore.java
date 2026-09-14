package cn.itcraft.speedboat.persistence;

import cn.itcraft.speedboat.raft.LogEntry;

import java.util.Collections;
import java.util.List;

/**
 * 空持久化实现（内存模式）。
 *
 * <p>所有写操作为 no-op，恢复接口返回"未持久化"占位——
 * 算法行为与未接入持久化前的历史版本完全一致。
 * 用于：不以 durability 为目标的选主场景、教程与演示。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class NopRaftStore implements RaftStore {

    private static final NopRaftStore INSTANCE = new NopRaftStore();

    public static NopRaftStore getInstance() {
        return INSTANCE;
    }

    @Override
    public void persistAndFlushTerm(long term, String votedFor, String leaderId) {
        // no-op：内存模式不落盘
    }

    @Override
    public void persistLogEntries(List<LogEntry> entries) {
        // no-op
    }

    @Override
    public void truncateLogEntriesFrom(long fromIndex) {
        // no-op
    }

    @Override
    public void truncateLogEntriesUntil(long untilIndex) {
        // no-op
    }

    @Override
    public void deleteSnapshotChunks() {
        // no-op
    }

    @Override
    public void flush() {
        // no-op
    }

    @Override
    public RaftTermRecord restoreTerm() {
        return null;
    }

    @Override
    public List<LogEntry> restoredLogEntries() {
        return Collections.emptyList();
    }
}
