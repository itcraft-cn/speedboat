package cn.itcraft.speedboat.persistence;

import cn.itcraft.speedboat.raft.LogEntry;

import java.util.List;

/**
 * Raft 持久化存储契约（Phase D 前置抽象，对标 MicroRaft persistence/RaftStore）。
 *
 * <p><b>契约铁律（必须遵守）：</b></p>
 * <ol>
 *   <li><b>先持久化、后可见</b>：RaftNode 对本接口的所有方法都是
 *       "调用返回成功后，才允许修改内存状态"——调用方保证顺序，
 *       实现方保证返回时数据已安全（同步实现即 flush 完毕）；</li>
 *   <li><b>分级 flush</b>：<b>term/votedFor 必须原子落盘</b>（persistAndFlushTerm
 *       原子完成），日志条目允许批量/异步（persistLogEntries 返回即视为
 *       durable，实现方自行决定 fsync 时机，可通过 {@link #flush()} 强制）；</li>
 *   <li><b>调用线程为 raft 单消费者线程</b>：实现允许在这里阻塞/做 IO，
 *       算法侧把该时间视为"慢的现实"；但频繁大批量 IO 会拖慢心跳——
 *       实现方应自行评估批次大小；</li>
 *   <li><b>幂等与重启一致</b>：恢复接口（{@link #restoreTerm()} /
 *       {@link #restoredLogEntries()}）返回的数据必须能重建节点重启前
 *       最后可见的一致状态。</li>
 * </ol>
 *
 * <p>缺省实现为 {@link NopRaftStore}（内存模式，行为与历史版本一致）；
 * 测试用 {@link InMemoryRaftStore}；生产持久化引擎（文件 WAL 等）按此契约接入。</p>
 *
 * @author speedboat
 * @see NopRaftStore
 * @see InMemoryRaftStore
 * @since 1.1.0
 */
public interface RaftStore {

    /**
     * 原子持久化任期状态（term + votedFor + leaderId）。
     *
     * <p>铁律：term/votedFor 的持久化必须与内存变更串行——
     * 本方法返回后才允许 term 内存值被后续逻辑"发布"。</p>
     *
     * @param term     当前任期
     * @param votedFor 当前任期内已投票节点（可为 null）
     * @param leaderId 当前已知 leader（可为 null）
     */
    void persistAndFlushTerm(long term, String votedFor, String leaderId);

    /**
     * 持久化日志条目（追加/覆盖语义由恢复语义覆盖）。
     *
     * <p>调用方传入当前全量尾部日志（本组件日志按 index 连续），
     * 实现方按需做 append 或重建。</p>
     */
    void persistLogEntries(List<LogEntry> entries);

    /** 截断 [fromIndex, +∞) 的日志（leader 侧冲突覆盖时调用）。 */
    void truncateLogEntriesFrom(long fromIndex);

    /** 截断 (-∞, untilIndex] 的日志（快照后的头部收缩）。 */
    void truncateLogEntriesUntil(long untilIndex);

    /** 删除全部快照分块（本文版本快照未启用，保留契约口）。 */
    void deleteSnapshotChunks();

    /** 强制刷盘（日志异步实现方在需要 durability 时调用）。 */
    void flush();

    /**
     * 恢复任期状态（节点启动时调用一次）。
     *
     * @return 任期状态记录；从未持久化时返回 null
     */
    RaftTermRecord restoreTerm();

    /** 恢复日志条目（按 index 升序）；未持久化时返回空列表。 */
    List<LogEntry> restoredLogEntries();

    /**
     * 状态机检查点文件（可落盘实现提供；缺省 null = 无检查点恢复链）。
     *
     * <p>非 null 时 {@link cn.itcraft.speedboat.statemachine.StateMachine} 的
     * snapshot/restore 会以该路径持久化/恢复锁表与 lastAppliedIndex。
     * MmapRaftStore 提供、Nop/Mem 返回 null。</p>
     */
    default String getCheckpointFile() {
        return null;
    }

    /**
     * 上报检查点水位（P4-M4 容量回收联动；缺省 no-op）。
     *
     * <p>{@code appliedIndex} 表示"状态机已完整落盘覆盖至该索引"——实现方据此
     * 确认：索引 ≤ appliedIndex 的日志在<b>重启恢复</b>时不再需要（可从检查点重建），
     * 从而在容量压力下安全回收前缀（Mmap 档压实）。</p>
     *
     * <p>调用时机：RaftNodeImpl 在状态机 snapshot 成功 + flush 之后，raft 单线程调用。</p>
     */
    default void markCheckpoint(long appliedIndex) {
        // 缺省无容量回收需求（Nop/Mem）
    }
}
