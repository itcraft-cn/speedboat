package cn.itcraft.speedboat.statemachine;

import cn.itcraft.speedboat.raft.LogEntry;

/**
 * 状态机接口，定义日志应用、快照和恢复的契约。
 * 
 * <p>Raft 日志一致性保证状态机状态的一致性复制。</p>
 * 
 * @author speedboat
 * @since 1.0.0
 */
public interface StateMachine {

    void apply(LogEntry entry);

    void snapshot(String snapshotPath);

    void restore(String snapshotPath);

    long getLastAppliedIndex();

    /**
     * 本状态机是否真实承载检查点能力（报告002 H1）。
     *
     * <p>检查点驱动的 WAL 压实安全性取决于一条铁律：水位推进前必须已把
     * "覆盖 lastApplied 的状态"真实落盘。空实现状态机（如父组的 NoopStateMachine）
     * {@code snapshot()} 无任何副作用——它若被允许推进 {@code RaftStore} 的
     * compactFloor，会在 WAL 压力路径上回收掉本应由检查点覆盖的日志前缀，
     * 而重启后无从恢复（apply 重放到缺失索引只能静默跳过）。故空状态机
     * 必须显式声明"不可检查点"，由 Checkpointer 在推进水位前判定。</p>
     *
     * <p>缺省 true：具备真实 snapshot/restore 语义的状态机（LockStateMachine 等）
     * 行为与引入前逐字节一致。</p>
     */
    default boolean canSnapshot() {
        return true;
    }
}