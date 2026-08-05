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
}