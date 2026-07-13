package cn.itcraft.speedboat.statemachine;

import cn.itcraft.speedboat.raft.LogEntry;

public interface StateMachine {

    void apply(LogEntry entry);

    void snapshot(String snapshotPath);

    void restore(String snapshotPath);

    long getLastAppliedIndex();
}