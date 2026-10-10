package cn.itcraft.speedboat.statemachine;

import cn.itcraft.speedboat.raft.LogEntry;

/**
 * 空实现状态机（跨机房父组专用）。
 *
 * <p>父组（跨机房层）的唯一职责是选出<b>全局唯一 Leader</b>，<b>不复制任何业务状态</b>——
 * 没有锁表、没有用户数据、没有 checkpoint 价值。但仍需提供一个状态机实例：
 * 父组会照常走 Raft 日志复制流程（Leader 心跳折叠为空 AppendEntries），
 * 状态机若为 {@code null} 会在 apply 路径上抛 NPE。</p>
 *
 * <p>本实现对所有操作无副作用，仅推进 {@code lastAppliedIndex} 水位，
 * 使父组的 commit/apply 单调前进、报告视图不出现倒退。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class NoopStateMachine implements StateMachine {

    /** 已应用到的最大日志索引（volatile 快照，供报告线程弱一致读） */
    private volatile long lastAppliedIndex = 0L;

    @Override
    public void apply(LogEntry entry) {
        if (entry != null && entry.getIndex() > lastAppliedIndex) {
            lastAppliedIndex = entry.getIndex();
        }
    }

    @Override
    public void snapshot(String snapshotPath) {
        // 父组无状态可快照
    }

    @Override
    public void restore(String snapshotPath) {
        // 父组无状态可恢复
    }

    @Override
    public long getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    /**
     * 空状态机毫无检查点能力：snapshot()/restore() 均为空操作、无任何文件写入。
     * 若允许它推进 WAL 压实水位，父组 WAL 前缀会被"没有检查点兜底"的压实回收
     * （报告002 H1 的失败链源头），故必须显式关闭检查点路径。
     *
     * @return 恒 false
     */
    @Override
    public boolean canSnapshot() {
        return false;
    }
}
