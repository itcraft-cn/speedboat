package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 状态机检查点服务（内部协作器，非公开 API）。
 *
 * <p>职责唯一：状态机 snapshot/restore 与 {@code RaftStore} 水位管理。</p>
 *
 * <p>成功次序铁律：先检查点落盘 + flush，再 {@code markCheckpoint} 上报水位——
 * 水位一旦上报即允许存储层压实回收 WAL 前缀，顺序颠倒会导致回收后仍无检查点可恢复。</p>
 *
 * <p>启动恢复链三段（行序不可错）：</p>
 * <ol>
 *   <li>{@code restoreTermIfPresent}（任期/WAL 恢复，归 RaftLogStore）；</li>
 *   <li>{@code restoreStateCheckpoint}（检查点 lastApplied 跳位）；</li>
 *   <li>{@code applyCommittedEntries} 全序重放（由 apply 侧负责）。</li>
 * </ol>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class Checkpointer {

    private static final Logger logger = LoggerFactory.getLogger(Checkpointer.class);

    /** 缺省检查点触发增量（applied 条数） */
    static final int DEFAULT_CHECKPOINT_INTERVAL = 1024;

    private final NodeContext ctx;

    Checkpointer(NodeContext ctx) {
        this.ctx = ctx;
    }

    /**
     * 启动期检查点恢复（start() 的第二段）：
     * 先落到 checkpoint 时刻的状态机（锁表/epoch），之后只重放 > lastApplied 的日志尾巴。
     *
     * <p>仅当 raftStore 提供检查点路径（MmapRaftStore）时生效；异常只告警（保活性）。</p>
     *
     * @return 恢复到的 appliedIndex（无检查点返回 0）
     */
    long restoreStateCheckpoint(StateMachine stateMachine) {
        if (stateMachine == null) {
            return 0;
        }
        String checkpointFile = ctx.raftStore.getCheckpointFile();
        if (checkpointFile == null) {
            return 0;
        }
        try {
            stateMachine.restore(checkpointFile);
            long smApplied = stateMachine.getLastAppliedIndex();
            if (smApplied > 0) {
                lastCheckpoint(smApplied);
                logger.info("Node {} checkpoint restore applied={}", ctx.nodeId, smApplied);
                return smApplied;
            }
        } catch (Exception e) {
            logger.warn("Node {} state machine checkpoint restore failed: {}", ctx.nodeId, e.toString());
        }
        return 0;
    }

    /** shutdown 阶段的兜底快照：有检查点且 lastApplied 有增量才落盘。 */
    void snapshotOnShutdown(StateMachine stateMachine) {
        if (stateMachine == null) {
            return;
        }
        // H1：不可检查点的状态机（NoopStateMachine 等）禁止推进压实水位——
        // 空快照不写入任何文件，markCheckpoint 却会让 WAL 前缀失去唯一的恢复来源
        if (!stateMachine.canSnapshot()) {
            try {
                ctx.raftStore.flush();
            } catch (Exception e) {
                logger.warn("Node {} shutdown flush only (no checkpoint) failed: {}", ctx.nodeId, e.toString());
            }
            return;
        }
        try {
            String checkpointFile = ctx.raftStore.getCheckpointFile();
            if (checkpointFile != null && ctx.lastApplied > ctx.lastCheckpointApplied) {
                stateMachine.snapshot(checkpointFile);
                ctx.lastCheckpointApplied = ctx.lastApplied;
                ctx.raftStore.markCheckpoint(ctx.lastApplied);
            }
            ctx.raftStore.flush();
        } catch (Exception e) {
            logger.warn("Node {} shutdown checkpoint/flush failed: {}", ctx.nodeId, e.toString());
        }
    }

    /**
     * 检查点判定：仅当 raftStore 提供检查点路径（MmapRaftStore）且间隔达标时落盘。
     * raft 单线程调用；同步写（频率可配，缺省 1024 才一次），阻塞可接受。
     */
    void maybeCheckpoint() {
        StateMachine sm = ctx.getStateMachine();
        if (sm == null) {
            return;
        }
        // H1：不可检查点的状态机禁止推进水位（同 snapshotOnShutdown），WAL 保持原状
        if (!sm.canSnapshot()) {
            return;
        }
        String checkpointFile = ctx.raftStore.getCheckpointFile();
        if (checkpointFile == null) {
            return;
        }
        if (ctx.lastApplied - ctx.lastCheckpointApplied < ctx.checkpointInterval) {
            return;
        }
        try {
            sm.snapshot(checkpointFile);
            ctx.raftStore.flush();
            ctx.lastCheckpointApplied = ctx.lastApplied;
            ctx.raftStore.markCheckpoint(ctx.lastApplied);
        } catch (Exception e) {
            logger.warn("Node {} checkpoint failed: {}", ctx.nodeId, e.toString());
        }
    }

    /** 触发水位更新（启动恢复链使用） */
    private void lastCheckpoint(long applied) {
        ctx.lastCheckpointApplied = applied;
    }
}
