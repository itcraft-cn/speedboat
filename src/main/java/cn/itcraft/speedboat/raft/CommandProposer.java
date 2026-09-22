package cn.itcraft.speedboat.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 命令提案器（内部协作器，非公开 API）。
 *
 * <p>职责唯一：Leader 收到用户命令 → 写日志（先持久化后可见）→ 触发出站复制。
 * 不做互斥/业务判定——那些属于状态机 apply（全序）。</p>
 *
 * <p>并发契约：仅限 raft 单线程调用。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class CommandProposer {

    private static final Logger logger = LoggerFactory.getLogger(CommandProposer.class);

    private final NodeContext ctx;
    private final RaftLogStore logStore;
    /** 提案后立即广播一轮（由 ReplicationPump 提供，避免依赖环） */
    private final Runnable sendAppendFlows;

    CommandProposer(NodeContext ctx, RaftLogStore logStore, Runnable sendAppendFlows) {
        this.ctx = ctx;
        this.logStore = logStore;
        this.sendAppendFlows = sendAppendFlows;
    }

    /**
     * 提交一条命令到 Raft 日志。
     *
     * <p>调用者应使用返回的日志索引配合 {@code waitForApply} 确认命令已被状态机应用。
     * 返回 -1 表示当前节点不是 Leader 或数据为空，命令未写入日志。</p>
     *
     * @param data 序列化后的命令数据
     * @return 命令在日志中的索引（≥1），或 -1 表示失败
     */
    long propose(byte[] data) {
        if (ctx.currentState != NodeState.LEADER) {
            logger.warn("Only leader can propose commands");
            return -1;
        }

        if (data == null || data.length == 0) {
            logger.warn("Cannot propose empty data");
            return -1;
        }

        long newIndex = logStore.getLastLogIndex() + 1;
        CommandLogEntry entry = CommandLogEntry.create(newIndex, ctx.term.getCurrent(), ctx.nodeId, data);
        logStore.appendEntry(entry);

        logger.debug("Leader {} added entry at index {}, log size now {}", ctx.nodeId, newIndex, ctx.log.size());

        sendAppendFlows.run();

        logger.debug("Leader {} proposed command at index {}", ctx.nodeId, newIndex);
        return newIndex;
    }
}
