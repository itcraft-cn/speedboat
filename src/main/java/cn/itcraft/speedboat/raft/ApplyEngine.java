package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 应用引擎（内部协作器，非公开 API）。
 *
 * <p>职责唯一：commitIndex/lastApplied 的推进水位管理与"日志条目 → 状态机"
 * 全序重放（COMMAND 交给 stateMachine.apply；MEMBER_CHANGE 交给
 * {@code MembershipManager.processMemberChange}；其他类型承载 leaderId，
 * 不做状态机改动），以及重放后的检查点判定。</p>
 *
 * <p>并发契约：仅限 raft 单线程调用。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class ApplyEngine {

    private static final Logger logger = LoggerFactory.getLogger(ApplyEngine.class);

    private final NodeContext ctx;
    private final Checkpointer checkpointer;
    /** 成员变更应用分派（由门面在布线期注入 MembershipManager） */
    private MembershipManager membershipManager;
    /** 日志仓（注册后提供 O(1) 条目查询；启动期布线注入） */
    private RaftLogStore logStore;
    /** 父侧独立优先级持久化（阶段五；可空，子组/单机房无持久化） */
    private cn.itcraft.speedboat.persistence.PriorityStore priorityStore;

    ApplyEngine(NodeContext ctx, Checkpointer checkpointer) {
        this.ctx = ctx;
        this.checkpointer = checkpointer;
    }

    void bind(MembershipManager membershipManager) {
        this.membershipManager = membershipManager;
    }

    void bind(RaftLogStore logStore) {
        this.logStore = logStore;
    }

    /** commitIndex 推进（含应用重放）。调用方负责真空断判定。 */
    void advanceToCommit(long index) {
        ctx.commitIndex = index;
        applyCommittedEntries();
    }

    /**
     * 全序重放已提交未应用条目：lastApplied → commitIndex 区间逐条 apply。
     * <ul>
     *   <li>COMMAND → stateMachine.apply（状态机可空）；</li>
     *   <li>MEMBER_CHANGE → membershipManager.processMemberChange；</li>
     *   <li>其他类型 → 仅承载 leaderId 视角推进（leader-entry）。</li>
     * </ul>
     * 重放完成后按阈值判定检查点。
     */
    void applyCommittedEntries() {
        while (ctx.lastApplied < ctx.commitIndex) {
            ctx.lastApplied++;
            LogEntry entry = findEntryAt(ctx.lastApplied);
            if (entry != null) {
                if (entry.getEntryType() == LogEntry.EntryType.COMMAND) {
                    StateMachine stateMachine = ctx.getStateMachine();
                    if (stateMachine != null) {
                        stateMachine.apply(entry);
                    }
                } else if (entry.getEntryType() == LogEntry.EntryType.MEMBER_CHANGE) {
                    dispatchMemberChange((MemberChangeEntry) entry);
                } else if (entry.getEntryType() == LogEntry.EntryType.PRIORITY_CHANGE) {
                    dispatchPriorityChange((PriorityChangeEntry) entry);
                } else {
                    ctx.leaderId = entry.getLeaderId();
                }
                logger.debug("Node {} applied entry at index {}: type={}",
                    ctx.nodeId, ctx.lastApplied, entry.getEntryType());
            }
        }
        checkpointer.maybeCheckpoint();
    }

    private LogEntry findEntryAt(long index) {
        if (logStore == null) {
            return null;
        }
        return logStore.getEntryAt(index);
    }

    private void dispatchMemberChange(MemberChangeEntry entry) {
        if (membershipManager == null) {
            logger.warn("Node {} member change arrived with no membership manager: index={}",
                ctx.nodeId, entry.getIndex());
            return;
        }
        membershipManager.processMemberChange(entry);
    }

    /**
     * 应用机房优先级变更条目（阶段五人工升级 API）。
     *
     * <p>把已提交的 {@link PriorityChangeEntry} 落到共享优先级表——仅当条目 epoch 高于
     * 本表当前 epoch 才采纳（幂等：重放已应用的低 epoch 条目不回灌）。落表后写入
     * 父侧独立持久化文件，保证重启不丢提升状态。优先级表为 null（子组/单机房）时跳过。</p>
     *
     * <p>并发契约：仅限 raft 单线程调用。</p>
     */
    private void dispatchPriorityChange(PriorityChangeEntry entry) {
        cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable table = ctx.priorityTable;
        if (table == null) {
            logger.debug("Node {} priority change applied with no priority table: index={}",
                ctx.nodeId, entry.getIndex());
            return;
        }
        cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable.Source source =
            "PROMOTED".equals(entry.source())
                ? cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable.Source.PROMOTED
                : cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable.Source.CONFIG;
        boolean adopted = table.convergeIfNewer(entry.epoch(), entry.weights(), source);
        if (adopted) {
            logger.info("Node {} adopted priority change epoch={} source={} weights={}",
                ctx.nodeId, entry.epoch(), entry.source(), entry.weights());
            if (priorityStore != null) {
                priorityStore.save(table.current());
            }
        } else {
            logger.debug("Node {} ignored stale priority change epoch={} (current={})",
                ctx.nodeId, entry.epoch(), table.current().epoch());
        }
    }

    /**
     * 注入父侧独立优先级持久化（启动期布线；可空——子组/单机房无持久化）。
     * 与 {@code RaftStore} 分离，不动其接口与契约。
     */
    void bindPriorityStore(cn.itcraft.speedboat.persistence.PriorityStore priorityStore) {
        this.priorityStore = priorityStore;
    }
}
