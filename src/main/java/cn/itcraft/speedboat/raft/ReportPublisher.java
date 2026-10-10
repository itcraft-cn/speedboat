package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.raft.report.RaftNodeReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 状态报告发布器（内部协作器，非公开 API）。
 *
 * <p>职责唯一：构建不可变 {@code RaftNodeReport} 并经 listener 发布（事件触发 +
 * 周期兜底双通道，对标 MicroRaft ReportPublishPeriodSecs）。无 listener 时 no-op；
 * 监听器异常只告警不影响算法。</p>
 *
 * <p>并发契约：仅限 raft 单线程调用（快照收集点）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class ReportPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ReportPublisher.class);

    private final NodeContext ctx;

    ReportPublisher(NodeContext ctx) {
        this.ctx = ctx;
    }

    /**
     * 构建并发布一次状态快照（raft 单线程内收集，快照值不可变）。
     */
    private RaftNodeReport snapshot() {
        List<String> all = new ArrayList<>(ctx.peerIds);
        if (!all.contains(ctx.nodeId)) {
            all.add(ctx.nodeId);
        }
        long lastLogIndex = ctx.log.isEmpty() ? 0 : ctx.log.get(ctx.log.size() - 1).getIndex();
        return new RaftNodeReport(
            ctx.nodeId, ctx.currentState, ctx.term.getCurrent(), ctx.votedFor, ctx.leaderId,
            ctx.commitIndex, ctx.lastApplied, (int) lastLogIndex,
            all, new HashMap<>(ctx.matchIndex));
    }

    void publish(RaftNodeReport.ReportReason reason) {
        if (ctx.reportListener == null) {
            return;
        }
        try {
            ctx.reportListener.onReport(snapshot());
        } catch (Exception e) {
            logger.warn("Report listener failed: {}", e.toString(), e);
        } finally {
            logger.debug("Node {} published report reason={}", ctx.nodeId, reason);
        }
    }
}
