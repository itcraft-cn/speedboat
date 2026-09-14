package cn.itcraft.speedboat.raft.report;

/**
 * Raft 节点报告监听器（Phase E，对标 MicroRaft report/RaftNodeReportListener）。
 *
 * <p>核心算法状态外化的唯一通道：核心零依赖任何 metrics 库，
 * 监控实现（Micrometer/Flink 日志等）通过注册本接口接入，
 * 在 {@code RaftNode.Builder} 的 {@code reportListener(...)} 完成。</p>
 *
 * <p><b>调用契约：</b>在 raft 单消费者线程上回调——实现应快速消费
 * （打印/计数），不得阻塞（否则拖慢心跳与选举）。</p>
 *
 * @author speedboat
 * @see RaftNodeReport
 * @since 1.1.0
 */
public interface RaftNodeReportListener {

    /** 收到一次快照报告（raft 线程内同步回调，请勿阻塞） */
    void onReport(RaftNodeReport report);
}
