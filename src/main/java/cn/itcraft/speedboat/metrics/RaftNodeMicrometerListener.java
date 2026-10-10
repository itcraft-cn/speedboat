package cn.itcraft.speedboat.metrics;

import cn.itcraft.speedboat.raft.NodeState;
import cn.itcraft.speedboat.raft.report.RaftNodeReport;
import cn.itcraft.speedboat.raft.report.RaftNodeReportListener;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Micrometer 状态外挂（可选依赖；对标 Ratis metrics-* 分层 / MicroRaft microraft-metrics）。
 *
 * <p>用法：应用侧依赖 micrometer-core 后，把本类注册到 {@code reportListener(...)}：</p>
 * <pre>{@code
 * RaftNode node = RaftNode.builder()
 *     ...
 *     .reportListener(new RaftNodeMicrometerListener(registry))
 *     .build();
 * }</pre>
 *
 * <p><b>不声明该依赖时：</b>listener 类不进装配路径即零接触——micrometer 依赖
 * 为 {@code optional}，不向下游传递，保持 speedboat 核心"零 metrics 库依赖"。</p>
 *
 * <p>指标清单（对 Ratis 常用维度裁剪）：</p>
 * <ul>
 *   <li>speedboat.raft.term / commitIndex / lastApplied / lastLogIndex：单调进度</li>
 *   <li>speedboat.raft.leader：角色标记（LEADER=1，其余 0）——丢主预警最直接</li>
 *   <li>speedboat.raft.followerMatch：follower matchIndex（tag=peerId）——落后度</li>
 * </ul>
 *
 * <p><b>调用契约：</b>在 raft 单消费者线程回调——实现只登记 Gauge 与快照引用
 * （volatile引用替换，无锁竞争），Gauge 采集值由 metrics 侧光时再取。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class RaftNodeMicrometerListener implements RaftNodeReportListener {

    private static final Logger logger = LoggerFactory.getLogger(RaftNodeMicrometerListener.class);

    private final MeterRegistry registry;
    /** follower matchIndex Gauge 存储（member 变更时增删；peer 维 tag） */
    private final Map<String, Gauge> followerGauges = new ConcurrentHashMap<>();

    /** 最新快照（raft 线程写；Gauge supplier 读——volatile 单字段替换无锁） */
    private volatile RaftNodeReport latest;

    public RaftNodeMicrometerListener(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void onReport(RaftNodeReport report) {
        boolean first = latest == null;
        latest = report;

        if (first) {
            registerCoreGauges();
        }
        refreshFollowerGauges(report);
    }

    /** 首份报告：登记基础进度 Gauges（ nodeId tag 取自快照，nodeId 静态故一次性） */
    private void registerCoreGauges() {
        RaftNodeReport snapshot = latest;
        String base = "speedboat.raft";
        String nodeId = snapshot == null ? "unknown" : snapshot.getNodeId();
        // 报告002 L-6：基础 Gauge 统一带 nodeId tag（同 registry 多节点会重复注册冲突）
        Gauge.builder(base + ".term", this, l -> valueOf(latest.getTerm()))
            .description("Current Raft term")
            .tag("nodeId", nodeId)
            .register(registry);
        Gauge.builder(base + ".commitIndex", this, l -> valueOf(latest.getCommitIndex()))
            .tag("nodeId", nodeId)
            .register(registry);
        Gauge.builder(base + ".lastApplied", this, l -> valueOf(latest.getLastApplied()))
            .tag("nodeId", nodeId)
            .register(registry);
        Gauge.builder(base + ".lastLogIndex", this, l -> valueOf(latest.getLastLogIndex()))
            .tag("nodeId", nodeId)
            .register(registry);
        Gauge.builder(base + ".leader", this,
                l -> latest != null && latest.getRole() == NodeState.LEADER ? 1d : 0d)
            .tag("nodeId", nodeId)
            .register(registry);
        logger.info("Speedboat Micrometer listener initialized (base={}, nodeId={})",
            base, snapshot == null ? "unknown" : snapshot.getNodeId());
    }

    /** follower matchIndex Gauge 补删：按本次 report 的 member 集合同步增删 */
    private void refreshFollowerGauges(RaftNodeReport report) {
        Map<String, Long> current = report.getFollowerMatchIndices();
        for (String registered : followerGauges.keySet().toArray(new String[0])) {
            if (!current.containsKey(registered)) {
                Gauge g = followerGauges.remove(registered);
                if (g != null) {
                    registry.remove(g);
                }
            }
        }
        for (String peer : current.keySet()) {
            if (!followerGauges.containsKey(peer)) {
                Gauge gauge = Gauge.builder("speedboat.raft.followerMatch", peer,
                        p -> {
                            Long v = latest == null ? null : latest.getFollowerMatchIndices().get(p);
                            return v == null ? 0d : (double) v;
                        })
                    .tag("peer", peer)
                    .register(registry);
                followerGauges.put(peer, gauge);
            }
        }
    }

    private static double valueOf(long v) {
        return v;
    }

    /** 便捷工厂：显式 registry 传入 */
    public static RaftNodeMicrometerListener of(MeterRegistry registry) {
        return new RaftNodeMicrometerListener(registry);
    }
}
