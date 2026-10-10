package cn.itcraft.speedboat;
import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.strategy.consistency.ConsistencyPolicy;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable;
import cn.itcraft.speedboat.transport.NodeEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 人工切主网关（阶段五运维兜底的实例级编排体，非公开 API）。
 *
 * <p>职责（从 {@code Speedboat} 拆出，见 review 20261010-001 M1）：校验 + 连通性闸门 +
 * 委派父组 {@link RaftNode} + {@code PROMOTE-AUDIT} 结构化审计。 raft 核心不吞运维语义——
 * 本网关是门面侧的唯一把关层，与 {@code RaftNodeImpl} 内的同口径闸门构成纵深防御。</p>
 *
 * <h3>并发契约</h3>
 * <p>宿主单例状态（{@code parentRaftNode} 等）由门面维护；本类仅在宿主 running 时被调用，
 * 且仅在 start 线程写、其他线程读的散发布局下访问宿主字段（同包 private 直读）。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
final class PromoteGateway {

    private static final Logger logger = LoggerFactory.getLogger(PromoteGateway.class);

    /** 连通性探测超时（毫秒）：单端点短超时，避免闸门长时间阻塞调用方 */
    private static final int CONNECT_PROBE_TIMEOUT_MS = 1000;

    /** AP 模式下人工切主被忽略的审计 detail（固定文案，便于 grep 与告警归类） */
    private static final String MANUAL_SWITCH_IGNORED_DETAIL =
        "manual-switch-ignored-in-ap-mode";

    private final Speedboat host;

    PromoteGateway(Speedboat host) {
        this.host = host;
    }

    /**
     * 人工切主是否应被忽略（仅 AP 模式忽略；CP 与未注入策略按缺省 CP 放行）。
     *
     * <p>CP 是"只有一主或无主"的硬约束，人工切主是其运维兜底；AP 已显式接受
     * 分区期间短暂双主，"把某机房提为唯一主"在该模式下没有定义良好的语义，
     * 故一律忽略而非报错——避免运维脚本把"模式不适用"当故障反复重试。</p>
     *
     * <p>策略为 null（提供方未实现该方法）时按缺省 CP 放行，与 {@code NodeContext} 兜底一致。
     * 仅读 {@code mode()}，不触碰降级态，故任意线程可调用。</p>
     *
     * @param policy 启动时选定的一致性策略（可为 null）
     * @return true 表示该调用应被忽略
     */
    static boolean manualSwitchIgnored(ConsistencyPolicy policy) {
        return policy != null && ConsistencyPolicy.MODE_AP.equals(policy.mode());
    }

    /** 提升本机房在父组的优先级（校验 + 连通性闸门 + 委派 + 审计）。 */
    boolean doPromoteDatacenter(String operator, String datacenterId, String reason) {
        // ---- 模式闸门（最前）：AP 下人工切主一律忽略——不改状态、不抛异常、仅留痕 ----
        if (manualSwitchIgnored(host.policy())) {
            auditPromote("IGNORED", operator, datacenterId, reason, MANUAL_SWITCH_IGNORED_DETAIL, null, null);
            return false;
        }
        // ---- 校验：跨机房模式 + 操作者非空 + 目标机房匹配 ----
        if (!host.isCrossDatacenterMode() || host.parentRaftNodeOrNull() == null) {
            auditPromote("REJECTED", operator, datacenterId, reason, "not-cross-datacenter-mode", null, null);
            return false;
        }
        if (operator == null || operator.trim().isEmpty()) {
            auditPromote("REJECTED", operator, datacenterId, reason, "operator-required", null, null);
            return false;
        }
        if (datacenterId == null || !datacenterId.equals(host.dcId())) {
            auditPromote("REJECTED", operator, datacenterId, reason,
                "target-datacenter-mismatch(local=" + host.dcId() + ")", null, null);
            return false;
        }
        // ---- 连通性闸门：对侧机房父组端点任一可达即拒绝 ----
        if (anyOppositeParentEndpointReachable()) {
            auditPromote("REJECTED", operator, datacenterId, reason, "opposite-datacenter-still-reachable",
                null, null);
            return false;
        }
        // ---- 放行到父组 raft 线程：委派前后各捕获一次快照供审计 ----
        AuditState before = captureAuditState();
        long termLeap = host.config().getPromoteTermLeap();
        boolean ok = host.parentRaftNodeOrNull().promoteDatacenter(operator, termLeap, reason);
        AuditState after = captureAuditState();
        auditPromote(ok ? "ACCEPTED" : "EXEC-FAILED", operator, datacenterId, reason,
            "termLeap=" + termLeap, before, after);
        return ok;
    }

    /** 回退优先级表（校验 + 委派 + 审计）。回退不做连通性闸门（它降低优先级，不打开双主窗口）。 */
    boolean doRestoreDefaultPriorities(String operator, String reason) {
        // ---- 模式闸门（最前）：回退与提升同属人工切主通路，AP 下同样忽略 ----
        if (manualSwitchIgnored(host.policy())) {
            auditPromote("IGNORED", operator, null, reason, MANUAL_SWITCH_IGNORED_DETAIL, null, null);
            return false;
        }
        if (!host.isCrossDatacenterMode() || host.parentRaftNodeOrNull() == null) {
            auditPromote("REJECTED", operator, null, reason, "not-cross-datacenter-mode", null, null);
            return false;
        }
        if (operator == null || operator.trim().isEmpty()) {
            auditPromote("REJECTED", operator, null, reason, "operator-required", null, null);
            return false;
        }
        AuditState before = captureAuditState();
        boolean ok = host.parentRaftNodeOrNull().restoreDefaultPriorities(operator, reason);
        AuditState after = captureAuditState();
        auditPromote(ok ? "ACCEPTED" : "EXEC-FAILED", operator, null, reason, "restore-default", before, after);
        return ok;
    }

    /** 捕获当前父组 term 与优先级快照（供审计前后对比） */
    private AuditState captureAuditState() {
        if (host.parentRaftNodeOrNull() == null) {
            return new AuditState(0, 0, null);
        }
        DatacenterPriorityTable.Snapshot snap =
            host.parentRaftNodeOrNull().getPrioritySnapshot();
        long term = 0;
        try {
            term = host.parentRaftNodeOrNull().getTerm().getCurrent();
        } catch (Exception e) {
            // term 读取失败不影响审计主流程，但必须留痕（禁止空 catch）
            logger.debug("captureAuditState: term read failed for node {}", host.nodeId(), e);
        }
        return new AuditState(term, snap == null ? 0 : snap.epoch(), snap == null ? null : snap.weights());
    }

    /**
     * 连通性闸门：探对侧机房全部父组端点，<b>任一可达</b>返回 true（表示网络仍通，须拒绝提升）。
     *
     * <p>对每个"机房 != 本机房"的父组端点做 TCP 连接探测（短超时）。只要有一个能连上，
     * 说明主机房未死也未分区——此时提升会打开"双主"窗口。全部连不上才认为可安全提升。
     * 探测失败（连接被拒/超时）不视为可达。单机房模式无对侧端点，恒返回 false。</p>
     */
    private boolean anyOppositeParentEndpointReachable() {
        for (NodeEndpoint ep : host.parentPeerEndpoints()) {
            String peerDc = host.parentPeerDatacenters().get(ep.getNodeId());
            if (peerDc == null || peerDc.equals(host.dcId())) {
                continue; // 只探对侧机房
            }
            try (Socket sock = new Socket()) {
                sock.connect(new InetSocketAddress(ep.getHost(), ep.getPort()),
                    CONNECT_PROBE_TIMEOUT_MS);
                logger.info("Connectivity gate: opposite endpoint {}/{} (dc={}) reachable",
                    ep.getHost(), ep.getPort(), peerDc);
                return true;
            } catch (Exception e) {
                logger.debug("Connectivity gate: opposite endpoint {}/{} unreachable ({})",
                    ep.getHost(), ep.getPort(), e.toString());
            }
        }
        return false;
    }

    /**
     * 结构化审计日志（阶段五；每次提升/回退调用打一条，含前后 term/epoch/权重表）。
     * 固定字段顺序便于 grep：op/operator/node/dc/target/reason/detail/termBefore/termAfter/
     * epochBefore/epochAfter/weightsBefore/weightsAfter。
     */
    private void auditPromote(String op, String operator, String targetDc, String reason, String detail,
                              AuditState before, AuditState after) {
        long termBefore = before == null ? 0 : before.term;
        long termAfter = after == null ? 0 : after.term;
        long epochBefore = before == null ? 0 : before.epoch;
        long epochAfter = after == null ? 0 : after.epoch;
        Object weightsBefore = before == null ? null : before.weights;
        Object weightsAfter = after == null ? null : after.weights;
        // 文本字段净化：自由文本（operator/reason）可能携带 \r\n，会折断单行结构化审计、
        // 破坏"固定字段顺序便于 grep"的自身契约——统一转义为字面 \n 显示
        logger.warn("PROMOTE-AUDIT op={} operator={} node={} dc={} target={} reason={} detail={} "
                + "termBefore={} termAfter={} epochBefore={} epochAfter={} "
                + "weightsBefore={} weightsAfter={}",
            op, sanitizeForLog(operator), host.nodeId(), host.dcId(), sanitizeForLog(targetDc),
            sanitizeForLog(reason), sanitizeForLog(detail),
            termBefore, termAfter, epochBefore, epochAfter,
            weightsBefore, weightsAfter);
    }

    /** 日志文本净化：换行折叠为字面标记，保证审计单行完整（包内可见，宿主停机路径同用） */
    static String sanitizeForLog(String s) {
        return s == null ? null : s.replaceAll("[\\r\\n]", "\\\\n");
    }

    /** 审计用状态切片（term + 优先级快照）；父组不存在时全零/空 */
    private static final class AuditState {
        final long term;
        final long epoch;
        final Object weights;

        AuditState(long term, long epoch, Object weights) {
            this.term = term;
            this.epoch = epoch;
            this.weights = weights;
        }
    }
}
