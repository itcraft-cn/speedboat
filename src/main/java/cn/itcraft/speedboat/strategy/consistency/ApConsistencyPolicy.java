package cn.itcraft.speedboat.strategy.consistency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 可用性优先策略：对侧机房失联超阈值后进入降级态，把对侧机房剔除出多数派分母，
 * 使本机房可单方成主。代价是<b>分区期间可能短暂双主</b>——这是 AP 明示接受的取舍。
 *
 * <p><b>降级判定（进入条件，全部满足）：</b></p>
 * <ol>
 *   <li>{@link #allowDegradedTakeover()} 恒 true（本策略的存在意义）；</li>
 *   <li>本进程持有本机房代表席位（{@code seatHeld}），否则无从代表本机房成主；</li>
 *   <li>存在明确的对侧机房（同机房集群无对侧，永不降级）；</li>
 *   <li>对侧机房已持续无应答达 {@link #degradedTimeoutMs()}。</li>
 * </ol>
 *
 * <p><b>退出条件：</b>对侧机房恢复应答（喂入的 {@code lastSeenOppositeNanos} 变新鲜）。
 * 分区愈合后走标准 Raft 任期机制收敛——低任期 Leader 收到高任期消息后降级，
 * 全网归一为单主。退出与进入均落 WARN 级审计日志，便于事后追溯双主窗口。</p>
 *
 * <p><b>并发契约：</b>仅在 raft 单线程内调用（Actor 模型），无需加锁；
 * 剔除集合以不可变快照对外暴露，读方（{@code QuorumCalculator}）零同步成本。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
public final class ApConsistencyPolicy implements ConsistencyPolicy {

    private static final Logger logger = LoggerFactory.getLogger(ApConsistencyPolicy.class);

    /** 缺省降级阈值：显著大于 check-quorum 新鲜阈值(5s)与跨机房选举超时上界(5s) */
    public static final long DEFAULT_DEGRADED_TIMEOUT_MS = 10_000L;

    private final long degradedTimeoutNanos;

    /** 当前是否处于降级态（raft 单线程独占写） */
    private volatile boolean degraded;

    /** 降级态下应剔除的机房集合（raft 单线程独占写；对外以不可变快照读） */
    private volatile Set<String> excludedDatacenters = ConsistencyPolicy.emptyExcluded();

    /** 进入降级态时所剔除的机房（供审计日志与退出判定回溯） */
    private String degradedAgainst;

    /** 进入降级态时的本机房标识（供 reset/退出审计日志回填机房字段） */
    private String degradedSelfDatacenter;

    /**
     * @param degradedTimeoutMs 对侧机房持续无应答多久后允许降级接管；&lt;=0 时取缺省 10s
     */
    public ApConsistencyPolicy(long degradedTimeoutMs) {
        this.degradedTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(
            degradedTimeoutMs > 0 ? degradedTimeoutMs : DEFAULT_DEGRADED_TIMEOUT_MS);
    }

    @Override
    public String mode() {
        return MODE_AP;
    }

    @Override
    public boolean allowDegradedTakeover() {
        return true;
    }

    @Override
    public long degradedTimeoutMs() {
        return TimeUnit.NANOSECONDS.toMillis(degradedTimeoutNanos);
    }

    @Override
    public boolean isDegraded() {
        return degraded;
    }

    @Override
    public Set<String> excludedDatacenters() {
        // 读方（QuorumCalculator 按机房聚合分支）只需遍历判断包含，
        // 不可变快照保证 raft 单线程写入与读取之间无数据竞争
        return excludedDatacenters;
    }

    @Override
    public boolean evaluateDegraded(String selfDatacenter, String oppositeDatacenter,
                                    boolean seatHeld, long lastSeenOppositeNanos, long nowNanos) {
        // 无对侧机房（同机房集群）或无席位：不参与降级，保持原分母
        if (oppositeDatacenter == null || oppositeDatacenter.equals(selfDatacenter) || !seatHeld) {
            if (degraded) {
                // 条件不再满足（如失去席位）：主动退出，避免带着失效前提继续收缩分母
                exitDegraded(selfDatacenter, "precondition-no-longer-met");
            }
            return degraded;
        }

        long silentNanos = nowNanos - lastSeenOppositeNanos;
        if (!degraded) {
            if (silentNanos >= degradedTimeoutNanos) {
                enterDegraded(selfDatacenter, oppositeDatacenter, silentNanos);
            }
        } else if (silentNanos < degradedTimeoutNanos) {
            // 对侧恢复应答：退出降级，分母回到全量，标准 Raft 任期机制接管收敛
            exitDegraded(selfDatacenter, "opposite-datacenter-recovered");
        }
        return degraded;
    }

    /**
     * 进入降级态：把对侧机房加入剔除集合，分母收缩，本机房可单方成主。
     *
     * <p>WARN 级审计：降级接管是"可能产生双主"的关键动作，必须可追溯到机房、时长与阈值。</p>
     *
     * @param selfDatacenter     本机房标识
     * @param oppositeDatacenter 对侧（失联）机房标识
     * @param silentNanos        对侧已持续无应答时长（纳秒）
     */
    private void enterDegraded(String selfDatacenter, String oppositeDatacenter, long silentNanos) {
        Set<String> next = new HashSet<String>(2);
        next.add(oppositeDatacenter);
        this.excludedDatacenters = Collections.unmodifiableSet(next);
        this.degradedAgainst = oppositeDatacenter;
        this.degradedSelfDatacenter = selfDatacenter;
        this.degraded = true;
        logger.warn("CONSISTENCY AUDIT: node dc={} ENTERING DEGRADED takeover mode, "
                + "opposite dc={} silent for {}ms (>= {}ms), excluding it from quorum denominator. "
                + "Brief dual-leader is now possible until partition heals.",
            selfDatacenter, oppositeDatacenter,
            TimeUnit.NANOSECONDS.toMillis(silentNanos), degradedTimeoutMs());
    }

    /**
     * 退出降级态：恢复全量分母，标准 Raft 任期机制接管收敛。
     *
     * @param selfDatacenter 本机房标识
     * @param reason         退出原因（审计用）
     */
    private void exitDegraded(String selfDatacenter, String reason) {
        String against = degradedAgainst;
        this.degraded = false;
        this.excludedDatacenters = ConsistencyPolicy.emptyExcluded();
        this.degradedAgainst = null;
        this.degradedSelfDatacenter = null;
        logger.warn("CONSISTENCY AUDIT: node dc={} EXITING DEGRADED takeover mode against dc={} reason={}. "
                + "Quorum denominator restored to full; standard Raft term convergence takes over.",
            selfDatacenter, against, reason);
    }

    @Override
    public void reset() {
        if (degraded) {
            // 复位沿用进入降级时记录的本机房标识，保证审计日志机房字段不为空
            exitDegraded(degradedSelfDatacenter, "reset");
        }
    }

    @Override
    public String toString() {
        return "ApConsistencyPolicy{timeoutMs=" + degradedTimeoutMs() + ", degraded=" + degraded
            + ", excluded=" + excludedDatacenters + "}";
    }
}
