package cn.itcraft.speedboat.strategy.consistency;

import java.util.Collections;
import java.util.Set;

/**
 * 一致性策略接口：决定跨机房级联父组在"对侧机房失联"时的行为。
 *
 * <p>存在意义：CP（强一致唯一性）与 AP（可用性优先、允许短暂双主）是两种互斥的取舍，
 * 不能靠运行期协商，必须在<b>启动时</b>由配置选定并在整个生命周期内恒定。把这组取舍
 * 收敛为策略接口，调用点（{@code HeartbeatWatchdog}、{@code ElectionCoordinator}、
 * {@code QuorumCalculator}）只负责在各自决策点"问策略该怎么办"，不各自硬编码分支——
 * 这样 CP 与 AP 共享同一条主干，新增第三种策略（如按链路质量、按人工确认）只需新增实现。</p>
 *
 * <p>策略实现：</p>
 * <ul>
 *   <li>{@link CpConsistencyPolicy}：<b>缺省</b>。硬唯一性——父组分母恒定、绝不剔除机房、
 *       永不进入降级态。对侧整机房死亡时退化为"无主"，绝不双主。</li>
 *   <li>{@link ApConsistencyPolicy}：可用性优先——对侧机房失联超过阈值后进入降级态，
 *       把对侧机房剔除出法定多数分母，使本机房可单方成主。代价是分区期间可能短暂双主。</li>
 * </ul>
 *
 * <p><b>并发契约</b>：全部方法仅在 raft 单线程内调用（Actor 模型），实现无需加锁。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
public interface ConsistencyPolicy {

    /** 策略模式标识：强一致唯一性 */
    String MODE_CP = "cp";

    /** 策略模式标识：可用性优先（允许短暂双主） */
    String MODE_AP = "ap";

    /**
     * 策略模式标识（{@link #MODE_CP} / {@link #MODE_AP}）。
     * 供日志与监控区分口径——AP 下 {@code isMain} 全网计数可能短暂为 2，告警阈值需按模式区分。
     *
     * @return 模式标识
     */
    String mode();

    /**
     * 是否允许"降级接管"（把失联机房剔除出多数派分母，使本机房可单方成主）。
     *
     * <p>CP 恒 {@code false}，是其"绝不双主"承诺的执行开关；AP 为 {@code true}。</p>
     *
     * @return true 表示允许降级接管
     */
    boolean allowDegradedTakeover();

    /**
     * 降级判定阈值：对侧机房持续无应答多久后才允许进入降级态。
     *
     * <p>必须显著大于 check-quorum 新鲜阈值（5s）与跨机房选举超时上界，避免链路抖动误接管。
     * CP 下该值不参与任何判定。</p>
     *
     * @return 阈值毫秒
     */
    long degradedTimeoutMs();

    /**
     * 当前是否处于降级态。
     *
     * @return true 表示已进入降级态（分母已收缩）
     */
    boolean isDegraded();

    /**
     * 当前应从法定多数分母中<b>剔除</b>的机房集合（只读视图）。
     *
     * <p>CP 恒返回空集——这正是 CP 路径与引入本策略前<b>逐字节等价</b>的保证：
     * 分母聚合逻辑读到空集即跳过剔除分支，行为不变。AP 在降级态返回对侧机房 ID。</p>
     *
     * @return 剔除集合（不可变；CP 为空集）
     */
    Set<String> excludedDatacenters();

    /**
     * 评估并推进降级态（两个降级点均调用，CP 恒为 no-op 且返回 false）。
     *
     * <p>进入条件（全部满足）：策略允许降级接管、本进程持有代表席位、存在明确的对侧机房、
     * 对侧机房已持续无应答达阈值。退出条件：对侧机房恢复应答（由调用方喂入的
     * {@code lastSeenOppositeNanos} 变新鲜）。进入与退出均落 WARN 级审计日志。</p>
     *
     * @param selfDatacenter        本机房标识
     * @param oppositeDatacenter    对侧机房标识（同机房集群为 null，永不降级）
     * @param seatHeld              本进程是否持有本机房代表席位
     * @param lastSeenOppositeNanos 最近一次看到对侧机房存活的时间戳（纳秒）。
     *                              follower 喂 {@code lastHeartbeatNanos}（父组 Leader 在对侧机房）；
     *                              leader 喂 {@code lastResponseNanos} 中对侧 peer 的最大值。
     * @param nowNanos              当前 {@code System.nanoTime()}
     * @return 本次评估后是否处于降级态
     */
    boolean evaluateDegraded(String selfDatacenter, String oppositeDatacenter,
                             boolean seatHeld, long lastSeenOppositeNanos, long nowNanos);

    /**
     * 退出降级态并复位（进程重启 / 机房恢复 / 显式回退时调用）。
     */
    void reset();

    /**
     * 空剔除集合的共享不可变实例（CP 实现与未启用策略时的兜底返回值）。
     *
     * @return 空的不可变集合
     */
    static Set<String> emptyExcluded() {
        return Collections.emptySet();
    }
}
