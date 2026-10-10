package cn.itcraft.speedboat.strategy.consistency;

import java.util.Set;

/**
 * 强一致唯一性策略（<b>缺省</b>）：父组分母恒定、绝不剔除机房、永不降级接管。
 *
 * <p>这是 CP 的执行体，也是"绝不双主"承诺的实现。核心不变式：{@link #excludedDatacenters()}
 * 恒返回空集，因此 {@code QuorumCalculator} 的按机房聚合分支读到空集即跳过剔除，
 * 全部权重/多数派计算与引入本策略<b>之前逐字节等价</b>——既有单机房与六机实机回归必须原样通过。</p>
 *
 * <p>代价：对侧整机房死亡或失联时，本机房 {@code self < required} 永远凑不齐多数，
 * 退化为<b>无主</b>（0 主），而非降级接管。这是唯一性硬约束在两故障域下的必然结果，
 * 非缺陷。需要人工介入时走 {@code Speedboat.promoteDatacenter} 运维兜底。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
public final class CpConsistencyPolicy implements ConsistencyPolicy {

    private static final CpConsistencyPolicy INSTANCE = new CpConsistencyPolicy();

    /**
     * 单例（无状态）。策略本身不携带任何运行期状态，全部判定都是恒定值，
     * 故共享同一实例即可，也便于在日志/监控中以 identity 判断"当前是否 CP"。
     *
     * @return CP 策略单例
     */
    public static CpConsistencyPolicy getInstance() {
        return INSTANCE;
    }

    private CpConsistencyPolicy() {
    }

    @Override
    public String mode() {
        return MODE_CP;
    }

    @Override
    public boolean allowDegradedTakeover() {
        return false;
    }

    @Override
    public long degradedTimeoutMs() {
        // CP 不参与降级判定，返回 0 仅作占位（调用方以 allowDegradedTakeover 短路）
        return 0L;
    }

    @Override
    public boolean isDegraded() {
        return false;
    }

    @Override
    public Set<String> excludedDatacenters() {
        // CP 的唯一不变式：恒不剔除任何机房 → 分母恒定 → 结构性排除双主
        return ConsistencyPolicy.emptyExcluded();
    }

    @Override
    public boolean evaluateDegraded(String selfDatacenter, String oppositeDatacenter,
                                    boolean seatHeld, long lastSeenOppositeNanos, long nowNanos) {
        // 恒 no-op：CP 不进入降级态，返回 false 表示"未降级，按原分母判定"
        return false;
    }

    @Override
    public void reset() {
        // 无状态，无需复位
    }

    @Override
    public String toString() {
        return "CpConsistencyPolicy";
    }
}
