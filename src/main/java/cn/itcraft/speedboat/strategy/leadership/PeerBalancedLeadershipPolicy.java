package cn.itcraft.speedboat.strategy.leadership;

/**
 * 三机房对等策略（<b>缺省</b>）：各机房地位平等、先到先得，sticky 照旧拦截。
 *
 * <p>零依赖、零行为变更：这项策略对单机房、双机房、三机房对等以及任何未显式配置
 * {@code crossdc.leadership=dominant} 的部署，都与引入本策略前逐字节等价。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
public final class PeerBalancedLeadershipPolicy implements LeadershipPolicy {

    private static final PeerBalancedLeadershipPolicy INSTANCE = new PeerBalancedLeadershipPolicy();

    private PeerBalancedLeadershipPolicy() {
    }

    /** @return 单例（无状态） */
    public static PeerBalancedLeadershipPolicy getInstance() {
        return INSTANCE;
    }

    @Override
    public String mode() {
        return MODE_PEER;
    }

    @Override
    public boolean stickyMayYield(String candidateDatacenter, String incumbentLeaderDatacenter) {
        // 对等语义：现任健康即拒（现状），任何机房（含权重更高者）都不夺回
        return false;
    }

    @Override
    public String toString() {
        return "PeerBalancedLeadershipPolicy";
    }
}
