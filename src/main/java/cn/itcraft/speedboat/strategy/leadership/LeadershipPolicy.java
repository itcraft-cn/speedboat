package cn.itcraft.speedboat.strategy.leadership;

import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;

/**
 * 领袖优先级策略（三机房拓扑语义决策）。
 *
 * <p><b>存在意义：</b>三机房 3/2/2 下，主机房权重 3 物理上需要一张备房票才能成主，
 * 与"两备机房互投 2+2=4"竞速同速。若初始父组选举被低权重机房抢先，父组 PreVote 的
 * "现任 Leader 心跳健康即拒"sticky 会把高权重候选者的夺回通道<b>永久堵死</b>
 * （term 膨胀防护以"不再选"为前提）。该行为对两种拓扑的取舍不同：</p>
 * <ul>
 *   <li><b>同城双备 / 异地灾备模式（dominant）</b>：运维必须指定主机房，
 *       高权重机房优先，sticky 需为"权重严格更高的候选者"让位——否则主机房
 *       优先级只是纸面概念；</li>
 *   <li><b>三机房对等模式（peer）</b>：各房地位平等、先到先得，sticky 照旧拦截
 *       （现状语义，不折腾）。</li>
 * </ul>
 *
 * <p>把"是否让位"收敛为策略接口，选举侧只在 sticky 判定点"问策略该怎么办"，
 * 不掺一点拓扑知识。决策必须<b>单向严格</b>：只有"候选者机房权重 &gt; 现任
 * Leader 机房权重"才让位——高夺低可、低夺高不可，无乒乓，全网收敛到最高权重机房。</p>
 *
 * <p><b>并发契约：</b>仅在 raft 单线程内调用（与 {@code VoteWeightStrategy} 相同）。</p>
 *
 * @author speedboat
 * @see PeerBalancedLeadershipPolicy
 * @see DominantDatacenterLeadershipPolicy
 * @since 1.2.0
 */
public interface LeadershipPolicy {

    /** 模式标识：三机房对等（先到先得，sticky 照旧拦截） */
    String MODE_PEER = "peer";

    /** 模式标识：主机房主导（同城双备 / 异地灾备，主机房必须指定且单向夺回） */
    String MODE_DOMINANT = "dominant";

    /** 模式标识（{@link #MODE_PEER} / {@link #MODE_DOMINANT}），供日志与监控区分。 */
    String mode();

    /**
     * 现任 Leader 心跳健康（sticky 生效）时，是否应为该候选者让位。
     *
     * <p>实现必须满足：</p>
     * <ul>
     *   <li>对等模式恒 false——零行为变更（历史自适应）；</li>
     *   <li>主导模式仅当 {@code candidateWeight > incumbentWeight} 时 true，
     *       同权重 / 低权重一律 false（否则乒乓震荡）。</li>
     * </ul>
     *
     * @param candidateDatacenter      候选者所在机房标识（可为 null；未自报按同机房处理）
     * @param incumbentLeaderDatacenter 现任 Leader 所在机房标识（自身为 Leader 时已由
     *                                  sticky 第一分支短路，不会带 null 进本方法）
     * @return true 表示 sticky 让位（PreVote 放行且后续正式选举 term 推高，接管路径完整）
     */
    boolean stickyMayYield(String candidateDatacenter, String incumbentLeaderDatacenter);

    /**
     * 缺省策略：三机房对等（peer），零行为变更。
     *
     * <p>无状态、无依赖，单机房 / 双机房 / 一切未显式配置的部署沿历史行为不变。</p>
     *
     * @return 缺省策略单例
     */
    static LeadershipPolicy defaultPolicy() {
        return PeerBalancedLeadershipPolicy.getInstance();
    }

    /**
     * 主导房策略工厂：机器权重策略注入，比较算法由权重表最新快照承载。
     *
     * @param weightStrategy 机房权重策略（{@code dcWeight} 为唯一消费点）
     * @return 主导房策略
     */
    static LeadershipPolicy dominant(VoteWeightStrategy weightStrategy) {
        return new DominantDatacenterLeadershipPolicy(weightStrategy);
    }
}
