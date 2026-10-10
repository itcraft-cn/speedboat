package cn.itcraft.speedboat.strategy.leadership;

import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;

/**
 * 主机房主导策略（dominant）：sticky 为权重严格更高的候选者让位。
 *
 * <p>几何：三机房"同城双备（机房权重 3）＋ 异地灾备（权重 2/2）"语义下必须指定主机房，
 * 高权重机房优先级必须真实兑现——不只是初始选举的纸面优势，还包括：
 * 初始竞速万一被低权重机房抢先（它按"同权重互投"凑齐 required 的合法路径）,
 * 主机房也要能<b>单向</b>夺回。夺回路径：sticky 让位 → PreVote 放行 → 候选者以
 * {@code term+1} 发起正式选举 → 备房 follower 视 term 更高对齐授票 → 主机房成主、
 * 低权重 Leader 退位。</p>
 *
 * <p><b>严格单向（数学保证无乒乓）</b>：候选者机房权重必须<b>严格大于</b>现任 Leader
 * 机房权重才让位。同权重（两对等备房之间）与低权重一律拒绝——低权重方永远无法
 * 反向夺回，全网最终收敛到最高权重机房的一方，与 {@code DatacenterPriorityVoteWeightStrategy}
 * 的 shouldGrantVote 闸门（只投给不低于本机房的候选者）形成一致闭环。</p>
 *
 * <p>权重快照来自注入的 {@link VoteWeightStrategy#dcWeight(String)}——同样遵守
 * "机房属性、与观察者无关"语义，并随优先级表的热更新（人工提升/回退）自动变化。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
public final class DominantDatacenterLeadershipPolicy implements LeadershipPolicy {

    private final VoteWeightStrategy weightStrategy;

    /**
     * @param weightStrategy 机房权重策略（仅消费其 {@code dcWeight} 比较口径；null 视为缺省权重 1）
     */
    public DominantDatacenterLeadershipPolicy(VoteWeightStrategy weightStrategy) {
        this.weightStrategy = weightStrategy;
    }

    @Override
    public String mode() {
        return MODE_DOMINANT;
    }

    @Override
    public boolean stickyMayYield(String candidateDatacenter, String incumbentLeaderDatacenter) {
        if (candidateDatacenter == null || incumbentLeaderDatacenter == null
            || candidateDatacenter.equals(incumbentLeaderDatacenter)) {
            return false;
        }
        int candidateWeight = weightStrategy != null
            ? weightStrategy.dcWeight(candidateDatacenter) : 1;
        int incumbentWeight = weightStrategy != null
            ? weightStrategy.dcWeight(incumbentLeaderDatacenter) : 1;
        // 严格单向：高夺低可、低夺高不可（双向放开会乒乓震荡，term 反复翻车）
        return candidateWeight > incumbentWeight;
    }

    /**
     * dominant：仅当本机房权重严格高于现任 Leader 机房时，允许选举定时器不被心跳冻结
     * （持有"夺主探测权"）；否则照常冻结。权重并列/更低的机房必须冻结——
     * 两对等机房若都不冻结会乒乓震荡。
     */
    @Override
    public boolean freezeHeartbeatElectionTimer(String leaderDatacenter, String selfDatacenter) {
        if (leaderDatacenter == null || selfDatacenter == null
            || leaderDatacenter.equals(selfDatacenter)) {
            return true;
        }
        int selfWeight = weightStrategy != null ? weightStrategy.dcWeight(selfDatacenter) : 1;
        int leaderWeight = weightStrategy != null ? weightStrategy.dcWeight(leaderDatacenter) : 1;
        // false = 不冻结（定时器到期可发起夺主探测）；仅 self 严格高于 leader 时 False
        return selfWeight <= leaderWeight;
    }

    @Override
    public String toString() {
        return "DominantDatacenterLeadershipPolicy";
    }
}
