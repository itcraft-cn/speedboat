package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;

/**
 * EvenNodeVoteWeightStrategy 类。
 *
 * <p>语义说明（报告002 L-7 复核）：经 {@code PropertiesConfigProvider#getVoteWeightStrategy}
 * 分支 {@code vote.weight.strategy=even} 实际可达（旧报告"配置入口不可达"有误）；
 * 附加权重恒 0 —— 等价于"无策略"的均一投票语义，与 {@code DefaultVoteWeightStrategy}
 * 三向等价（null / none / even），无行为伤害，保留作为显式声明等权拓扑的用户可读路径。</p>
 *
 * @author speedboat
 * @since 1.0.0
 */
@Deprecated
public class EvenNodeVoteWeightStrategy implements VoteWeightStrategy {
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        int weight = 0;
        if (context.isStartupFirst()) weight++;
        if (context.isElectionInitiator()) weight++;
        return weight;
    }
}