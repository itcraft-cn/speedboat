package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;

/**
 * EvenNodeVoteWeightStrategy 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class EvenNodeVoteWeightStrategy implements VoteWeightStrategy {
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        int weight = 0;
        if (context.isStartupFirst()) weight++;
        if (context.isElectionInitiator()) weight++;
        return weight;
    }
}