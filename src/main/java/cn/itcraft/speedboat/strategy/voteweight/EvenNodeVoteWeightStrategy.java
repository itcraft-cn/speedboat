package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;

public class EvenNodeVoteWeightStrategy implements VoteWeightStrategy {
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        int weight = 0;
        if (context.isStartupFirst()) weight++;
        if (context.isElectionInitiator()) weight++;
        return weight;
    }
}