package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;

public class DefaultVoteWeightStrategy implements VoteWeightStrategy {
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        return 0;
    }
}