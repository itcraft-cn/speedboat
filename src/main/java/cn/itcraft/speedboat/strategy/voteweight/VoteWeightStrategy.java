package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;

public interface VoteWeightStrategy {
    
    int calculateAdditionalWeight(VoteContext context);
}