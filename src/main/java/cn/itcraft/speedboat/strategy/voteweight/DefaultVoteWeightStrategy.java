package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;

/**
 * DefaultVoteWeightStrategy 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class DefaultVoteWeightStrategy implements VoteWeightStrategy {
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        return 0;
    }
}