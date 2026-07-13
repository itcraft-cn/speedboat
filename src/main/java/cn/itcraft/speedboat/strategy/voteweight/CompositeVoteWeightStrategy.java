package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CompositeVoteWeightStrategy implements VoteWeightStrategy {
    
    private final List<VoteWeightStrategy> strategies;
    
    public CompositeVoteWeightStrategy(VoteWeightStrategy... strategies) {
        this.strategies = Collections.unmodifiableList(Arrays.asList(strategies));
    }
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        return strategies.stream()
                .mapToInt(s -> s.calculateAdditionalWeight(context))
                .sum();
    }
}