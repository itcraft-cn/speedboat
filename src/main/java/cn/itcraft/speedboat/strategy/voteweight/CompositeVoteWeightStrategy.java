package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 复合投票权重策略，组合多个权重策略的结果。
 * 
 * <p>将多个 {@link VoteWeightStrategy} 的权重计算结果累加，实现策略组合。</p>
 * 
 * @author speedboat
 * @since 1.0.0
 */
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