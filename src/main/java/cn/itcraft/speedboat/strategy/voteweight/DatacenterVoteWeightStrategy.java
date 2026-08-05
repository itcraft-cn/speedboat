package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;
import java.util.Objects;

/**
 * DatacenterVoteWeightStrategy 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class DatacenterVoteWeightStrategy implements VoteWeightStrategy {
    
    private final String localDatacenter;
    
    public DatacenterVoteWeightStrategy(String localDatacenter) {
        this.localDatacenter = localDatacenter;
    }
    
    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        String candidateDc = context.getCandidateDatacenter();
        if (candidateDc == null || candidateDc.isEmpty()) {
            return 0;
        }
        
        if (Objects.equals(candidateDc, localDatacenter)) {
            return 2;
        }
        return -1;
    }
}