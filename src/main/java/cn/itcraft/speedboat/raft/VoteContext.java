package cn.itcraft.speedboat.raft;

/**
 * 投票上下文类，封装投票决策所需的所有相关信息。
 * 
 * <p>用于 {@link VoteWeightStrategy} 计算投票权重时提供决策上下文。</p>
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class VoteContext {
    
    private final String candidateId;
    private final String candidateDatacenter;
    private final boolean startupFirst;
    private final boolean electionInitiator;
    private final long term;
    
    public VoteContext(String candidateId, String candidateDatacenter, 
                       boolean startupFirst, boolean electionInitiator, long term) {
        this.candidateId = candidateId;
        this.candidateDatacenter = candidateDatacenter;
        this.startupFirst = startupFirst;
        this.electionInitiator = electionInitiator;
        this.term = term;
    }
    
    public String getCandidateId() { 
        return candidateId; 
    }
    
    public String getCandidateDatacenter() { 
        return candidateDatacenter; 
    }
    
    public boolean isStartupFirst() { 
        return startupFirst; 
    }
    
    public boolean isElectionInitiator() { 
        return electionInitiator; 
    }
    
    public long getTerm() { 
        return term; 
    }
    
    public static VoteContext forCandidate(String candidateId, long term) {
        return new VoteContext(candidateId, "", false, false, term);
    }
    
    public static VoteContext forDatacenter(String candidateId, String datacenter, long term) {
        return new VoteContext(candidateId, datacenter, false, false, term);
    }
}