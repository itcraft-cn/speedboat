package cn.itcraft.speedboat.raft;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public enum NodeState {
    
    LEADER,
    FOLLOWER,
    CANDIDATE;
    
    private Set<NodeState> validNextStates;
    
    static {
        LEADER.validNextStates = new HashSet<>(Arrays.asList(FOLLOWER));
        FOLLOWER.validNextStates = new HashSet<>(Arrays.asList(CANDIDATE, FOLLOWER));
        CANDIDATE.validNextStates = new HashSet<>(Arrays.asList(LEADER, FOLLOWER, CANDIDATE));
    }
    
    public boolean canTransitionTo(NodeState next) {
        return validNextStates.contains(next);
    }
    
    public NodeState[] getValidNextStates() {
        return validNextStates.toArray(new NodeState[0]);
    }
}