package cn.itcraft.speedboat.strategy.membership;

import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.raft.MemberChangeEntry;
import java.util.List;

public interface ChangeValidationStrategy {

    boolean canProposeChange(RaftNode raftNode, MemberChangeEntry proposedEntry);
    
    boolean shouldAcceptChange(RaftNode raftNode, MemberChangeEntry proposedEntry);
    
    default boolean isQuorumAvailable(RaftNode raftNode) {
        // 简化实现：假设总是有quorum
        return true;
    }
    
    default boolean validateAdd(String newPeerId, List<String> currentPeerIds) {
        // 基础验证：节点不能已存在
        return !currentPeerIds.contains(newPeerId);
    }
    
    default boolean validateRemove(String peerId, List<String> currentPeerIds) {
        // 基础验证：节点必须存在
        return currentPeerIds.contains(peerId);
    }
}