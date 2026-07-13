package cn.itcraft.speedboat.strategy.membership.impl;

import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.raft.MemberChangeEntry;
import cn.itcraft.speedboat.strategy.membership.ChangeValidationStrategy;

public class DefaultChangeValidationStrategy implements ChangeValidationStrategy {

    @Override
    public boolean canProposeChange(RaftNode raftNode, MemberChangeEntry proposedEntry) {
        if (!raftNode.isMain()) {
            return false;
        }
        
        if (!isQuorumAvailable(raftNode)) {
            return false;
        }
        
        switch (proposedEntry.getChangeType()) {
            case ADD:
                // 简化实现：总是允许添加
                return true;
            case REMOVE:
                // 不允许移除自己
                return !proposedEntry.getNodeId().equals(raftNode.getNodeId());
            default:
                return false;
        }
    }

    @Override
    public boolean shouldAcceptChange(RaftNode raftNode, MemberChangeEntry proposedEntry) {
        if (!isQuorumAvailable(raftNode)) {
            return false;
        }
        
        switch (proposedEntry.getChangeType()) {
            case ADD:
                return true;
            case REMOVE:
                // 不允许移除自己
                return !proposedEntry.getNodeId().equals(raftNode.getNodeId());
            default:
                return false;
        }
    }
}