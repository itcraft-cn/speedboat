package cn.itcraft.speedboat.strategy.membership.impl;

import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.raft.MemberChangeEntry;
import cn.itcraft.speedboat.strategy.membership.ChangeValidationStrategy;

/**
 * 默认变更验证策略实现，提供基本的成员变更验证逻辑。
 * 
 * <p>实现 {@link ChangeValidationStrategy} 接口，包含 Leader 检查、仲裁可用性验证等基础规则。</p>
 * 
 * @author speedboat
 * @since 1.0.0
 */
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