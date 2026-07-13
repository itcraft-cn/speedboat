package cn.itcraft.speedboat.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("NodeState 状态转换测试")
class NodeStateTest {

    @Test
    @DisplayName("LEADER 可以转换为 FOLLOWER")
    void leaderCanTransitionToFollower() {
        assertTrue(NodeState.LEADER.canTransitionTo(NodeState.FOLLOWER));
    }

    @Test
    @DisplayName("LEADER 不能转换为 CANDIDATE")
    void leaderCannotTransitionToCandidate() {
        assertFalse(NodeState.LEADER.canTransitionTo(NodeState.CANDIDATE));
    }

    @Test
    @DisplayName("LEADER 不能转换为 LEADER")
    void leaderCannotTransitionToLeader() {
        assertFalse(NodeState.LEADER.canTransitionTo(NodeState.LEADER));
    }

    @Test
    @DisplayName("FOLLOWER 可以转换为 CANDIDATE")
    void followerCanTransitionToCandidate() {
        assertTrue(NodeState.FOLLOWER.canTransitionTo(NodeState.CANDIDATE));
    }

    @Test
    @DisplayName("FOLLOWER 可以转换为 FOLLOWER")
    void followerCanTransitionToFollower() {
        assertTrue(NodeState.FOLLOWER.canTransitionTo(NodeState.FOLLOWER));
    }

    @Test
    @DisplayName("FOLLOWER 不能转换为 LEADER")
    void followerCannotTransitionToLeader() {
        assertFalse(NodeState.FOLLOWER.canTransitionTo(NodeState.LEADER));
    }

    @Test
    @DisplayName("CANDIDATE 可以转换为 LEADER")
    void candidateCanTransitionToLeader() {
        assertTrue(NodeState.CANDIDATE.canTransitionTo(NodeState.LEADER));
    }

    @Test
    @DisplayName("CANDIDATE 可以转换为 FOLLOWER")
    void candidateCanTransitionToFollower() {
        assertTrue(NodeState.CANDIDATE.canTransitionTo(NodeState.FOLLOWER));
    }

    @Test
    @DisplayName("CANDIDATE 可以转换为 CANDIDATE")
    void candidateCanTransitionToCandidate() {
        assertTrue(NodeState.CANDIDATE.canTransitionTo(NodeState.CANDIDATE));
    }

    @Test
    @DisplayName("getValidNextStates 返回正确数组")
    void getValidNextStatesReturnsCorrectArray() {
        NodeState[] leaderNext = NodeState.LEADER.getValidNextStates();
        assertEquals(1, leaderNext.length);
        assertEquals(NodeState.FOLLOWER, leaderNext[0]);

        NodeState[] followerNext = NodeState.FOLLOWER.getValidNextStates();
        assertEquals(2, followerNext.length);
    }

    @Test
    @DisplayName("getValidNextStates 返回数组副本，修改不影响原状态")
    void getValidNextStatesReturnsClone() {
        NodeState[] originalNext = NodeState.LEADER.getValidNextStates();
        originalNext[0] = NodeState.CANDIDATE;
        
        NodeState[] nextAfterModification = NodeState.LEADER.getValidNextStates();
        assertEquals(NodeState.FOLLOWER, nextAfterModification[0]);
    }
}
