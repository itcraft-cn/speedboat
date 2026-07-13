package cn.itcraft.speedboat.strategy;

import cn.itcraft.speedboat.raft.VoteContext;
import cn.itcraft.speedboat.strategy.voteweight.DefaultVoteWeightStrategy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("VoteWeightStrategy 权重策略测试")
class VoteWeightStrategyTest {

    private VoteWeightStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new DefaultVoteWeightStrategy();
    }

    @Test
    @DisplayName("默认策略返回0权重")
    void defaultStrategyReturnsZero() {
        VoteContext context = VoteContext.forCandidate("node1", 1L);
        assertEquals(0, strategy.calculateAdditionalWeight(context));
    }

    @Test
    @DisplayName("forCandidate 创建简单候选者上下文")
    void forCandidateCreatesSimpleContext() {
        VoteContext context = VoteContext.forCandidate("node1", 5L);
        
        assertEquals("node1", context.getCandidateId());
        assertEquals("", context.getCandidateDatacenter());
        assertFalse(context.isStartupFirst());
        assertFalse(context.isElectionInitiator());
        assertEquals(5L, context.getTerm());
    }

    @Test
    @DisplayName("forDatacenter 创建带数据中心的上下文")
    void forDatacenterCreatesContextWithDatacenter() {
        VoteContext context = VoteContext.forDatacenter("node1", "dc1", 10L);
        
        assertEquals("node1", context.getCandidateId());
        assertEquals("dc1", context.getCandidateDatacenter());
        assertFalse(context.isStartupFirst());
        assertFalse(context.isElectionInitiator());
        assertEquals(10L, context.getTerm());
    }

    @Test
    @DisplayName("完整构造函数创建上下文")
    void fullConstructorCreatesContext() {
        VoteContext context = new VoteContext("node1", "dc1", true, true, 100L);
        
        assertEquals("node1", context.getCandidateId());
        assertEquals("dc1", context.getCandidateDatacenter());
        assertTrue(context.isStartupFirst());
        assertTrue(context.isElectionInitiator());
        assertEquals(100L, context.getTerm());
    }

    @Test
    @DisplayName("空数据中心字符串被接受")
    void emptyDatacenterIsAccepted() {
        VoteContext context = VoteContext.forCandidate("node1", 1L);
        assertEquals("", context.getCandidateDatacenter());
    }

    @Test
    @DisplayName("null 候选者ID被接受")
    void nullCandidateIdIsAccepted() {
        VoteContext context = VoteContext.forCandidate(null, 1L);
        assertNull(context.getCandidateId());
    }

    @Test
    @DisplayName("负数任期被接受")
    void negativeTermIsAccepted() {
        VoteContext context = VoteContext.forCandidate("node1", -1L);
        assertEquals(-1L, context.getTerm());
    }

    @Test
    @DisplayName("策略接口可被自定义实现")
    void strategyCanBeCustomized() {
        VoteWeightStrategy customStrategy = context -> {
            if (context.isStartupFirst()) {
                return 10;
            }
            return 5;
        };
        
        VoteContext startupContext = new VoteContext("node1", "", true, false, 1L);
        VoteContext normalContext = new VoteContext("node2", "", false, false, 1L);
        
        assertEquals(10, customStrategy.calculateAdditionalWeight(startupContext));
        assertEquals(5, customStrategy.calculateAdditionalWeight(normalContext));
    }

    @Test
    @DisplayName("多次调用默认策略始终返回0")
    void defaultStrategyAlwaysReturnsZero() {
        VoteContext context1 = VoteContext.forCandidate("node1", 1L);
        VoteContext context2 = new VoteContext("node2", "dc1", true, true, 100L);
        
        assertEquals(0, strategy.calculateAdditionalWeight(context1));
        assertEquals(0, strategy.calculateAdditionalWeight(context2));
    }
}