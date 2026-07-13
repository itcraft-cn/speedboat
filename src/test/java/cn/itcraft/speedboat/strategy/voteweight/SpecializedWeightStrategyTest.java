package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("专用权重策略测试")
class SpecializedWeightStrategyTest {

    @Test
    @DisplayName("EvenNodeVoteWeightStrategy - 启动优先节点获得+1权重")
    void evenNodeStrategy_startupFirst() {
        EvenNodeVoteWeightStrategy strategy = new EvenNodeVoteWeightStrategy();
        VoteContext context = new VoteContext("node1", "dc1", true, false, 1L);
        
        assertEquals(1, strategy.calculateAdditionalWeight(context));
    }

    @Test
    @DisplayName("EvenNodeVoteWeightStrategy - 选举发起者获得+1权重")
    void evenNodeStrategy_electionInitiator() {
        EvenNodeVoteWeightStrategy strategy = new EvenNodeVoteWeightStrategy();
        VoteContext context = new VoteContext("node1", "dc1", false, true, 1L);
        
        assertEquals(1, strategy.calculateAdditionalWeight(context));
    }

    @Test
    @DisplayName("EvenNodeVoteWeightStrategy - 同时满足两个条件获得+2权重")
    void evenNodeStrategy_bothConditions() {
        EvenNodeVoteWeightStrategy strategy = new EvenNodeVoteWeightStrategy();
        VoteContext context = new VoteContext("node1", "dc1", true, true, 1L);
        
        assertEquals(2, strategy.calculateAdditionalWeight(context));
    }

    @Test
    @DisplayName("EvenNodeVoteWeightStrategy - 无特殊条件获得0权重")
    void evenNodeStrategy_noConditions() {
        EvenNodeVoteWeightStrategy strategy = new EvenNodeVoteWeightStrategy();
        VoteContext context = new VoteContext("node1", "dc1", false, false, 1L);
        
        assertEquals(0, strategy.calculateAdditionalWeight(context));
    }

    @Test
    @DisplayName("DatacenterVoteWeightStrategy - 同机房获得+2权重")
    void datacenterStrategy_sameDatacenter() {
        DatacenterVoteWeightStrategy strategy = new DatacenterVoteWeightStrategy("dc1");
        VoteContext context = new VoteContext("node1", "dc1", false, false, 1L);
        
        assertEquals(2, strategy.calculateAdditionalWeight(context));
    }

    @Test
    @DisplayName("DatacenterVoteWeightStrategy - 异地获得-1权重")
    void datacenterStrategy_differentDatacenter() {
        DatacenterVoteWeightStrategy strategy = new DatacenterVoteWeightStrategy("dc1");
        VoteContext context = new VoteContext("node1", "dc2", false, false, 1L);
        
        assertEquals(-1, strategy.calculateAdditionalWeight(context));
    }

    @Test
    @DisplayName("DatacenterVoteWeightStrategy - 空机房获得0权重")
    void datacenterStrategy_emptyDatacenter() {
        DatacenterVoteWeightStrategy strategy = new DatacenterVoteWeightStrategy("dc1");
        VoteContext context = new VoteContext("node1", "", false, false, 1L);
        
        assertEquals(0, strategy.calculateAdditionalWeight(context));
    }

    @Test
    @DisplayName("DatacenterVoteWeightStrategy - null机房获得0权重")
    void datacenterStrategy_nullDatacenter() {
        DatacenterVoteWeightStrategy strategy = new DatacenterVoteWeightStrategy("dc1");
        VoteContext context = new VoteContext("node1", null, false, false, 1L);
        
        assertEquals(0, strategy.calculateAdditionalWeight(context));
    }

    @Test
    @DisplayName("CompositeVoteWeightStrategy - 组合多个策略累加权重")
    void compositeStrategy_multipleStrategies() {
        EvenNodeVoteWeightStrategy evenNodeStrategy = new EvenNodeVoteWeightStrategy();
        DatacenterVoteWeightStrategy dcStrategy = new DatacenterVoteWeightStrategy("dc1");
        CompositeVoteWeightStrategy composite = new CompositeVoteWeightStrategy(
            evenNodeStrategy, dcStrategy
        );
        
        VoteContext context = new VoteContext("node1", "dc1", true, true, 1L);
        
        assertEquals(4, composite.calculateAdditionalWeight(context));
    }

    @Test
    @DisplayName("CompositeVoteWeightStrategy - 单策略场景")
    void compositeStrategy_singleStrategy() {
        EvenNodeVoteWeightStrategy strategy = new EvenNodeVoteWeightStrategy();
        CompositeVoteWeightStrategy composite = new CompositeVoteWeightStrategy(strategy);
        
        VoteContext context = new VoteContext("node1", "dc1", true, false, 1L);
        
        assertEquals(1, composite.calculateAdditionalWeight(context));
    }

    @Test
    @DisplayName("CompositeVoteWeightStrategy - 无策略返回0")
    void compositeStrategy_noStrategies() {
        CompositeVoteWeightStrategy composite = new CompositeVoteWeightStrategy();
        
        VoteContext context = new VoteContext("node1", "dc1", true, true, 1L);
        
        assertEquals(0, composite.calculateAdditionalWeight(context));
    }

    @Test
    @DisplayName("CompositeVoteWeightStrategy - 异地且无特殊条件的综合场景")
    void compositeStrategy_mixedScenario() {
        EvenNodeVoteWeightStrategy evenNodeStrategy = new EvenNodeVoteWeightStrategy();
        DatacenterVoteWeightStrategy dcStrategy = new DatacenterVoteWeightStrategy("dc1");
        CompositeVoteWeightStrategy composite = new CompositeVoteWeightStrategy(
            evenNodeStrategy, dcStrategy
        );
        
        VoteContext context = new VoteContext("node1", "dc2", false, false, 1L);
        
        assertEquals(-1, composite.calculateAdditionalWeight(context));
    }
}
