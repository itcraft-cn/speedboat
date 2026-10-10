package cn.itcraft.speedboat.strategy.leadership;

import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityVoteWeightStrategy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 领袖优先级策略（LeadershipPolicy）单测。
 *
 * <p><b>被守护的语义（三机房 3/2/2 实机验证踩坑）：</b>三机房下主机房权重 3 需要一张
 * 备房票成主，与两备房互投（2+2=4）竞速同速。若初始选举被低权重机房抢先，PreVote 的
 * "现任 healthy 即拒" sticky 会把高权重候选者的夺回通道<b>永久堵死</b>——机房优先级
 * 因此必须有策略化的"sticky 让位"通路，且必须<b>严格单向</b>（高夺低可、低夺高不可），
 * 否则两对等机会乒乓震荡、term 反复翻车。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
class LeadershipPolicyTest {

    /** 3/2/2 权重表（优先级表热更新载体；实机拓扑复刻） */
    private static DatacenterPriorityVoteWeightStrategy priorityStrategy() {
        Map<String, Integer> weights = new LinkedHashMap<>();
        weights.put("dc-0", 3);
        weights.put("dc-1", 2);
        weights.put("dc-2", 2);
        DatacenterPriorityTable table =
            new DatacenterPriorityTable(weights, DatacenterPriorityTable.Source.CONFIG);
        return new DatacenterPriorityVoteWeightStrategy(weights, "dc-0", 1, table);
    }

    @Test
    @DisplayName("缺省策略恒 peer：sticky 永不让位（单机房/双机房/未配置部署零行为变更）")
    void defaultPolicyNeverYields() {
        LeadershipPolicy p = LeadershipPolicy.defaultPolicy();
        assertEquals(LeadershipPolicy.MODE_PEER, p.mode());
        assertFalse(p.stickyMayYield("dc-0", "dc-1"), "peer 下权重再高也不让位");
        assertFalse(p.stickyMayYield(null, "dc-1"), "peer 下 null 候选也不让位");
    }

    @Test
    @DisplayName("dominant：高夺低让位、同权不放、低夺高不放（严格单向，无乒乓）")
    void dominantYieldsOnlyUpward() {
        LeadershipPolicy p = LeadershipPolicy.dominant(priorityStrategy());
        assertEquals(LeadershipPolicy.MODE_DOMINANT, p.mode());

        assertTrue(p.stickyMayYield("dc-0", "dc-1"),
            "主机房(3) 必须能 夺回 权重2 的在位机房——sticky 让位是机房优先级的兑现通道");
        assertTrue(p.stickyMayYield("dc-0", "dc-2"),
            "主机房(3) 对另一权重机房同样单向让位");
        assertFalse(p.stickyMayYield("dc-1", "dc-2"), "两对等备房互夺必须被拒（否则乒乓）");
        assertFalse(p.stickyMayYield("dc-2", "dc-1"), "两对等备房互夺必须被拒");
        assertFalse(p.stickyMayYield("dc-1", "dc-0"), "低权重机房不得夺回主机房（低夺高不可）");
        assertFalse(p.stickyMayYield("dc-1", "dc-1"), "同一机房无从让位");
        assertFalse(p.stickyMayYield(null, "dc-1"), "未自报机房的候选者不让位（保守）");
        assertFalse(p.stickyMayYield("dc-1", null), "无在位 Leader 无从让位");
    }

    @Test
    @DisplayName("dominant 冻结例外：本房权重严格高于现任 Leader 房时的选举定时器不冻结，反之一律冻结")
    void dominantFreezeExceptionFollowsWeight() {
        LeadershipPolicy p = LeadershipPolicy.dominant(priorityStrategy());

        // 主机房(3) 持有夺主探测权：在位备房(2) 心跳不得冻结其选举定时器
        assertFalse(p.freezeHeartbeatElectionTimer("dc-1", "dc-0"),
            "dc-0(3) 对 dc-1(2) 在位心跳不冻结选举定时器（夺主探测必须有发起时机）");
        assertFalse(p.freezeHeartbeatElectionTimer("dc-2", "dc-0"),
            "dc-0(3) 对 dc-2(2) 同样不冻结");
        // 反向（低权重对上高权重现任）必须冻结——否则两对等机房乒乓震荡
        assertTrue(p.freezeHeartbeatElectionTimer("dc-0", "dc-1"), "低夺高冻结");
        assertTrue(p.freezeHeartbeatElectionTimer("dc-1", "dc-2"), "两对等备房互不夺主（冻结）");
        assertTrue(p.freezeHeartbeatElectionTimer(null, "dc-0"), "未知现任机房保守冻结");
        assertTrue(p.freezeHeartbeatElectionTimer("dc-1", "dc-1"), "同机房冻结");
    }

    @Test
    @DisplayName("缺省 peer 策略：冻结口径恒 true（逐字节等价于引入前的全局行为）")
    void peerAlwaysFreezes() {
        LeadershipPolicy p = LeadershipPolicy.defaultPolicy();
        assertTrue(p.freezeHeartbeatElectionTimer("dc-2", "dc-0"), "peer 不做冻结例外");
        assertTrue(p.freezeHeartbeatElectionTimer(null, null));
    }
    @Test
    @DisplayName("dominant + 无权重策略兜底：权重全 1，任何候选者都不让位（failsafe）")
    void dominantWithoutWeightsNeverYields() {
        LeadershipPolicy p = LeadershipPolicy.dominant(null);
        assertFalse(p.stickyMayYield("dc-0", "dc-1"),
            "无权重口径时 1 vs 1，不得让位——错误配置也不应打开夺回通道");
    }

    @Test
    @DisplayName("dominant 追随优先级表热更新：人工提升后新机房权重立即参与让位判定")
    void dominantFollowsPriorityTableHotUpdate() {
        Map<String, Integer> weights = new LinkedHashMap<>();
        weights.put("dc-0", 3);
        weights.put("dc-1", 2);
        weights.put("dc-2", 2);
        DatacenterPriorityTable table =
            new DatacenterPriorityTable(weights, DatacenterPriorityTable.Source.CONFIG);
        DatacenterPriorityVoteWeightStrategy strategy =
            new DatacenterPriorityVoteWeightStrategy(weights, "dc-0", 1, table);
        LeadershipPolicy p = LeadershipPolicy.dominant(strategy);

        assertFalse(p.stickyMayYield("dc-1", "dc-2"), "提升前两备房同权互不让位");

        // 模拟人工提升 dc-1（PriorityPromoteTest 同款：本房 = max(对侧)+2）
        table.replace(1L, java.util.Collections.singletonMap("dc-1", 4),
            DatacenterPriorityTable.Source.PROMOTED);

        assertTrue(p.stickyMayYield("dc-1", "dc-2"),
            "人工提升后 dc-1 权重 4 > dc-2 权重 2，sticky 应立即让位（热更新紧跟表快照）");
        assertFalse(p.stickyMayYield("dc-2", "dc-1"), "被提升后的 dc-1 不再被 dc-2 夺回");
    }
}
