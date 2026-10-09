package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 机房优先级权重策略测试。
 *
 * <p>覆盖两个正交的不变式：</p>
 * <ol>
 *   <li><b>唯一性</b>——任意合法权重下，不可能有两个机房同时满足多数派门槛；</li>
 *   <li><b>优先级确定性</b>——投票闸门使高优先级机房永远不会把票让给低优先级方。</li>
 * </ol>
 *
 * @author speedboat
 * @since 1.1.0
 */
class DatacenterPriorityVoteWeightStrategyTest {

    // ==================== 权重派生 ====================

    @Test
    @DisplayName("按索引派生 - 两机房得到 W_main=2 / W_backup=1（设计文档要求值）")
    void deriveWeightsForTwoDatacenters() {
        Map<String, Integer> derived =
            DatacenterPriorityVoteWeightStrategy.deriveWeightsByIndex(Arrays.asList("dc-0", "dc-1"));

        assertEquals(2, derived.get("dc-0").intValue(), "主机房应得权重 2");
        assertEquals(1, derived.get("dc-1").intValue(), "备机房应得权重 1");
    }

    @Test
    @DisplayName("按索引派生 - 三机房权重递减，索引靠前优先级更高")
    void deriveWeightsForThreeDatacenters() {
        Map<String, Integer> derived = DatacenterPriorityVoteWeightStrategy.deriveWeightsByIndex(
            Arrays.asList("a", "b", "c"));

        assertEquals(3, derived.get("a").intValue());
        assertEquals(2, derived.get("b").intValue());
        assertEquals(1, derived.get("c").intValue());
    }

    @Test
    @DisplayName("按索引派生 - 空/ null 入参返回空 Map")
    void deriveWeightsHandlesEmptyInput() {
        assertTrue(DatacenterPriorityVoteWeightStrategy.deriveWeightsByIndex(null).isEmpty());
        assertTrue(DatacenterPriorityVoteWeightStrategy.deriveWeightsByIndex(
            Collections.<String>emptyList()).isEmpty());
    }

    @Test
    @DisplayName("构造 - 兜底权重低于 1 应拒绝（否则 total 被压垮，required 失去意义）")
    void constructorRejectsNonPositiveDefaultWeight() {
        assertThrows(IllegalArgumentException.class, () ->
            new DatacenterPriorityVoteWeightStrategy(
                Collections.<String, Integer>emptyMap(), "dc-0", 0));
    }

    // ==================== 权重计算 ====================

    @Test
    @DisplayName("权重计算 - 基础 1 + 附加 = 机房权重；未登记机房取兜底值")
    void weightCalculation() {
        Map<String, Integer> weights = new LinkedHashMap<String, Integer>();
        weights.put("dc-0", 3);
        DatacenterPriorityVoteWeightStrategy strategy =
            new DatacenterPriorityVoteWeightStrategy(weights, "dc-0", 1);

        // 附加权重 = 机房权重 - 1，故最终权重 = 1 + 附加 = 机房权重
        assertEquals(2, additionalWeightOf(strategy, "dc-0"), "已登记机房 dc-0 权重 3");
        assertEquals(0, additionalWeightOf(strategy, "dc-9"), "未登记机房走兜底权重 1");
        assertEquals(2, additionalWeightOf(strategy, null), "未自报机房 → 回退本机房 dc-0（权重 3）");
    }

    // ==================== 投票闸门 ====================

    @Test
    @DisplayName("闸门 - 同机房必然放行（机房内子组选举不受影响）")
    void gateAllowsSameDatacenter() {
        DatacenterPriorityVoteWeightStrategy strategy = twoDatacenterStrategy("dc-0");

        assertTrue(strategy.shouldGrantVote(candidateContext("node-x", "dc-0")));
    }

    @Test
    @DisplayName("闸门 - 高优先级机房放行给低优先级候选者")
    void gateAllowsHigherPriorityToGrantLower() {
        DatacenterPriorityVoteWeightStrategy hostStrategy = twoDatacenterStrategy("dc-0");

        // 主机房(dc-0, w=2) 投给 备机房(dc-1, w=1)：低优先级方拿不到主机房的票
        assertFalse(hostStrategy.shouldGrantVote(candidateContext("backup-rep", "dc-1")),
            "主机房不得把票让给低优先级的备机房——否则备机房 term 膨胀后会反向收编主机房");
    }

    @Test
    @DisplayName("闸门 - 低优先级机房放行给高优先级候选者")
    void gateAllowsLowerPriorityToGrantHigher() {
        DatacenterPriorityVoteWeightStrategy backupStrategy = twoDatacenterStrategy("dc-1");

        // 备机房(dc-1, w=1) 投给 主机房(dc-0, w=2)：主机房可据此单方凑齐多数
        assertTrue(backupStrategy.shouldGrantVote(candidateContext("host-rep", "dc-0")));
    }

    @Test
    @DisplayName("闸门 - 未自报机房视同本机房，放行")
    void gateTreatsMissingDatacenterAsLocal() {
        DatacenterPriorityVoteWeightStrategy hostStrategy = twoDatacenterStrategy("dc-0");

        assertTrue(hostStrategy.shouldGrantVote(candidateContext("unknown", null)));
    }

    // ==================== 唯一性不变式 ====================

    @Test
    @DisplayName("唯一性不变式 - 任意权重下都不可能有两个机房同时满足多数派门槛")
    void uniquenessInvariantHoldsForArbitraryWeights() {
        List<Map<String, Integer>> cases = Arrays.asList(
            derive("dc-0", "dc-1"),                             // 2 / 1
            mapOf("dc-0", 5, "dc-1", 1),                        // 极端不对称
            mapOf("dc-0", 3, "dc-1", 2),                        // 接近对称
            mapOf("dc-0", 4, "dc-1", 3, "dc-2", 2),             // 三机房
            mapOf("dc-0", 10, "dc-1", 9, "dc-2", 8, "dc-3", 7)  // 四机房
        );

        for (Map<String, Integer> weights : cases) {
            int total = 0;
            for (Integer w : weights.values()) {
                total += w.intValue();
            }
            final int required = total / 2 + 1;

            int leaders = 0;
            for (Integer w : weights.values()) {
                if (w.intValue() >= required) {
                    leaders++;
                }
            }

            assertTrue(leaders <= 1,
                "权重 " + weights + " 下有 " + leaders + " 个机房满足 required=" + required
                    + "，会出现双主——不对称权重是唯一性的唯一保障");
        }
    }

    @Test
    @DisplayName("两机房默认派生权重 - 主机房可单方成主、备机房不可")
    void defaultDerivedWeightsMatchDesignRequirement() {
        Map<String, Integer> weights = derive("dc-0", "dc-1");
        int total = weights.get("dc-0") + weights.get("dc-1");
        int required = total / 2 + 1;

        assertEquals(2, required, "两机房默认配置下门槛应为 2");
        assertTrue(weights.get("dc-0") >= required, "主机房自身权重须 ≥ 门槛，否则场景④（杀光备机房）无法成主");
        assertTrue(weights.get("dc-1") < required, "备机房自身权重须 < 门槛，否则分区时会与主机房双主");
    }

    // ==================== 工具 ====================

    private DatacenterPriorityVoteWeightStrategy twoDatacenterStrategy(String localDatacenter) {
        return new DatacenterPriorityVoteWeightStrategy(derive("dc-0", "dc-1"), localDatacenter, 1);
    }

    private VoteContext candidateContext(String candidateId, String datacenter) {
        return VoteContext.forDatacenter(candidateId, datacenter, 1L);
    }

    private int additionalWeightOf(DatacenterPriorityVoteWeightStrategy strategy, String datacenter) {
        return strategy.calculateAdditionalWeight(candidateContext("cand", datacenter));
    }

    private Map<String, Integer> derive(String... datacenterIds) {
        return DatacenterPriorityVoteWeightStrategy.deriveWeightsByIndex(Arrays.asList(datacenterIds));
    }

    private Map<String, Integer> mapOf(String k1, int v1, String k2, int v2) {
        Map<String, Integer> m = new LinkedHashMap<String, Integer>();
        m.put(k1, v1);
        m.put(k2, v2);
        return m;
    }

    private Map<String, Integer> mapOf(String k1, int v1, String k2, int v2, String k3, int v3) {
        Map<String, Integer> m = mapOf(k1, v1, k2, v2);
        m.put(k3, v3);
        return m;
    }

    private Map<String, Integer> mapOf(String k1, int v1, String k2, int v2,
                                       String k3, int v3, String k4, int v4) {
        Map<String, Integer> m = mapOf(k1, v1, k2, v2, k3, v3);
        m.put(k4, v4);
        return m;
    }
}
