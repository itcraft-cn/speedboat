package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityVoteWeightStrategy;
import cn.itcraft.speedboat.strategy.voteweight.DefaultVoteWeightStrategy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 跨机房父组「法定多数」口径测试。
 *
 * <p><b>被守护的缺陷：</b>{@code peerIds} 在 Raft 里同时是<b>消息投递集合</b>与
 * <b>多数派分母的成员集合</b>。父组为把请求送达对侧"尚未确定是谁"的代表，必须把 peer
 * 铺满对侧机房的<b>全部</b>节点；但对侧真正持有席位的代表<b>只有一个</b>。若仍按节点
 * 逐个累加权重，分母被虚增——两机房各 3 节点时 {@code total=4、required=3}，而主机房
 * 自身权重仅 2，<b>永远凑不齐多数</b>，父组连基线都选不出主，整套级联直接失效。</p>
 *
 * <p><b>修复口径：</b>{@link VoteWeightStrategy#quorumByDatacenter()} 为 true 时，
 * 同一机房的多个 peer 视为<b>同一席位的多条投递路径</b>，权重只计一次，
 * 分母回到 {@code Σ机房权重}（设计文档 §7 决策二的原始推导）。</p>
 *
 * <p>本测试全部走 {@link QuorumCalculator} 纯函数路径，不启动节点、不依赖时序，
 * 结果确定可复现。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
class ParentGroupQuorumTest {

    /** 主机房（高优先级，权重 2） */
    private static final String MAIN_DC = "dc-0";
    /** 备机房（低优先级，权重 1） */
    private static final String BACKUP_DC = "dc-1";

    /** 设计文档 §7 决策二给定的机房权重表：W_main=2 / W_backup=1 */
    private static Map<String, Integer> weightsByDatacenter() {
        Map<String, Integer> weights = new LinkedHashMap<String, Integer>();
        weights.put(MAIN_DC, 2);
        weights.put(BACKUP_DC, 1);
        return weights;
    }

    /**
     * 父组装配结果：上下文 + 法定多数计算器 + peer 清单（便于构造投票/响应集合）。
     */
    private static final class ParentGroup {

        final NodeContext ctx;
        final QuorumCalculator quorum;
        final List<String> peerIds;

        /**
         * @param localDatacenter 本机房标识
         * @param peerDatacenter  对侧机房标识（peer 铺满该机房的全部节点）
         * @param peerCount       对侧节点数量
         */
        ParentGroup(String localDatacenter, String peerDatacenter, int peerCount) {
            this.peerIds = new ArrayList<String>();
            for (int i = 0; i < peerCount; i++) {
                // 形如真实父组节点：按对侧父组端口(22001)推导的身份
                peerIds.add("node-192.168.193.5" + i + "-22001");
            }

            RaftNode.Builder builder = new RaftNode.Builder()
                .nodeId("node-192.168.193.174-22001")
                .peerIds(peerIds)
                .electionTimeout(new ElectionTimeout(3000, 5000))
                .datacenter(localDatacenter)
                .voteWeightStrategy(new DatacenterPriorityVoteWeightStrategy(
                    weightsByDatacenter(), localDatacenter, 1));

            this.ctx = new NodeContext(builder);
            this.quorum = new QuorumCalculator(ctx);
            for (String peerId : peerIds) {
                quorum.setPeerDatacenter(peerId, peerDatacenter);
            }
        }

        ParentGroup(NodeContext ctx, QuorumCalculator quorum, List<String> peerIds) {
            this.ctx = ctx;
            this.quorum = quorum;
            this.peerIds = peerIds;
        }

        /** 模拟"对侧全部 peer 都投了赞成票" */
        Map<String, Boolean> allGranted() {
            Map<String, Boolean> votes = new HashMap<String, Boolean>();
            for (String peerId : peerIds) {
                votes.put(peerId, Boolean.TRUE);
            }
            return votes;
        }
    }

    // ==================== 分母口径 ====================

    @Test
    @DisplayName("分母按机房聚合 - total=Σ机房权重，与对侧 peer 数量无关")
    void denominatorAggregatesByDatacenter() {
        // 对侧若"已知代表是谁"，只铺 1 个 peer
        ParentGroup onePeer = new ParentGroup(MAIN_DC, BACKUP_DC, 1);
        // 实际必须铺满对侧全部进程，才能把消息送达尚未确定的代表
        ParentGroup threePeers = new ParentGroup(MAIN_DC, BACKUP_DC, 3);
        ParentGroup ninePeers = new ParentGroup(MAIN_DC, BACKUP_DC, 9);

        assertEquals(3, onePeer.quorum.totalWeight(), "total 应等于机房权重之和 2+1");
        assertEquals(3, threePeers.quorum.totalWeight(), "分母不得随 peer 数量膨胀");
        assertEquals(3, ninePeers.quorum.totalWeight(), "分母不得随 peer 数量膨胀");

        assertEquals(2, threePeers.quorum.requiredWeight(),
            "required = 3/2+1 = 2，主机房单方权重 2 才够得着");
    }

    @Test
    @DisplayName("聚合前的旧口径确实会虚增 - 逐节点累加得 total=4 / required=3（缺陷本体）")
    void perNodeSumWouldInflateDenominator() {
        // 用不启用聚合的缺省策略复现"按节点逐个累加"的历史口径：
        // 1(自身硬编码) + 3 个 peer × 1 = 4 → required = 3
        RaftNode.Builder builder = new RaftNode.Builder()
            .nodeId("node-192.168.193.174-22001")
            .peerIds(Arrays.asList("node-51-22001", "node-53-22001", "node-55-22001"))
            .electionTimeout(new ElectionTimeout(3000, 5000))
            .datacenter(MAIN_DC)
            .voteWeightStrategy(new DefaultVoteWeightStrategy());
        QuorumCalculator legacy = new QuorumCalculator(new NodeContext(builder));

        assertFalse(legacy.byDatacenter(), "缺省策略必须保持旧口径");
        assertEquals(4, legacy.totalWeight(), "旧口径下 3 个对侧 peer 会把分母抬到 4");
        assertEquals(3, legacy.requiredWeight(), "旧口径下门槛抬到 3，而主机房仅 2 → 永远选不出主");
    }

    @Test
    @DisplayName("聚合开关归属 - 仅机房优先级策略启用聚合，缺省策略恒为 false")
    void aggregationFlagOnlyOnDatacenterPriorityStrategy() {
        assertTrue(new DatacenterPriorityVoteWeightStrategy(weightsByDatacenter(), MAIN_DC, 1)
                .quorumByDatacenter(),
            "父组策略必须启用机房聚合");
        assertFalse(new DefaultVoteWeightStrategy().quorumByDatacenter(),
            "缺省策略必须保持逐节点累加，否则会静默改变所有既有单机房集群的门槛");
        assertFalse(new VoteWeightStrategy() {
            @Override
            public int calculateAdditionalWeight(VoteContext context) {
                return 3;
            }
        }.quorumByDatacenter(), "自定义策略缺省不得启用聚合");
    }

    // ==================== 唯一性：一主或无主 ====================

    @Test
    @DisplayName("主机房可单方成主 - self(2) ≥ required(2)，无需对侧任何选票")
    void mainDatacenterWinsAlone() {
        ParentGroup main = new ParentGroup(MAIN_DC, BACKUP_DC, 3);

        assertEquals(2, main.quorum.selfWeight(1), "主机房权重 2");
        assertTrue(main.quorum.selfWeight(1) >= main.quorum.requiredWeight(),
            "主机房必须能凭自身权重成主，否则杀光备机房后会退化为无主（场景④失败）");

        // 对侧一张票都没有（备机房全灭/网络分区）依然达标
        int received = main.quorum.receivedWeight(new HashMap<String, Boolean>());
        assertTrue(received >= main.quorum.requiredWeight(),
            "零票状态 receivedWeight=" + received + " 仍应 ≥ required");
    }

    @Test
    @DisplayName("备机房不可单方成主 - self(1) < required(2)，退化为无主而非接管")
    void backupDatacenterCannotWinAlone() {
        ParentGroup backup = new ParentGroup(BACKUP_DC, MAIN_DC, 3);

        assertEquals(1, backup.quorum.selfWeight(1), "备机房权重 1");
        assertTrue(backup.quorum.selfWeight(1) < backup.quorum.requiredWeight(),
            "备机房必须凑不齐多数，否则主机房死亡期会降级接管产生第二个主");

        int received = backup.quorum.receivedWeight(new HashMap<String, Boolean>());
        assertTrue(received < backup.quorum.requiredWeight(),
            "主机房全灭时备机房 receivedWeight=" + received + " 必须 < required → 无主");
    }

    @Test
    @DisplayName("唯一性的数学根 - 两个机房不可能同时满足多数派门槛")
    void bothDatacentersCannotReachQuorumSimultaneously() {
        // 权重之和恰为 total，而 required 严格大于 total/2，
        // 故不存在两个机房权重同时 ≥ required 的可能——与网络状态无关。
        ParentGroup main = new ParentGroup(MAIN_DC, BACKUP_DC, 3);
        ParentGroup backup = new ParentGroup(BACKUP_DC, MAIN_DC, 3);

        int total = main.quorum.totalWeight();
        int required = main.quorum.requiredWeight();
        assertEquals(total, backup.quorum.totalWeight(), "两侧看到的分母必须一致");
        assertEquals(required, backup.quorum.requiredWeight(), "两侧看到的门槛必须一致");

        int mainWeight = main.quorum.selfWeight(1);
        int backupWeight = backup.quorum.selfWeight(1);
        assertEquals(total, mainWeight + backupWeight, "两机房权重之和恰为 total");
        assertTrue(mainWeight < required || backupWeight < required,
            "两端权重之和 = total < 2×required，必然至少有一端达不到门槛");
    }

    // ==================== 票数聚合 ====================

    @Test
    @DisplayName("同机房多票只计一次 - 对侧 3 票等于 1 个席位而非 3 个")
    void sameDatacenterVotesCountOnce() {
        ParentGroup main = new ParentGroup(MAIN_DC, BACKUP_DC, 3);

        // 对侧 3 个 peer 全部赞成：只应加机房权重 1，而非 1×3
        assertEquals(2 + 1, main.quorum.receivedWeight(main.allGranted()),
            "同一机房的多票应去重为一个席位");
    }

    @Test
    @DisplayName("看门狗同口径 - 备机房全灭后主机房仍达门槛，不被 check-quorum 误降级")
    void checkQuorumKeepsMainLeaderWhenBackupAnnihilated() {
        ParentGroup main = new ParentGroup(MAIN_DC, BACKUP_DC, 3);

        // 对侧全部失联：没有任何 peer 有新鲜心跳响应
        int fresh = main.quorum.freshWeight(new HashSet<String>(), 1);

        assertTrue(fresh >= main.quorum.requiredWeight(),
            "freshWeight=" + fresh + " 必须 ≥ required=" + main.quorum.requiredWeight()
                + "，否则主机房 Leader 会被误降级，杀光备机房后全局退化为无主");
    }

    @Test
    @DisplayName("看门狗同口径 - 对侧仍新鲜时按机房计一次而非逐节点累加")
    void checkQuorumCountsFreshPeersByDatacenter() {
        ParentGroup main = new ParentGroup(MAIN_DC, BACKUP_DC, 3);

        int fresh = main.quorum.freshWeight(new HashSet<String>(main.peerIds), 1);

        // self(2) + 对侧机房一次(1) = 3，而非 2 + 3×1 = 5
        assertEquals(2 + 1, fresh, "对侧 3 个新鲜 peer 应聚合为一个机房席位");
    }

    // ==================== 全网互联拓扑（本机房非代表进程作为 peer） ====================

    /**
     * 构造全网互联拓扑下的父组：peer 覆盖**全部 5 个**其他进程，
     * 其中 2 个与本进程同机房（子组的非代表进程，事实上的 Raft learner）。
     *
     * @param localDatacenter 本机房标识
     * @return 装配好的父组
     */
    private static ParentGroup fullMesh(String localDatacenter) {
        String peerDatacenter = MAIN_DC.equals(localDatacenter) ? BACKUP_DC : MAIN_DC;

        // 前 2 个：与本进程**同机房**的非代表进程（事实上的 Raft learner）
        // 后 3 个：对侧机房的全部进程
        List<String> peerIds = new ArrayList<String>();
        if (MAIN_DC.equals(localDatacenter)) {
            peerIds.addAll(Arrays.asList(
                "node-192.168.193.175-22001", "node-192.168.193.176-22001",
                "node-192.168.193.51-22001", "node-192.168.193.53-22001",
                "node-192.168.193.55-22001"));
        } else {
            peerIds.addAll(Arrays.asList(
                "node-192.168.193.53-22001", "node-192.168.193.55-22001",
                "node-192.168.193.174-22001", "node-192.168.193.175-22001",
                "node-192.168.193.176-22001"));
        }

        RaftNode.Builder builder = new RaftNode.Builder()
            .nodeId("node-192.168.193.174-22001")
            .peerIds(peerIds)
            .electionTimeout(new ElectionTimeout(3000, 5000))
            .datacenter(localDatacenter)
            .voteWeightStrategy(new DatacenterPriorityVoteWeightStrategy(
                weightsByDatacenter(), localDatacenter, 1));

        NodeContext ctx = new NodeContext(builder);
        QuorumCalculator quorum = new QuorumCalculator(ctx);
        for (int i = 0; i < peerIds.size(); i++) {
            // 前 2 个与本进程同机房，后 3 个为对侧
            quorum.setPeerDatacenter(peerIds.get(i), i < 2 ? localDatacenter : peerDatacenter);
        }
        return new ParentGroup(ctx, quorum, peerIds);
    }

    @Test
    @DisplayName("全网互联不改变分母 - 本机房非代表进程的 peer 不计入 total")
    void fullMeshKeepsDenominatorUnchanged() {
        ParentGroup main = fullMesh(MAIN_DC);

        assertEquals(5, main.peerIds.size(), "全网互联应有 5 个 peer（自身除外）");
        assertEquals(3, main.quorum.totalWeight(),
            "本机房 2 个 peer 与对侧 3 个 peer 都必须按机房去重，分母仍是 Σ机房权重");
        assertEquals(2, main.quorum.requiredWeight());
        assertEquals(2, main.quorum.selfWeight(1), "主机房自身权重仍为 2");
        assertTrue(main.quorum.selfWeight(1) >= main.quorum.requiredWeight(),
            "主机房单方成主的能力不得因全网互联而丧失");
    }

    @Test
    @DisplayName("全网互联下唯一性不破 - 备机房仍凑不齐多数，两房不能同时达标")
    void fullMeshPreservesUniqueness() {
        ParentGroup main = fullMesh(MAIN_DC);
        ParentGroup backup = fullMesh(BACKUP_DC);

        assertEquals(main.quorum.totalWeight(), backup.quorum.totalWeight(), "两侧分母一致");
        assertEquals(main.quorum.requiredWeight(), backup.quorum.requiredWeight(), "两侧门槛一致");

        assertTrue(main.quorum.selfWeight(1) >= main.quorum.requiredWeight(),
            "主机房可单方成主（场景④）");
        assertTrue(backup.quorum.selfWeight(1) < backup.quorum.requiredWeight(),
            "备机房不可单方成主（场景②必须退化为无主）");
    }

    @Test
    @DisplayName("全网互联下看门狗 - 杀光对侧后同机房 peer 不把 freshWeight 抬离自身权重")
    void fullMeshWatchdogCountsOnlyOtherDatacenter() {
        ParentGroup main = fullMesh(MAIN_DC);

        // 对侧全灭：仅本机房 2 个 peer 有新鲜响应
        Set<String> onlyLocal = new HashSet<String>(main.peerIds.subList(0, 2));
        assertEquals(2, main.quorum.freshWeight(onlyLocal, 1),
            "同机房 peer 计入机房去重，不额外加分——freshWeight 恰等于 selfWeight");

        // 对侧仍可达：self(2) + 对侧机房一次(1)
        assertEquals(2 + 1, main.quorum.freshWeight(new HashSet<String>(main.peerIds), 1));
    }
}
