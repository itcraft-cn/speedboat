package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.rpc.RequestVoteRequest;

import java.util.List;
import java.util.Map;

/**
 * 权重法定多数计算器（内部协作器，非公开 API）。
 *
 * <p>职责唯一：把"投票权重"问题收敛为纯函数——原 RaftNodeImpl 内 4 处同构循环
 * （calculateTotalVoteWeight / calculateReceivedVoteWeight / calculateRequiredWeight /
 * calculateVoteWeight）与 advanceCommitIndex 内联变体，在此归一为 3 个入口：</p>
 * <ul>
 *   <li>{@link #nodeWeight(String, long)}：单节点权重 = 1 + 策略附加权重；</li>
 *   <li>{@link #totalWeight()}：集群全体权重和（含自身）；</li>
 *   <li>{@link #requiredWeight()}：胜利多多数 = total/2 + 1；</li>
 *   <li>{@link #receivedWeight(Map)}：给定"已授予投票集合"的权重和。</li>
 * </ul>
 *
 * <p>策略为 null 的分支语义原样保留（null → 全员权重 1，等价默认多数派）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class QuorumCalculator {

    private final NodeContext ctx;

    QuorumCalculator(NodeContext ctx) {
        this.ctx = ctx;
    }

    /**
     * 单节点权重：基础 1 + 投票权策略附加权重。
     *
     * @param peerId 节点/候选者 ID
     * @param term   当前活动任期（策略上下文用）
     * @return 权重（≥1，保证所有法定判定单调）
     */
    int nodeWeight(String peerId, long term) {
        int weight = 1;
        if (ctx.voteWeightStrategy != null) {
            VoteContext context = VoteContext.forDatacenter(peerId, peerDatacenter(peerId), term);
            weight += ctx.voteWeightStrategy.calculateAdditionalWeight(context);
        }
        return weight;
    }

    /** 本节点作为候选者的自身权重（基础 1 + 附加权重） */
    int selfWeight(long term) {
        int weight = 1;
        if (ctx.voteWeightStrategy != null) {
            VoteContext context = VoteContext.forDatacenter(ctx.nodeId, ctx.datacenter, term);
            weight += ctx.voteWeightStrategy.calculateAdditionalWeight(context);
        }
        return weight;
    }

    /**
     * 集群全体权重和（含自身）。
     * 复制多数派判定的分母来源（Ratis 式"required 语义"）。
     *
     * <p>分母口径由 {@link cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy#quorumByDatacenter()}
     * 决定：缺省逐节点累加（行为不变）；父组按机房去重聚合，见
     * {@link #totalWeightByDatacenter()}。</p>
     */
    int totalWeight() {
        if (byDatacenter()) {
            return totalWeightByDatacenter();
        }
        int totalWeight = 1;
        for (String peerId : ctx.peerIds) {
            totalWeight += nodeWeight(peerId, ctx.term.getCurrent());
        }
        return totalWeight;
    }

    /**
     * 分母按机房聚合：自身权重 + 每个<b>不同对侧机房</b>各计一次权重。
     *
     * <p>自身先以 {@link #selfWeight(long)} 计入（基础 1 + 本机房策略附加），再对 peer 按
     * {@link #peerDatacenter(String)} 去重——同一机房的多个 peer 是"同一席位的多条投递路径"，
     * 只取首个出现者的权重。这样两机房各 3 节点的父组得到 {@code total = 2 + 1 = 3}、
     * {@code required = 2}，与设计文档的唯一性推导一致。</p>
     *
     * @return 按机房聚合后的全体权重和
     */
    private int totalWeightByDatacenter() {
        long term = ctx.term.getCurrent();
        java.util.Set<String> countedDatacenters = new java.util.HashSet<String>();
        countedDatacenters.add(ctx.datacenter);
        int total = selfWeight(term);
        for (String peerId : ctx.peerIds) {
            String peerDc = peerDatacenter(peerId);
            // AP 降级态：被剔除的机房不计入分母（CP 恒为空集，此分支永不触发）
            if (isExcludedDatacenter(peerDc)) {
                continue;
            }
            if (countedDatacenters.add(peerDc)) {
                total += nodeWeight(peerId, term);
            }
        }
        return total;
    }

    /**
     * 机房是否被一致性策略剔除出法定多数分母。
     *
     * <p>仅 {@link cn.itcraft.speedboat.strategy.consistency.ApConsistencyPolicy}
     * 在降级态返回 true（对侧失联机房被剔除，使本机房可单方成主）；CP 恒 false，
     * 故分母聚合行为与引入策略前逐字节等价。</p>
     *
     * @param datacenter 机房标识
     * @return true 表示该机房已从分母剔除
     */
    private boolean isExcludedDatacenter(String datacenter) {
        return ctx.consistencyPolicy != null
            && !ctx.consistencyPolicy.excludedDatacenters().isEmpty()
            && ctx.consistencyPolicy.excludedDatacenters().contains(datacenter);
    }

    /** 是否启用"分母按机房聚合"口径（仅跨机房父组为 true，见策略接口说明）。 */
    boolean byDatacenter() {
        return ctx.voteWeightStrategy != null && ctx.voteWeightStrategy.quorumByDatacenter();
    }

    /**
     * 法定多数派权重最低门槛（全集群 total/2 + 1）。
     * 多数派以全体权重计算，与"谁投了票"无关。
     */
    int requiredWeight() {
        return totalWeight() / 2 + 1;
    }

    /**
     * 已收到投票的权重和（含自我投票，由调用方保证 votesReceived 已含自己）。
     *
     * @param votesReceived 投票记录（key=peerId）
     * @return 已收到的权重和
     */
    int receivedWeight(Map<String, Boolean> votesReceived) {
        if (byDatacenter()) {
            return receivedWeightByDatacenter(votesReceived);
        }
        int receivedWeight = 1;
        for (String peerId : ctx.peerIds) {
            if (votesReceived.containsKey(peerId)) {
                receivedWeight += nodeWeight(peerId, ctx.term.getCurrent());
            }
        }
        return receivedWeight;
    }

    /**
     * 已收投票权重（机房聚合口径）：自身 + 每个<b>已投票</b>的不同机房各计一次。
     *
     * <p>这是父组选举能收敛的关键：主机房代表只需本机房权重 2 ≥ required 2 即可免票成主；
     * 备机房代表即使拿到对侧全部票，也只会因"同一机房只计一次"而止步于
     * {@code self(1) + host(2) = 3 ≥ 2}——但闸门
     * {@link cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy#shouldGrantVote}
     * 已使主机房根本不把票投给低优先级机房，故备机房实际只能停在 1 &lt; 2。</p>
     *
     * @param votesReceived 投票记录（key=peerId，值恒为 true）
     * @return 按机房聚合后的已收投票权重和
     */
    private int receivedWeightByDatacenter(Map<String, Boolean> votesReceived) {
        long term = ctx.term.getCurrent();
        java.util.Set<String> countedDatacenters = new java.util.HashSet<String>();
        countedDatacenters.add(ctx.datacenter);
        int weight = selfWeight(term);
        for (String peerId : ctx.peerIds) {
            if (votesReceived.containsKey(peerId) && countedDatacenters.add(peerDatacenter(peerId))) {
                weight += nodeWeight(peerId, term);
            }
        }
        return weight;
    }

    /**
     * 对外旧的 calculateVoteWeight 语义保留：请求方基础权重 + 本地校验附加权重。
     *
     * <p>候选者机房以<b>请求方自报</b>的 {@code datacenter} 字段为准；旧版本节点
     * 发出的请求不携带该字段（null）时，回退为本节点机房标识，保持既有行为不变。</p>
     */
    int calculateVoteWeight(RequestVoteRequest request) {
        int baseWeight = request.getVoteWeight();
        int additionalWeight = 0;
        if (ctx.voteWeightStrategy != null) {
            String candidateDatacenter = request.getDatacenter() != null
                ? request.getDatacenter()
                : ctx.datacenter;
            VoteContext context = VoteContext.forDatacenter(
                request.getCandidateId(), candidateDatacenter, request.getTerm());
            additionalWeight = ctx.voteWeightStrategy.calculateAdditionalWeight(context);
        }
        return baseWeight + additionalWeight;
    }

    /**
     * 给定"peer 集合中各单位"的权重求和（check-quorum 与 prevote 共用）。
     * selfIncluded 为调用方已经自计的权重（自身固定 1）。
     *
     * @param grantedPeers 已授出投票/响应的 peer 集合
     * @param termRef      权重策略上下文任期
     * @return 含自身的权重和
     */
    int grantedWeight(java.util.Set<String> grantedPeers, long termRef) {
        if (byDatacenter()) {
            // 机房聚合口径：自身按 selfWeight 计，对侧各机房只取首个已授票 peer 的权重
            java.util.Set<String> countedDatacenters = new java.util.HashSet<String>();
            countedDatacenters.add(ctx.datacenter);
            int aggregated = selfWeight(termRef);
            for (String grantedPeer : grantedPeers) {
                if (grantedPeer.equals(ctx.nodeId)) {
                    continue;
                }
                if (countedDatacenters.add(peerDatacenter(grantedPeer))) {
                    aggregated += weightOfPeer(grantedPeer, termRef);
                }
            }
            return aggregated;
        }
        int weight = 1;
        for (String grantedPeer : grantedPeers) {
            if (grantedPeer.equals(ctx.nodeId)) {
                continue;
            }
            if (ctx.voteWeightStrategy != null) {
                VoteContext context = VoteContext.forDatacenter(grantedPeer, peerDatacenter(grantedPeer), termRef);
                weight += 1 + ctx.voteWeightStrategy.calculateAdditionalWeight(context);
            } else {
                weight += 1;
            }
        }
        return weight;
    }

    /** 单个 peer 的权重（基础 1 + 策略附加），等价于 {@link #nodeWeight(String, long)}。 */
    private int weightOfPeer(String peerId, long termRef) {
        if (ctx.voteWeightStrategy == null) {
            return 1;
        }
        VoteContext context = VoteContext.forDatacenter(peerId, peerDatacenter(peerId), termRef);
        return 1 + ctx.voteWeightStrategy.calculateAdditionalWeight(context);
    }

    /**
     * 看门狗口径：给定"最近有响应的 peer 集合"求权重和（含自身）。
     *
     * <p>与选举侧必须严格同口径——自身按 {@link #selfWeight(long)} 计、分母按机房聚合，
     * 否则会出现"选举按权重判多数、check-quorum 按节点计数判多数"的分裂：
     * 杀光备机房后主机房 {@code self=2}，若此处仍按节点数算则 {@code freshWeight} 只有
     * 对侧残余的计数，一旦跌破 {@code required} 便误降级，场景④退化为无主。</p>
     *
     * @param freshPeers 最近一次心跳有响应的 peer 集合
     * @param termRef    权重策略上下文任期
     * @return 含自身的权重和
     */
    int freshWeight(java.util.Set<String> freshPeers, long termRef) {
        if (byDatacenter()) {
            java.util.Set<String> countedDatacenters = new java.util.HashSet<String>();
            countedDatacenters.add(ctx.datacenter);
            int aggregated = selfWeight(termRef);
            for (String freshPeer : freshPeers) {
                if (freshPeer.equals(ctx.nodeId)) {
                    continue;
                }
                String freshPeerDc = peerDatacenter(freshPeer);
                // AP 降级态：分子与分母同口径剔除，避免对侧"部分新鲜"时口径分裂
                if (isExcludedDatacenter(freshPeerDc)) {
                    continue;
                }
                if (countedDatacenters.add(freshPeerDc)) {
                    aggregated += weightOfPeer(freshPeer, termRef);
                }
            }
            return aggregated;
        }
        int weight = 1;
        for (String freshPeer : freshPeers) {
            if (freshPeer.equals(ctx.nodeId)) {
                continue;
            }
            weight += weightOfPeer(freshPeer, termRef);
        }
        return weight;
    }

    /**
     * 给定"matchIndex 覆盖索引"的复制权重求和（advanceCommitIndex 的内联第 4 变体归一）。
     *
     * @param matchIndex peer → 已匹配 index
     * @param threshold  判定阈值 index
     * @param termRef    权重策略上下文任期
     * @return 含自身的权重和
     */
    int matchedWeight(Map<String, Long> matchIndex, long threshold, long termRef) {
        if (byDatacenter()) {
            // 机房聚合口径：自身 + 每个"已达阈值"的不同机房各计一次
            java.util.Set<String> countedDatacenters = new java.util.HashSet<String>();
            countedDatacenters.add(ctx.datacenter);
            int aggregated = selfWeight(termRef);
            for (String peerId : ctx.peerIds) {
                Long m = matchIndex.get(peerId);
                if (m != null && m >= threshold && countedDatacenters.add(peerDatacenter(peerId))) {
                    aggregated += weightOfPeer(peerId, termRef);
                }
            }
            return aggregated;
        }
        int weight = 1;
        for (String peerId : ctx.peerIds) {
            Long m = matchIndex.get(peerId);
            if (m != null && m >= threshold) {
                if (ctx.voteWeightStrategy != null) {
                    VoteContext context = VoteContext.forDatacenter(peerId, peerDatacenter(peerId), termRef);
                    weight += 1 + ctx.voteWeightStrategy.calculateAdditionalWeight(context);
                } else {
                    weight += 1;
                }
            }
        }
        return weight;
    }

    /**
     * 获取指定 peer 的机房标识（未设置时返回空字符串）。
     */
    String peerDatacenter(String peerId) {
        return ctx.peerDatacenters.getOrDefault(peerId, "");
    }

    /**
     * 识别<b>对侧机房</b>标识（AP 降级判定用）：peer 中首个与本机房不同的机房。
     *
     * <p>仅跨机房父组存在对侧机房；单机房集群全部 peer 同机房，返回 null，
     * AP 策略据此永不降级（无对侧可剔除）。</p>
     *
     * @return 对侧机房标识；同机房集群返回 null
     */
    String oppositeDatacenter() {
        for (String peerId : ctx.peerIds) {
            String dc = peerDatacenter(peerId);
            if (!dc.equals(ctx.datacenter)) {
                return dc;
            }
        }
        return null;
    }

    /** 设置指定 peer 的机房标识（null 归一为空串）。 */
    void setPeerDatacenter(String peerId, String datacenter) {
        ctx.peerDatacenters.put(peerId, datacenter != null ? datacenter : "");
    }

    /** 全体 peer ID 快照 */
    List<String> peerIds() {
        return new java.util.ArrayList<>(ctx.peerIds);
    }
}
