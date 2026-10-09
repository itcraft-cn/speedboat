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
     */
    int totalWeight() {
        int totalWeight = 1;
        for (String peerId : ctx.peerIds) {
            totalWeight += nodeWeight(peerId, ctx.term.getCurrent());
        }
        return totalWeight;
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
        int receivedWeight = 1;
        for (String peerId : ctx.peerIds) {
            if (votesReceived.containsKey(peerId)) {
                receivedWeight += nodeWeight(peerId, ctx.term.getCurrent());
            }
        }
        return receivedWeight;
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

    /**
     * 给定"matchIndex 覆盖索引"的复制权重求和（advanceCommitIndex 的内联第 4 变体归一）。
     *
     * @param matchIndex peer → 已匹配 index
     * @param threshold  判定阈值 index
     * @param termRef    权重策略上下文任期
     * @return 含自身的权重和
     */
    int matchedWeight(Map<String, Long> matchIndex, long threshold, long termRef) {
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

    /** 设置指定 peer 的机房标识（null 归一为空串）。 */
    void setPeerDatacenter(String peerId, String datacenter) {
        ctx.peerDatacenters.put(peerId, datacenter != null ? datacenter : "");
    }

    /** 全体 peer ID 快照 */
    List<String> peerIds() {
        return new java.util.ArrayList<>(ctx.peerIds);
    }
}
