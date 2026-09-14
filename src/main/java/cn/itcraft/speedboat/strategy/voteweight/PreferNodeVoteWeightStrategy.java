package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;

/**
 * 优先节点加权策略（Phase 权重选举验证用）。
 *
 * <p>对指定 nodeId 的投票权重 +N（默认 3），其余节点不加权。
 * 用于"4 节点加权选举"的可验证性设计：
 * 4 节点权重 (3,1,1,1)，总权重 6，required = (6/2)+1 = 4——
 * 3 个普通节点 = 3 &lt; required，单独构不成多数派；
 * 优先节点(权重3) + 任意 1 个普通 = 4 ✓ 必须包含优先节点。
 * 权重真正改变多数派判定路径，可从日志/报告端到端验证。</p>
 *
 * <p>注意（与真实业务语义对齐）：权重属于集群成员配置，
 * 所有节点的 {@code vote.weight.prefer}/{@code vote.weight.prefer.weight} 必须一致，
 * 保证各端 required/权重视图一致，否则同一轮选举会出现不同判定。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class PreferNodeVoteWeightStrategy implements VoteWeightStrategy {

    private final String preferredNodeId;
    /** 优先节点额外权重（默认 3；must be > 0） */
    private final int extraWeight;

    public PreferNodeVoteWeightStrategy(String preferredNodeId) {
        this(preferredNodeId, 3);
    }

    public PreferNodeVoteWeightStrategy(String preferredNodeId, int extraWeight) {
        this.preferredNodeId = preferredNodeId;
        this.extraWeight = extraWeight > 0 ? extraWeight : 1;
    }

    public int getExtraWeight() {
        return extraWeight;
    }

    public String getPreferredNodeId() {
        return preferredNodeId;
    }

    @Override
    public int calculateAdditionalWeight(cn.itcraft.speedboat.raft.VoteContext context) {
        if (preferredNodeId == null || context == null) {
            return 0;
        }
        return context.getCandidateId() != null && context.getCandidateId().equals(preferredNodeId)
            ? extraWeight : 0;
    }
}
