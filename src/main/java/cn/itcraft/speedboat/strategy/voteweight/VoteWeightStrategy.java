package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;

/**
 * 投票权重策略接口，用于在 Raft 选举中计算节点的额外投票权重。
 * 
 * <p>VoteWeightStrategy 允许根据节点属性（如数据中心、硬件配置、网络位置等）
 * 动态调整选举权重，实现智能化的 Leader 选举。</p>
 * 
 * <p>核心应用场景：</p>
 * <ul>
 *   <li><b>跨机房选举</b>：为同机房节点赋予更高权重，优先选举本地 Leader</li>
 *   <li><b>硬件差异</b>：根据 CPU/内存资源分配权重，优先选举高性能节点</li>
 *   <li><b>网络拓扑</b>：考虑网络延迟和带宽，优化选举结果</li>
 *   <li><b>偶数节点</b>：在偶数节点集群中解决平票问题</li>
 * </ul>
 * 
 * <p>策略实现：</p>
 * <ul>
 *   <li>{@link DefaultVoteWeightStrategy}：默认策略，返回固定权重</li>
 *   <li>{@link DatacenterVoteWeightStrategy}：基于数据中心的权重策略</li>
 *   <li>{@link EvenNodeVoteWeightStrategy}：偶数节点平票解决方案</li>
 *   <li>{@link CompositeVoteWeightStrategy}：组合多个策略的复合策略</li>
 * </ul>
 * 
 * <p>权重计算规则：</p>
 * <ol>
 *   <li>基础权重：所有节点默认权重为 0</li>
 *   <li>额外权重：根据策略计算，可为正数或负数</li>
 *   <li>最终权重：基础权重 + 额外权重，决定选举优先级</li>
 *   <li>平票解决：权重高的节点在得票数相同时优先成为 Leader</li>
 * </ul>
 * 
 * <p>使用示例：</p>
 * <pre>{@code
 * // 创建数据中心权重策略
 * VoteWeightStrategy strategy = new DatacenterVoteWeightStrategy("hangzhou001", 10);
 * 
 * // 在选举上下文中计算权重
 * VoteContext context = new VoteContext(candidateId, voterId, datacenterId);
 * int extraWeight = strategy.calculateAdditionalWeight(context);
 * 
 * // 如果候选人与投票者同数据中心，返回额外权重 10
 * // 否则返回 0
 * }</pre>
 * 
 * @author speedboat
 * @see VoteContext
 * @since 1.0.0
 */
public interface VoteWeightStrategy {
    
    int calculateAdditionalWeight(VoteContext context);
}