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

    /**
     * 机房优先级比较口径（接受预投票的 sticky 是否应让位给高优先级候选者）。
     *
     * <p>语义：候选者所在机房权重与"现任 Leader 所在机房"权重比较——实现方给出
     * 候选者机房权重时不含任何观察者视角（与 {@link #calculateAdditionalWeight} 的
     * 机房属性语义一致）。缺省局 1（未注入策略时无优先级概念，sticky 照旧拦截）。</p>
     *
     * <p>为什么需要它：三机房 3/2/2 下，主机房权重 3 物理上需要一张备房票成主，
     * 与"备房两房互投 2+2=4"竞速同速——若初始选举被低权重机房抢先，则 PreVote
     * 的"现任 healthy 即拒"sticky 会永久堵死高权重候选者的夺回通道（term 膨胀
     * 防护以"不再选"为前提）。sticky 必须为"权重严格更高候选者"让位，且只有
     * 单向让位（高夺低可、低夺高不可）从而无乒乓、收敛到最高权重机房。</p>
     *
     * @param datacenter 机房标识
     * @return 机房优先级权重（越高越优先）
     */
    default int dcWeight(String datacenter) {
        return 1;
    }

    /**
     * 投票授予闸门：本节点<b>是否应当</b>把票投给该候选者。
     *
     * <p>缺省恒放行，既有策略（{@link DefaultVoteWeightStrategy}、
     * {@link DatacenterVoteWeightStrategy} 等）行为完全不变。</p>
     *
     * <p><b>为什么需要它：</b>{@link #calculateAdditionalWeight} 只回答"权重是多少"，
     * 回答不了"该不该投"。跨机房父组里这条区别是致命的——低优先级机房在对侧死亡期间
     * 会持续发起选举导致 term 膨胀，对侧以空 term 重启后会被反向"收编"成下属，
     * 使既定的机房优先级永久失效。参见
     * {@link DatacenterPriorityVoteWeightStrategy} 的实现与说明。</p>
     *
     * @param candidate 候选者上下文（含候选者 ID 与机房标识）
     * @return true 表示放行（不因本闸门拒绝）；false 表示本节点拒绝投票
     */
    default boolean shouldGrantVote(VoteContext candidate) {
        return true;
    }

    /**
     * 法定多数派的分母是否按<b>机房聚合</b>计算。
     *
     * <p>缺省 {@code false}，既有逐节点累加行为完全不变。仅
     * {@link DatacenterPriorityVoteWeightStrategy}（跨机房级联父组）置为 {@code true}。</p>
     *
     * <p><b>为什么父组必须按机房聚合：</b>{@code peerIds} 在 Raft 里同时承担两个职责——
     * 消息投递集合，与多数派分母的成员集合。父组里这两者<b>天然冲突</b>：为保证请求能
     * 送达对侧"尚未确定是谁"的代表，peer 列表必须铺满对侧机房的<b>全部</b>节点；但对侧
     * 真正持有席位的代表只有<b>一个</b>。若仍按节点累加，分母会被虚增——以两机房各 3 节点
     * 为例，主机房自身权重 2、对侧 3 个 peer 各 1，逐节点累加得 total=4、required=3，
     * 而主机房单方仅 2，<b>永远凑不齐多数</b>，整个父组连基线都选不出主。</p>
     *
     * <p>按机房聚合后，同一机房的多个 peer 视为<b>同一席位的多条投递路径</b>，权重只计一次：
     * total = 主机房 2 + 备机房 1 = 3，required = 3/2+1 = 2。此时主机房 self=2 ≥ 2 可单方成主，
     * 备机房 self=1 &lt; 2 不可单方成主——正是不对称权重排除双主的原始推导。</p>
     *
     * @return true 表示分母按机房去重聚合；false 表示按节点逐个累加（缺省）
     */
    default boolean quorumByDatacenter() {
        return false;
    }
}