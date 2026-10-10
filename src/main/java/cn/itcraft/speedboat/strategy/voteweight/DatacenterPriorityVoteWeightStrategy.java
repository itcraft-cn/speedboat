package cn.itcraft.speedboat.strategy.voteweight;

import cn.itcraft.speedboat.raft.VoteContext;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 机房优先级权重策略（跨机房父组专用）。
 *
 * <p><b>与 {@link DatacenterVoteWeightStrategy} 的本质区别：</b>
 * 后者按"候选者是否与<b>本节点</b>同机房"提权/降权，权重取决于<b>观察者视角</b>；
 * 本策略的权重由候选者<b>机房自身的属性</b>决定，与谁在观察无关。</p>
 *
 * <p>这个区别在父组上是<b>生死攸关</b>的：父组的投票成员恰好是"各机房的代表"，
 * 每个代表看自己都是"同机房"，于是 {@code DatacenterVoteWeightStrategy} 会让
 * 两侧的自身权重都变成 3、法定门槛都变成 2 → <b>两房都能单方成主 → 双主</b>，
 * 直接违反"全局 Leader 只有一主或无主"的硬约束。</p>
 *
 * <h3>唯一性的数学基础</h3>
 * <p>设机房权重为 {@code W}，父组总权重 {@code total = ΣW}，法定门槛
 * {@code required = total/2 + 1}。双主要求两个机房代表<b>同时</b>满足
 * {@code W_i ≥ required}。但由于 {@code required = total/2 + 1} 严格大于
 * {@code total/2}，而两个权重之和恰为 {@code total}，两者不可能同时 ≥ total/2 + 1。
 * 因此只要权重<b>不对称</b>，双主在结构上即不可能发生——与网络状态、超时长短无关。</p>
 *
 * <p>默认按机房索引派生：{@code 权重 = 机房总数 - 索引}，使索引靠前的机房（主机房）
 * 权重最高。两机房拓扑下即 {@code W_main=2 / W_backup=1}，{@code required=2}：
 * 主机房自身 2 ≥ 2 可单方成主，备机房自身 1 &lt; 2 不可。</p>
 *
 * @author speedboat
 * @see VoteWeightStrategy
 * @since 1.1.0
 */
public class DatacenterPriorityVoteWeightStrategy implements VoteWeightStrategy {

    /** 机房权重下限：保证任何机房至少有 1 权重，避免 total 被压到 0 导致 required 失去意义 */
    private static final int MIN_WEIGHT = 1;

    private final Map<String, Integer> weightsByDatacenter;
    private final String localDatacenter;
    private final int defaultWeight;
    /**
     * 可运行时替换的优先级表（阶段五人工升级 API 用）；null 表示退化为构造期不可变
     * Map（单机房与既有部署路径，行为逐字节不变）。
     */
    private final DatacenterPriorityTable priorityTable;

    /**
     * @param weightsByDatacenter 机房标识 → 权重（null 视为空 Map）
     * @param localDatacenter     本机房标识（候选者未自报机房时的兜底）
     * @param defaultWeight       未在权重表中登记的机房所用权重（须 &gt;= {@value #MIN_WEIGHT}）
     */
    public DatacenterPriorityVoteWeightStrategy(Map<String, Integer> weightsByDatacenter,
                                                String localDatacenter,
                                                int defaultWeight) {
        this(weightsByDatacenter, localDatacenter, defaultWeight, null);
    }

    /**
     * @param weightsByDatacenter 机房标识 → 权重（null 视为空 Map；仅当 priorityTable 为 null 时生效）
     * @param localDatacenter     本机房标识（候选者未自报机房时的兜底）
     * @param defaultWeight       未在权重表中登记的机房所用权重（须 &gt;= {@value #MIN_WEIGHT}）
     * @param priorityTable       可运行时替换的优先级表；非 null 时每次权重计算读其最新快照，
     *                           构造期 Map 仅作初始值被忽略
     */
    public DatacenterPriorityVoteWeightStrategy(Map<String, Integer> weightsByDatacenter,
                                                String localDatacenter,
                                                int defaultWeight,
                                                DatacenterPriorityTable priorityTable) {
        Objects.requireNonNull(localDatacenter, "localDatacenter cannot be null");
        if (defaultWeight < MIN_WEIGHT) {
            throw new IllegalArgumentException("defaultWeight must be >= " + MIN_WEIGHT + ", got " + defaultWeight);
        }
        this.weightsByDatacenter = weightsByDatacenter == null
            ? Collections.<String, Integer>emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<String, Integer>(weightsByDatacenter));
        this.localDatacenter = localDatacenter;
        this.defaultWeight = defaultWeight;
        this.priorityTable = priorityTable;
    }

    /**
     * 按机房索引派生权重表（未显式配置时的缺省策略）。
     *
     * <p>公式：{@code 权重 = 机房总数 - 索引}，即索引越靠前权重越高。
     * 两机房 {@code [dc-0, dc-1]} 派生出 {@code {dc-0=2, dc-1=1}}，
     * 正好对应设计文档要求的 {@code W_main=2 / W_backup=1}。</p>
     *
     * @param datacenterIds 机房标识列表，顺序即 {@code nodes.<索引>} 的索引顺序
     * @return 机房标识 → 权重；入参为空时返回空 Map
     */
    public static Map<String, Integer> deriveWeightsByIndex(List<String> datacenterIds) {
        if (datacenterIds == null || datacenterIds.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Integer> derived = new LinkedHashMap<String, Integer>();
        int total = datacenterIds.size();
        for (int i = 0; i < total; i++) {
            derived.put(datacenterIds.get(i), total - i);
        }
        return derived;
    }

    @Override
    public int calculateAdditionalWeight(VoteContext context) {
        // 基础权重 1 + 附加权重 = 机房权重，故附加 = 权重 - 1
        return weightOf(candidateDatacenterOf(context)) - 1;
    }

    /**
     * 投票授予闸门：只把票投给<b>优先级不低于本机房</b>的候选者。
     *
     * <p><b>为什么这条闸门是必需的（缺了就会击穿主机房优先）：</b></p>
     *
     * <p>父组只有两个投票成员，法定门槛 {@code required = (W_host + W_backup)/2 + 1}。
     * 主机房死掉期间，备机房每次选举超时都会兜底进入正式选举、term 持续膨胀；
     * 待主机房以空 term 重启后，备机房以高 term 的 {@code RequestVote} 打过去，
     * 主机房会先对齐 term 再投票——于是备机房凑齐
     * {@code self(1) + host(2)} ≥ required 而成主。此后主机房想靠自身权重翻身，
     * 会被备机房"现任 leader 心跳健康即拒绝预票"的 sticky 规则永久挡住，
     * 既定机房优先级就此失效。</p>
     *
     * <p>把"高优先级席位持有者不把票让给低优先级方"显式化后：
     * 低优先级机房永远拿不到高优先级机房的票，因而永远凑不齐多数，
     * 无法反向收编；而高优先级机房自身权重已 ≥ required，不依赖他人的票即可成主。
     * 优先级顺序因而成为<b>确定性</b>结果，与 term 先后无关。</p>
     */
    @Override
    public boolean shouldGrantVote(VoteContext candidate) {
        String candidateDatacenter = candidateDatacenterOf(candidate);
        // 同机房（含未自报机房）必然放行：机房内子组选举不受本闸门影响
        if (candidateDatacenter.equals(localDatacenter)) {
            return true;
        }
        return weightOf(candidateDatacenter) >= weightOf(localDatacenter);
    }

    /**
     * 父组法定多数派必须按<b>机房聚合</b>，否则分母被虚增、基线就选不出主。
     *
     * <p>父组的 {@code peerIds} 铺满对侧机房<b>全部</b>节点（为把消息送达尚未确定的
     * 代表），但对侧真正持席的代表只有一个。逐节点累加会把 3 条投递路径当成 3 个投票
     * 席位，使 {@code required} 抬到 3，而主机房单方权重仅 2 —— 永远达不到。
     * 聚合后同一机房的多个 peer 共享一个席位，分母回到 {@code Σ机房权重}。</p>
     *
     * <p>缺省的 {@link VoteWeightStrategy#quorumByDatacenter()} 为 {@code false}，
     * 故单机房模式与既有策略的行为<b>逐字节不变</b>。</p>
     *
     * @return 恒为 true
     * @see VoteWeightStrategy#quorumByDatacenter()
     */
    @Override
    public boolean quorumByDatacenter() {
        return true;
    }

    /** 取候选者机房标识，未自报时回退为本机房 */
    private String candidateDatacenterOf(VoteContext context) {
        String candidateDatacenter = context.getCandidateDatacenter();
        return candidateDatacenter == null || candidateDatacenter.isEmpty()
            ? localDatacenter
            : candidateDatacenter;
    }

    /** 取某机房的权重，未登记时用兜底权重，并钳制到下限。注入了优先级表时读其最新快照。 */
    private int weightOf(String datacenter) {
        Integer configured = activeWeights().get(datacenter);
        int weight = configured == null ? defaultWeight : configured.intValue();
        return Math.max(weight, MIN_WEIGHT);
    }

    /**
     * 当前生效的权重表：注入了优先级表则读其最新快照（支持运行时热更新），
     * 否则返回构造期不可变 Map（既有路径，行为不变）。
     */
    private Map<String, Integer> activeWeights() {
        return priorityTable != null ? priorityTable.current().weights() : weightsByDatacenter;
    }

    /** 只读的权重表视图（日志与运维查询用）。优先反映优先级表最新快照。 */
    public Map<String, Integer> getWeightsByDatacenter() {
        return activeWeights();
    }

    /** 关联的优先级表（可为 null；人工升级 API 经此引用换表） */
    public DatacenterPriorityTable priorityTable() {
        return priorityTable;
    }

    /** 本机房标识 */
    public String getLocalDatacenter() {
        return localDatacenter;
    }
}
