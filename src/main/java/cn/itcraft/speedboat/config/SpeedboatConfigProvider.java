package cn.itcraft.speedboat.config;
import cn.itcraft.speedboat.strategy.consistency.ConsistencyPolicy;
import cn.itcraft.speedboat.strategy.consistency.CpConsistencyPolicy;
import cn.itcraft.speedboat.strategy.leadership.LeadershipPolicy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import java.util.List;
/**
 * Speedboat 配置提供者接口
 * 
 * <p>支持用户自定义配置来源（Properties/JSON/YAML/数据库等）</p>
 * <p>默认提供 PropertiesConfigProvider 实现（零依赖）</p>
 *
 * <p>Properties 文件格式：</p>
 * <pre>
 * # 机房ID（可选）
 * datacenter=hangzhou001
 * 
 * # 集群节点列表（必需）
 * # 格式：nodes.{机房索引}.{节点索引}=ip:port
 * nodes.0.0=192.168.10.1:3000
 * nodes.0.1=192.168.10.2:3000
 * nodes.0.2=192.168.10.3:3000
 * nodes.1.0=10.3.1.1:3000
 * nodes.1.1=10.3.1.2:3000
 * nodes.1.2=10.3.1.3:3000
 * 
 * # 可选：机房内选举超时（默认 1000-2000ms）
 * election.intra.timeout.min=1000
 * election.intra.timeout.max=2000
 * 
 * # 可选：机房间选举超时（默认 3000-5000ms）
 * election.cross.timeout.min=3000
 * election.cross.timeout.max=5000
 * </pre>
 *
 * @author speedboat
 * @since 1.0.0
 */
public interface SpeedboatConfigProvider {
    
    /**
     * 获取机房ID
     * 
     * <p>单机房场景可返回 null</p>
     * <p>跨机房场景用于标识本机房，系统会自动匹配节点</p>
     *
     * @return 机房ID，单机房场景返回 null
     */
    String getDatacenter();
    
    /**
     * 获取集群节点列表
     * 
     * <p>格式：List<List<String>></p>
     * <ul>
     *   <li>nodes[0] = 机房0的节点列表</li>
     *   <li>nodes[1] = 机房1的节点列表</li>
     *   <li>每个节点格式："ip:port"</li>
     * </ul>
     *
     * <p>示例：</p>
     * <pre>{@code
     * // 单机房
     * [["192.168.10.1:3000", "192.168.10.2:3000", "192.168.10.3:3000"]]
     * 
     * // 跨机房
     * [
     *   ["192.168.10.1:3000", "192.168.10.2:3000", "192.168.10.3:3000"],  // 机房0
     *   ["10.3.1.1:3000", "10.3.1.2:3000", "10.3.1.3:3000"]              // 机房1
     * ]
     * }</pre>
     *
     * @return 集群节点列表，不能为 null 或空
     */
    List<List<String>> getNodes();

    /**
     * 获取成员管理配置（Phase B2+ 成员变更策略）。
     *
     * <p>默认实现返回静态成员（自动剔除关闭）；配置文件可通过
     * {@code membership.auto.removal=true|false} 显式开启自动剔除。</p>
     */
    /**
     * 获取投票权重策略（Phase 权重选举）。
     *
     * <p>返回 null 时所有节点权重为 1（标准机制）。
     * 配置文件支持 {@code vote.weight.strategy=prefer|even|none} +
     * {@code vote.weight.prefer=<nodeId>}。</p>
     */
    default VoteWeightStrategy getVoteWeightStrategy() {
        return null;
    }

    /**
     * 一致性策略（CP/AP）。缺省 CP——强一致唯一性，父组分母恒定、绝不剔除机房、永不降级接管。
     *
     * <p>配置文件支持 {@code consistency.policy=cp|ap}（默认 cp）与
     * {@code consistency.degraded.timeout.ms=10000}（仅 ap 生效：整机房失联多久后允许降级接管）。
     * AP 允许分区期间短暂双主，监控告警阈值需按模式区分。</p>
     *
     * @return 一致性策略；null 视为 CP
     */
    default ConsistencyPolicy getConsistencyPolicy() {
        return CpConsistencyPolicy.getInstance();
    }

    /**
     * 领袖优先级模式（三机房拓扑语义）。缺省 peer（对等先到先得）——
     * 单机房、双机房与一切未显式配置的部署沿历史行为不变。
     *
     * <p>配置文件支持 {@code crossdc.leadership=peer|dominant}：三机房"同城双备 +
     * 异地灾备"语义（必须指定主机房）配 {@code dominant}——父组 PreVote 的 sticky
     * 为权重严格更高的候选者让位、且选举定时器不被在位心跳冻结，保证主机房优先级
     * 真实兑现（含初始竞速被抢先后的单向夺回）；三机房对等（无主机房指定）保持
     * {@code peer}，任一机房合法凑满 required 即可当主、现任成主后不再折腾。
     * 空/非法/未知 视为 peer。</p>
     *
     * <p><b>注意：本方法只解析模式字符串；dominant 的权重绑定在父组装时完成
     * （ParentGroupBuilder）——彼时父组权重表/投票策略才真正存在，过早绑定会
     * 拿不到 3/2/2 的机房权重。</b></p>
     */
    default String getLeadershipMode() {
        return LeadershipPolicy.MODE_PEER;
    }


    default MembershipConfig getMembershipConfig() {
        return new MembershipConfig();
    }

    /**
     * 内存日志保留上限（defect-20260918-01：重启副本重建判定状态的唯一依据，
     * 截断过深会引发 epoch 跨副本分叉；缺省 {@link SpeedboatConsts#DEFAULT_MAX_LOG_SIZE}）。
     */
    default int getMaxLogSize() {
        return SpeedboatConsts.DEFAULT_MAX_LOG_SIZE;
    }
    
    /**
     * 获取机房内选举超时下限（毫秒）
     * 
     * <p>用于同一机房内节点的选举</p>
     * <p>默认值：1000ms</p>
     * <p>建议值：1000-2000ms</p>
     *
     * @return 机房内选举超时下限（毫秒）
     */
    default int getIntraDatacenterElectionTimeoutMin() {
        return 1000;
    }
    
    /**
     * 获取机房内选举超时上限（毫秒）
     * 
     * <p>用于同一机房内节点的选举</p>
     * <p>默认值：2000ms</p>
     * <p>建议值：1000-2000ms</p>
     *
     * @return 机房内选举超时上限（毫秒）
     */
    default int getIntraDatacenterElectionTimeoutMax() {
        return 2000;
    }
    
    /**
     * 获取机房间选举超时下限（毫秒）
     * 
     * <p>用于跨机房级联选举</p>
     * <p>默认值：3000ms</p>
     * <p>建议值：3000-5000ms（考虑跨机房网络延迟）</p>
     *
     * @return 机房间选举超时下限（毫秒）
     */
    default int getCrossDatacenterElectionTimeoutMin() {
        return 3000;
    }
    
    /**
     * 获取机房间选举超时上限（毫秒）
     * 
     * <p>用于跨机房级联选举</p>
     * <p>默认值：5000ms</p>
     * <p>建议值：3000-5000ms（考虑跨机房网络延迟）</p>
     *
     * @return 机房间选举超时上限（毫秒）
     */
    default int getCrossDatacenterElectionTimeoutMax() {
        return 5000;
    }


    /**
     * 持久化三档选型（2026-09-18 P4-M5 收敛：**缺省 mmap**）：
     * <ul>
     *   <li>{@code none}：NopRaftStore（测试基线/显式关闭）</li>
     *   <li>{@code mem}：InMemoryRaftStore——<b>显式档</b>（无盘沙箱/容器 ephemeral；
     *       跨进程重启丢 term/votedFor/锁 epoch，不再是缺省）</li>
     *   <li>{@code mmap}（缺省）：MmapRaftStore——跨进程重启挂回，
     *       选主安全性与锁 epoch 保真度最高</li>
     * </ul>
     */
    default String getRaftPersistenceType() {
        return "mmap";
    }

    /** mmap 档持久化目录（缺省工作目录下 speedboat-data） */
    default String getRaftPersistenceDir() {
        return "speedboat-data";
    }

    /** mmap 档文件名（缺省 raft.mmap，可自定义；{nodeId} 占位符可用） */
    default String getRaftMmapFileName() {
        return "raft.mmap";
    }

    /** mmap 档文件大小（MB，缺省 128） */
    default int getRaftMmapSizeMb() {
        return 128;
    }

    /**
     * 状态机检查点触发阈值（applied 增量，缺省 1024；仅 Mmap 档生效）。
     * 高频锁场景可放大以减少检查点频率，低频场景可缩小以缩短重启恢复时长。
     */
    default int getRaftCheckpointInterval() {
        return 1024;
    }

    /**
     * 分布式锁默认租约时长（毫秒，缺省 30000）。
     * 租约越短迁移越快（失联接管时延 = 剩余租约），越短也意味着续期开销越高。
     */
    default long getLockLeaseMs() {
        return 30000L;
    }

    /**
     * 父组端口偏移（跨机房级联用，缺省 1000）。
     *
     * <p>父组（跨机房层）绑定的端口 = 本节点子组端口 + 本偏移。
     * 子组与父组必须<b>端口隔离</b>：两组是彼此独立的 Raft 实例，
     * 各有各的 term、日志与选举定时器，共用端口会导致消息串组。</p>
     *
     * <p>注意：所有节点必须使用<b>相同</b>的偏移值，否则互不可达。</p>
     */
    default int getCrossPortOffset() {
        return 1000;
    }

    /**
     * 机房权重显式配置（跨机房父组用，键 = 机房标识，值 = 该机房在父组中的权重）。
     *
     * <p>返回空 Map 表示未显式配置，此时由门面按机房索引派生：
     * {@code 权重 = 机房总数 - 索引}（即索引越靠前权重越高，主机房最高）。</p>
     *
     * <p><b>为什么父组需要权重而不是标准多数派：</b>两机房拓扑下父组只有 2 个投票成员，
     * 标准多数派无法表达"主机房优先"。通过<b>不对称</b>权重（主机房权重 ≥ required、
     * 备机房权重 &lt; required）可使主机房能单方成主而备机房不能，
     * 从而在结构上排除"双主"——详见设计文档 §8.3。</p>
     */
    default java.util.Map<String, Integer> getDatacenterWeights() {
        return java.util.Collections.emptyMap();
    }

    /**
     * 人工升级 API 的 term 跃升步长（阶段五；键 = {@code promote.term.leap}）。
     *
     * <p>{@code promoteDatacenter} 被接受后，本机房父组 term 直接抬升该步长，使主机房旧任期
     * 追不上、恢复后以低 term 退让。默认 100：既保证跃升远超正常选举抖动，又不至于一次
     * 抬得过大。值必须 &gt; 0，非法值回退默认。</p>
     */
    default long getPromoteTermLeap() {
        return 100L;
    }
}
