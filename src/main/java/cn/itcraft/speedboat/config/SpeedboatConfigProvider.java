package cn.itcraft.speedboat.config;

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
    default cn.itcraft.speedboat.config.MembershipConfig getMembershipConfig() {
        return new cn.itcraft.speedboat.config.MembershipConfig();
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
}
