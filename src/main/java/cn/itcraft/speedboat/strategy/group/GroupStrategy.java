package cn.itcraft.speedboat.strategy.group;

/**
 * Raft 组策略接口，定义 Raft 组的选举和通信行为。
 * 
 * <p>GroupStrategy 用于实现不同的 Raft 组管理策略，支持单机房扁平模式和跨机房级联模式。
 * 通过策略模式将组管理逻辑从核心 Raft 实现中解耦。</p>
 * 
 * <p>核心职责：</p>
 * <ul>
 *   <li><b>选举超时控制</b>：确定组的选举超时时间范围</li>
 *   <li><b>心跳间隔配置</b>：设置 Leader 发送心跳的频率</li>
 *   <li><b>跨组通信许可</b>：控制是否允许不同组间的直接通信</li>
 *   <li><b>超时范围管理</b>：提供最小和最大选举超时边界</li>
 * </ul>
 * 
 * <p>策略实现：</p>
 * <ul>
 *   <li>{@link DefaultGroupStrategy}：默认策略，适用于单机房场景</li>
 *   <li>{@link DatacenterGroupStrategy}：数据中心感知策略，支持跨机房级联</li>
 *   <li>{@link GlobalGroupStrategy}：全局单组策略，将所有节点视为一个组</li>
 * </ul>
 * 
 * <p>级联架构：</p>
 * <ol>
 *   <li><b>父组</b>：管理跨机房选举，使用较长的选举超时</li>
 *   <li><b>子组</b>：管理机房内选举，使用较短的选举超时</li>
 *   <li><b>通信控制</b>：子组内节点直接通信，跨组通信通过父组协调</li>
 * </ol>
 * 
 * <p>配置示例：</p>
 * <pre>{@code
 * // 单机房默认策略
 * GroupStrategy flatStrategy = new DefaultGroupStrategy();
 * // 选举超时：150-300ms，心跳间隔：50ms
 * 
 * // 跨机房级联策略
 * GroupStrategy cascadeStrategy = new DatacenterGroupStrategy();
 * // 机房内选举超时：150-300ms
 * // 机房间选举超时：1000-2000ms
 * }</pre>
 * 
 * @author speedboat
 * @see RaftGroup
 * @since 1.0.0
 */
public interface GroupStrategy {
    
    long getElectionTimeout();
    
    long getHeartbeatInterval();
    
    boolean allowCrossGroupCommunication();
    
    long getMinElectionTimeout();
    
    long getMaxElectionTimeout();
}