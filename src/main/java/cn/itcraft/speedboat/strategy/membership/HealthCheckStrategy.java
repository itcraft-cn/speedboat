package cn.itcraft.speedboat.strategy.membership;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 健康检查策略接口，定义节点健康状态检测的契约。
 * 
 * <p>用于检测集群中节点的可用性，支持主动探测和被动监控两种模式。</p>
 * 
 * @author speedboat
 * @since 1.0.0
 */
public interface HealthCheckStrategy {

    Set<String> checkHealth(Set<String> nodeIds);
    
    default List<String> checkUnhealthyPeers(List<String> nodeIds) {
        Set<String> healthy = checkHealth(Collections.unmodifiableSet(new HashSet<>(nodeIds)));
        return nodeIds.stream()
            .filter(nodeId -> !healthy.contains(nodeId))
            .collect(Collectors.toList());
    }
    
    default boolean isHealthy(String nodeId) {
        return checkHealth(Collections.singleton(nodeId)).contains(nodeId);
    }
    
    void start();
    
    void stop();
    
    boolean isRunning();
}