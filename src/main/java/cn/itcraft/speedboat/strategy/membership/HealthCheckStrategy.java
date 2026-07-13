package cn.itcraft.speedboat.strategy.membership;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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