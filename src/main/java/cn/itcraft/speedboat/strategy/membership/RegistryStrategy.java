package cn.itcraft.speedboat.strategy.membership;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 注册中心策略接口，定义与外部服务发现系统（如 Nacos、Redis）集成的契约。
 * 
 * <p>用于动态发现集群节点，支持自动成员添加和移除。</p>
 * 
 * @author speedboat
 * @since 1.0.0
 */
public interface RegistryStrategy {

    Map<String, String> getAllRegisteredNodes();
    
    default List<String> getRegisteredPeers() {
        Map<String, String> nodes = getAllRegisteredNodes();
        if (nodes == null || nodes.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableList(new ArrayList<>(nodes.keySet()));
    }
    
    boolean isRegistered(String nodeId);
    
    String getAddress(String nodeId);
    
    boolean isValid(String nodeId, String address);
    
    void start();
    
    void stop();
}