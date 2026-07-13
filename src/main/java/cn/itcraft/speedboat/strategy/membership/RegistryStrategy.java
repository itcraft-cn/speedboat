package cn.itcraft.speedboat.strategy.membership;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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