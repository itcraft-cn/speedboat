package cn.itcraft.speedboat.integration;

import cn.itcraft.speedboat.membership.MembershipCoordinator;
import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.strategy.membership.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class MembershipCoordinatorIntegrationTest {

    private RaftNode raftNode;
    private MembershipCoordinator coordinator;
    private MockHealthCheckStrategy healthCheckStrategy;
    private MockRegistryStrategy registryStrategy;
    private MockChangeValidationStrategy validationStrategy;

    @BeforeEach
    void setUp() {
        healthCheckStrategy = new MockHealthCheckStrategy();
        registryStrategy = new MockRegistryStrategy();
        validationStrategy = new MockChangeValidationStrategy();

        raftNode = new RaftNode.Builder()
                .nodeId("test-node-1")
                .peerIds(Arrays.asList("node-1", "node-2"))
                .healthCheckStrategy(healthCheckStrategy)
                .registryStrategy(registryStrategy)
                .changeValidationStrategy(validationStrategy)
                .build();

        coordinator = new MembershipCoordinator(raftNode);
    }

    @Test
    void testCoordinatorStartStop() {
        coordinator.start();
        assertTrue(coordinator.isRunning());
        
        coordinator.stop();
        assertFalse(coordinator.isRunning());
    }

    @Test
    void testHealthCheckDetection() {
        coordinator.start();
        
        healthCheckStrategy.setUnhealthyPeers(new HashSet<>(Arrays.asList("failing-node-1", "failing-node-2")));
        
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        coordinator.getHealthCheckStrategy().checkHealth(new HashSet<>(Arrays.asList("node-1", "node-2", "failing-node-1", "failing-node-2")));
        
        assertTrue(healthCheckStrategy.isStarted());
    }

    @Test
    void testRegistryValidation() {
        coordinator.start();
        
        Map<String, String> registry = new HashMap<>();
        registry.put("node-1", "127.0.0.1:8080");
        registry.put("node-2", "127.0.0.1:8081");
        registry.put("node-3", "127.0.0.1:8082");
        registry.put("node-4", "127.0.0.1:8083");
        registryStrategy.setRegisteredPeers(registry);
        
        List<String> registered = registryStrategy.getRegisteredPeers();
        assertNotNull(registered);
        assertEquals(4, registered.size());
        assertTrue(registered.containsAll(Arrays.asList("node-1", "node-2", "node-3", "node-4")));
    }

    @Test
    void testProposeMemberChange() {
        coordinator.start();
        
        boolean canAdd = validationStrategy.validateAdd("new-node-1", Arrays.asList("node-1", "node-2"));
        assertTrue(canAdd);
        
        boolean canRemove = validationStrategy.validateRemove("node-1", Arrays.asList("node-1", "node-2"));
        assertTrue(canRemove);
        
        assertTrue(validationStrategy.isRunning());
    }

    @Test
    void testValidationStrategyRejection() {
        boolean canAddInvalid = validationStrategy.validateAdd("node-1", Arrays.asList("node-1", "node-2"));
        assertFalse(canAddInvalid);
        
        boolean canRemoveInvalid = validationStrategy.validateRemove("node-3", Arrays.asList("node-1", "node-2"));
        assertFalse(canRemoveInvalid);
    }

    @Test
    void testCoordinatorIntegration() {
        coordinator.start();
        
        healthCheckStrategy.setUnhealthyPeers(Collections.singleton("node-2"));
        Map<String, String> registry = new HashMap<>();
        registry.put("node-1", "127.0.0.1:8080");
        registry.put("node-2", "127.0.0.1:8081");
        registry.put("node-3", "127.0.0.1:8082");
        registryStrategy.setRegisteredPeers(registry);
        
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        Set<String> healthyPeers = healthCheckStrategy.checkHealth(new HashSet<>(Arrays.asList("node-1", "node-2", "node-3")));
        assertEquals(2, healthyPeers.size());
        assertTrue(healthyPeers.contains("node-1"));
        assertTrue(healthyPeers.contains("node-3"));
        assertFalse(healthyPeers.contains("node-2"));
        
        List<String> registered = registryStrategy.getRegisteredPeers();
        assertEquals(3, registered.size());
        
        coordinator.stop();
        
        assertFalse(healthCheckStrategy.isRunning());
        assertFalse(registryStrategy.isRunning());
        assertFalse(coordinator.isRunning());
    }

    private static class MockHealthCheckStrategy implements HealthCheckStrategy {
        private Set<String> unhealthyPeers = new HashSet<>();
        private boolean running = false;
        private boolean started = false;

        void setUnhealthyPeers(Set<String> unhealthy) {
            this.unhealthyPeers = new HashSet<>(unhealthy);
        }

        @Override
        public Set<String> checkHealth(Set<String> nodeIds) {
            Set<String> result = new HashSet<>(nodeIds);
            result.removeAll(unhealthyPeers);
            return result;
        }

        @Override
        public void start() {
            started = true;
            running = true;
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        public boolean isStarted() {
            return started;
        }
    }

    private static class MockRegistryStrategy implements RegistryStrategy {
        private Map<String, String> registeredNodes = new HashMap<>();
        private boolean running = false;

        void setRegisteredPeers(Map<String, String> nodes) {
            this.registeredNodes = nodes;
        }

        @Override
        public Map<String, String> getAllRegisteredNodes() {
            return new HashMap<>(registeredNodes);
        }

        @Override
        public boolean isRegistered(String nodeId) {
            return registeredNodes.containsKey(nodeId);
        }

        @Override
        public String getAddress(String nodeId) {
            return registeredNodes.get(nodeId);
        }

        @Override
        public boolean isValid(String nodeId, String address) {
            return registeredNodes.containsKey(nodeId) && registeredNodes.get(nodeId).equals(address);
        }

        @Override
        public void start() {
            running = true;
        }

        @Override
        public void stop() {
            running = false;
        }

        public boolean isRunning() {
            return running;
        }
    }

    private static class MockChangeValidationStrategy implements ChangeValidationStrategy {
        private boolean running = true;

        @Override
        public boolean canProposeChange(RaftNode raftNode, cn.itcraft.speedboat.raft.MemberChangeEntry proposedEntry) {
            return true;
        }

        @Override
        public boolean shouldAcceptChange(RaftNode raftNode, cn.itcraft.speedboat.raft.MemberChangeEntry proposedEntry) {
            return true;
        }

        @Override
        public boolean validateAdd(String newPeerId, List<String> currentPeerIds) {
            // 基础验证：节点不能已存在
            return !currentPeerIds.contains(newPeerId);
        }

        @Override
        public boolean validateRemove(String peerId, List<String> currentPeerIds) {
            // 基础验证：节点必须存在
            return currentPeerIds.contains(peerId);
        }

        public boolean isRunning() {
            return running;
        }
    }
}