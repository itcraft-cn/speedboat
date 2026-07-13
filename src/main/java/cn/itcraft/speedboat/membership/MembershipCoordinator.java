package cn.itcraft.speedboat.membership;

import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.config.MembershipConfig;
import cn.itcraft.speedboat.strategy.membership.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MembershipCoordinator 协调成员变更的各个策略组件
 * 
 * 职责：
 * 1. 协调健康检测、注册中心、变更验证三个策略组件
 * 2. 管理变更检测任务的生命周期
 * 3. 提供统一的成员变更接口
 * 4. 维护故障记录状态
 */
public class MembershipCoordinator {
    
    private static final Logger logger = LoggerFactory.getLogger(MembershipCoordinator.class);
    
    private final RaftNode raftNode;
    private final MembershipConfig config;
    private final HealthCheckStrategy healthCheckStrategy;
    private final RegistryStrategy registryStrategy;
    private final ChangeValidationStrategy changeValidationStrategy;
    
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> detectionFuture;
    
    public MembershipCoordinator(RaftNode raftNode) {
        this.raftNode = raftNode;
        this.config = raftNode.getMembershipConfig();
        this.healthCheckStrategy = raftNode.getHealthCheckStrategy();
        this.registryStrategy = raftNode.getRegistryStrategy();
        this.changeValidationStrategy = raftNode.getChangeValidationStrategy();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }
    
    /**
     * 启动成员变更协调器
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            // 启动所有策略组件
            healthCheckStrategy.start();
            registryStrategy.start();
            
            // 如果启用了成员变更检测，启动定时任务
            if (config.isMembershipChangeEnabled()) {
                long checkInterval = config.getHealthCheckInterval();
                detectionFuture = scheduler.scheduleAtFixedRate(
                    this::detectAndProposeChanges,
                    checkInterval,
                    checkInterval,
                    TimeUnit.MILLISECONDS
                );
                logger.info("Started membership change detection with interval {} ms", checkInterval);
            }
            
            logger.info("MembershipCoordinator started for node {}", raftNode.getNodeId());
        }
    }
    
    /**
     * 停止成员变更协调器
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            // 停止检测任务
            if (detectionFuture != null) {
                detectionFuture.cancel(false);
                detectionFuture = null;
            }
            
            // 停止策略组件
            healthCheckStrategy.stop();
            registryStrategy.stop();
            
            // 关闭调度器
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while waiting for scheduler shutdown", e);
            }
            
            logger.info("MembershipCoordinator stopped for node {}", raftNode.getNodeId());
        }
    }
    
    /**
     * 检测并提议成员变更
     * 整合三个策略组件的检测结果，做出最终决策
     */
    private void detectAndProposeChanges() {
        if (!raftNode.isLeader()) {
            // 只有 Leader 节点可以提议变更
            return;
        }
        
        logger.debug("Detecting membership changes for node {}", raftNode.getNodeId());
        
        // 1. 执行健康检测
        List<String> unhealthyPeers = healthCheckStrategy.checkUnhealthyPeers(raftNode.getPeerIds());
        if (!unhealthyPeers.isEmpty()) {
            logger.info("Detected unhealthy peers: {}", unhealthyPeers);
            for (String peerId : unhealthyPeers) {
                processUnhealthyPeer(peerId);
            }
        }
        
        // 2. 检查注册中心差异
        List<String> registeredPeers = registryStrategy.getRegisteredPeers();
        if (registeredPeers != null && !registeredPeers.isEmpty()) {
            detectRegistryChanges(registeredPeers);
        }
    }
    
    /**
     * 处理不健康的对等节点
     */
    private void processUnhealthyPeer(String peerId) {
        // 获取故障记录
        cn.itcraft.speedboat.raft.FailureRecord record = 
            raftNode.getFailureRecords().computeIfAbsent(peerId, 
                k -> new cn.itcraft.speedboat.raft.FailureRecord(peerId));
        
        // 记录故障
        record.incrementFailures();
        
        // 检查是否应该提议移除
        if (record.shouldProposeRemoval(
                config.getFailureThreshold(),
                config.getConfirmationNanos())) {
            
            // 验证是否允许移除
            if (changeValidationStrategy.validateRemove(peerId, raftNode.getPeerIds())) {
                logger.info("Proposing removal of unhealthy peer: {}", peerId);
                boolean success = raftNode.proposeRemoveMember(peerId);
                if (success) {
                    // 成功提议后清除故障记录
                    raftNode.getFailureRecords().remove(peerId);
                }
            } else {
                logger.debug("Removal validation failed for peer: {}", peerId);
            }
        }
    }
    
    /**
     * 检测注册中心变更
     */
    private void detectRegistryChanges(List<String> registeredPeers) {
        List<String> currentPeers = raftNode.getPeerIds();
        
        // 发现新节点
        for (String newPeer : registeredPeers) {
            if (!currentPeers.contains(newPeer) && !newPeer.equals(raftNode.getNodeId())) {
                if (changeValidationStrategy.validateAdd(newPeer, currentPeers)) {
                    logger.info("Detected new peer in registry: {}", newPeer);
                    boolean success = raftNode.proposeAddMember(newPeer);
                    if (success) {
                        // 添加到健康检测
                        if (healthCheckStrategy instanceof cn.itcraft.speedboat.strategy.membership.impl.PassiveHealthCheckStrategy) {
                            ((cn.itcraft.speedboat.strategy.membership.impl.PassiveHealthCheckStrategy) healthCheckStrategy)
                                .markHealthy(newPeer);
                        }
                    }
                }
            }
        }
        
        // 发现已移除节点（在注册中心消失但仍在集群中）
        for (String currentPeer : currentPeers) {
            if (!registeredPeers.contains(currentPeer)) {
                // 检查是否为注册中心策略支持的节点（可能有些节点不在注册中心管理范围内）
                if (registryStrategy.isRegistered(currentPeer)) {
                    logger.info("Detected missing peer in registry: {}", currentPeer);
                    processUnhealthyPeer(currentPeer);
                }
            }
        }
    }
    
    /**
     * 手动提议添加成员
     */
    public boolean proposeAddMember(String peerId, String address) {
        if (!raftNode.isLeader()) {
            logger.warn("Only leader can propose member additions");
            return false;
        }
        
        // 使用变更验证策略验证
        if (!changeValidationStrategy.validateAdd(peerId, raftNode.getPeerIds())) {
            logger.warn("Validation failed for adding peer: {}", peerId);
            return false;
        }
        
        // 调用 RaftNode 的方法
        boolean success = raftNode.proposeAddMember(peerId);
        if (success && healthCheckStrategy instanceof cn.itcraft.speedboat.strategy.membership.impl.PassiveHealthCheckStrategy) {
            ((cn.itcraft.speedboat.strategy.membership.impl.PassiveHealthCheckStrategy) healthCheckStrategy)
                .markHealthy(peerId);
        }
        
        return success;
    }
    
    /**
     * 手动提议移除成员
     */
    public boolean proposeRemoveMember(String peerId) {
        if (!raftNode.isLeader()) {
            logger.warn("Only leader can propose member removals");
            return false;
        }
        
        // 使用变更验证策略验证
        if (!changeValidationStrategy.validateRemove(peerId, raftNode.getPeerIds())) {
            logger.warn("Validation failed for removing peer: {}", peerId);
            return false;
        }
        
        // 调用 RaftNode 的方法
        return raftNode.proposeRemoveMember(peerId);
    }
    
    /**
     * 获取协调器运行状态
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * 获取成员变更配置
     */
    public MembershipConfig getConfig() {
        return config;
    }
    
    /**
     * 获取健康检测策略
     */
    public HealthCheckStrategy getHealthCheckStrategy() {
        return healthCheckStrategy;
    }
    
    /**
     * 获取注册中心策略
     */
    public RegistryStrategy getRegistryStrategy() {
        return registryStrategy;
    }
    
    /**
     * 获取变更验证策略
     */
    public ChangeValidationStrategy getChangeValidationStrategy() {
        return changeValidationStrategy;
    }
}