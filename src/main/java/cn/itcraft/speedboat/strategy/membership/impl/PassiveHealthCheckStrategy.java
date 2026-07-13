package cn.itcraft.speedboat.strategy.membership.impl;

import cn.itcraft.speedboat.strategy.membership.HealthCheckStrategy;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PassiveHealthCheckStrategy implements HealthCheckStrategy {

    private final Set<String> knownHealthyNodes = new HashSet<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    public PassiveHealthCheckStrategy() {
    }

    @Override
    public Set<String> checkHealth(Set<String> nodeIds) {
        synchronized (knownHealthyNodes) {
            Set<String> healthy = new HashSet<>(nodeIds);
            healthy.retainAll(knownHealthyNodes);
            return healthy;
        }
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            // 被动检测无需主动定时任务
            // 仅初始化状态
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    public void markHealthy(String nodeId) {
        synchronized (knownHealthyNodes) {
            knownHealthyNodes.add(nodeId);
        }
    }

    public void markUnhealthy(String nodeId) {
        synchronized (knownHealthyNodes) {
            knownHealthyNodes.remove(nodeId);
        }
    }

    public void clear() {
        synchronized (knownHealthyNodes) {
            knownHealthyNodes.clear();
        }
    }
}