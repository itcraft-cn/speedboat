package cn.itcraft.speedboat.strategy.membership.impl;

import cn.itcraft.speedboat.strategy.membership.RegistryStrategy;

import java.util.Collections;
import java.util.Map;

/**
 * 空操作注册中心策略，用于禁用外部注册中心集成。
 * 
 * <p>当不需要外部服务发现时使用此策略，返回空节点列表。</p>
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class NoOpRegistryStrategy implements RegistryStrategy {

    private final Map<String, String> emptyMap = Collections.emptyMap();

    @Override
    public Map<String, String> getAllRegisteredNodes() {
        return emptyMap;
    }

    @Override
    public boolean isRegistered(String nodeId) {
        return false;
    }

    @Override
    public String getAddress(String nodeId) {
        return null;
    }

    @Override
    public boolean isValid(String nodeId, String address) {
        return false;
    }

    @Override
    public void start() {
        // 无操作
    }

    @Override
    public void stop() {
        // 无操作
    }
}