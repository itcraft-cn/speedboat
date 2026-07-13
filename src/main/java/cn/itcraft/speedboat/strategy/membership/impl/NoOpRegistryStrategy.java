package cn.itcraft.speedboat.strategy.membership.impl;

import cn.itcraft.speedboat.strategy.membership.RegistryStrategy;

import java.util.Collections;
import java.util.Map;

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