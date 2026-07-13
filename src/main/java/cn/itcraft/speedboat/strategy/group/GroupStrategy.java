package cn.itcraft.speedboat.strategy.group;

public interface GroupStrategy {
    
    long getElectionTimeout();
    
    long getHeartbeatInterval();
    
    boolean allowCrossGroupCommunication();
    
    long getMinElectionTimeout();
    
    long getMaxElectionTimeout();
}