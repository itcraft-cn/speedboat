package cn.itcraft.speedboat.strategy.group;

import cn.itcraft.speedboat.config.SpeedboatConsts;

import java.util.concurrent.ThreadLocalRandom;

/**
 * GlobalGroupStrategy 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class GlobalGroupStrategy implements GroupStrategy {
    
    @Override
    public long getElectionTimeout() {
        return SpeedboatConsts.GLOBAL_MIN_ELECTION_TIMEOUT_MS + 
               ThreadLocalRandom.current().nextLong(
                   SpeedboatConsts.GLOBAL_MAX_ELECTION_TIMEOUT_MS - SpeedboatConsts.GLOBAL_MIN_ELECTION_TIMEOUT_MS);
    }
    
    @Override
    public long getHeartbeatInterval() {
        return SpeedboatConsts.GLOBAL_HEARTBEAT_INTERVAL_MS;
    }
    
    @Override
    public boolean allowCrossGroupCommunication() {
        return true;
    }
    
    @Override
    public long getMinElectionTimeout() {
        return SpeedboatConsts.GLOBAL_MIN_ELECTION_TIMEOUT_MS;
    }
    
    @Override
    public long getMaxElectionTimeout() {
        return SpeedboatConsts.GLOBAL_MAX_ELECTION_TIMEOUT_MS;
    }
}