package cn.itcraft.speedboat.strategy.group;

import cn.itcraft.speedboat.config.SpeedboatConsts;
import java.util.concurrent.ThreadLocalRandom;

public class DefaultGroupStrategy implements GroupStrategy {
    
    @Override
    public long getElectionTimeout() {
        return SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS + 
               ThreadLocalRandom.current().nextLong(
                   SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS - SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS);
    }
    
    @Override
    public long getHeartbeatInterval() {
        return SpeedboatConsts.HEARTBEAT_INTERVAL_MS;
    }
    
    @Override
    public boolean allowCrossGroupCommunication() {
        return false;
    }
    
    @Override
    public long getMinElectionTimeout() {
        return SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS;
    }
    
    @Override
    public long getMaxElectionTimeout() {
        return SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS;
    }
}