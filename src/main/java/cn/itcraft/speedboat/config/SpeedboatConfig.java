package cn.itcraft.speedboat.config;

import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;

import java.util.Objects;

/**
 * Speedboat 配置类
 * 
 * <p>使用 Builder 模式构建配置对象，支持：</p>
 * <ul>
 *   <li>端口配置</li>
 *   <li>投票权重策略</li>
 *   <li>分组策略</li>
 *   <li>数据中心ID</li>
 * </ul>
 *
 * @author speedboat
 * @since 1.0.0
 */
public class SpeedboatConfig {

    private final int port;
    
    private final VoteWeightStrategy voteWeightStrategy;
    
    private final GroupStrategy groupStrategy;
    
    private final String datacenterId;

    private SpeedboatConfig(Builder builder) {
        this.port = builder.port;
        this.voteWeightStrategy = builder.voteWeightStrategy;
        this.groupStrategy = builder.groupStrategy;
        this.datacenterId = builder.datacenterId;
    }

    public int getPort() {
        return port;
    }

    public VoteWeightStrategy getVoteWeightStrategy() {
        return voteWeightStrategy;
    }

    public GroupStrategy getGroupStrategy() {
        return groupStrategy;
    }

    public String getDatacenterId() {
        return datacenterId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder from(SpeedboatConfig config) {
        Objects.requireNonNull(config, "config cannot be null");
        return new Builder()
            .port(config.port)
            .voteWeightStrategy(config.voteWeightStrategy)
            .groupStrategy(config.groupStrategy)
            .datacenterId(config.datacenterId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SpeedboatConfig that = (SpeedboatConfig) o;
        return port == that.port
            && Objects.equals(voteWeightStrategy, that.voteWeightStrategy)
            && Objects.equals(groupStrategy, that.groupStrategy)
            && Objects.equals(datacenterId, that.datacenterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(port, voteWeightStrategy, groupStrategy, datacenterId);
    }

    @Override
    public String toString() {
        return "SpeedboatConfig{"
            + "port=" + port
            + ", voteWeightStrategy=" + voteWeightStrategy
            + ", groupStrategy=" + groupStrategy
            + ", datacenterId='" + datacenterId + '\''
            + '}';
    }

    public static final class Builder {
        
        private int port;
        
        private VoteWeightStrategy voteWeightStrategy;
        
        private GroupStrategy groupStrategy;
        
        private String datacenterId;

        private Builder() {
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder voteWeightStrategy(VoteWeightStrategy voteWeightStrategy) {
            this.voteWeightStrategy = voteWeightStrategy;
            return this;
        }

        public Builder groupStrategy(GroupStrategy groupStrategy) {
            this.groupStrategy = groupStrategy;
            return this;
        }

        public Builder datacenterId(String datacenterId) {
            this.datacenterId = datacenterId;
            return this;
        }

        public SpeedboatConfig build() {
            return new SpeedboatConfig(this);
        }
    }
}