package cn.itcraft.speedboat.config;

import java.util.Objects;

public class MembershipConfig {

    public enum RegistryImpl {
        NONE,
        NACOS,
        REDIS
    }

    private final int healthCheckIntervalMs;
    private final int failureThreshold;
    private final int confirmationPeriods;
    private final int registryCheckIntervalMs;
    private final RegistryImpl registryImpl;
    private final boolean enableAutoRemoval;

    public MembershipConfig() {
        this.healthCheckIntervalMs = 1000;
        this.failureThreshold = 3;
        this.confirmationPeriods = 2;
        this.registryCheckIntervalMs = 30000;
        this.registryImpl = RegistryImpl.NONE;
        this.enableAutoRemoval = true;
    }

    public MembershipConfig(int healthCheckIntervalMs, int failureThreshold, int confirmationPeriods,
                           int registryCheckIntervalMs, RegistryImpl registryImpl, boolean enableAutoRemoval) {
        this.healthCheckIntervalMs = healthCheckIntervalMs;
        this.failureThreshold = failureThreshold;
        this.confirmationPeriods = confirmationPeriods;
        this.registryCheckIntervalMs = registryCheckIntervalMs;
        this.registryImpl = registryImpl;
        this.enableAutoRemoval = enableAutoRemoval;
    }

    public int getHealthCheckIntervalMs() {
        return healthCheckIntervalMs;
    }

    public boolean isMembershipChangeEnabled() {
        return enableAutoRemoval || registryImpl != RegistryImpl.NONE;
    }
    
    public int getHealthCheckInterval() {
        return healthCheckIntervalMs;
    }
    
    public int getFailureThreshold() {
        return failureThreshold;
    }

    public int getConfirmationPeriods() {
        return confirmationPeriods;
    }

    public int getRegistryCheckIntervalMs() {
        return registryCheckIntervalMs;
    }

    public RegistryImpl getRegistryImpl() {
        return registryImpl;
    }

    public boolean isEnableAutoRemoval() {
        return enableAutoRemoval;
    }

    public long getConfirmationNanos() {
        return confirmationPeriods * healthCheckIntervalMs * 1_000_000L;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MembershipConfig that = (MembershipConfig) o;
        return healthCheckIntervalMs == that.healthCheckIntervalMs &&
                failureThreshold == that.failureThreshold &&
                confirmationPeriods == that.confirmationPeriods &&
                registryCheckIntervalMs == that.registryCheckIntervalMs &&
                enableAutoRemoval == that.enableAutoRemoval &&
                registryImpl == that.registryImpl;
    }

    @Override
    public int hashCode() {
        return Objects.hash(healthCheckIntervalMs, failureThreshold, confirmationPeriods,
                registryCheckIntervalMs, registryImpl, enableAutoRemoval);
    }

    @Override
    public String toString() {
        return "MembershipConfig{" +
                "healthCheckIntervalMs=" + healthCheckIntervalMs +
                ", failureThreshold=" + failureThreshold +
                ", confirmationPeriods=" + confirmationPeriods +
                ", registryCheckIntervalMs=" + registryCheckIntervalMs +
                ", registryImpl=" + registryImpl +
                ", enableAutoRemoval=" + enableAutoRemoval +
                '}';
    }

    public static class Builder {
        private int healthCheckIntervalMs = 1000;
        private int failureThreshold = 3;
        private int confirmationPeriods = 2;
        private int registryCheckIntervalMs = 30000;
        private RegistryImpl registryImpl = RegistryImpl.NONE;
        private boolean enableAutoRemoval = true;

        public Builder healthCheckIntervalMs(int intervalMs) {
            this.healthCheckIntervalMs = intervalMs;
            return this;
        }

        public Builder failureThreshold(int threshold) {
            this.failureThreshold = threshold;
            return this;
        }

        public Builder confirmationPeriods(int periods) {
            this.confirmationPeriods = periods;
            return this;
        }

        public Builder registryCheckIntervalMs(int intervalMs) {
            this.registryCheckIntervalMs = intervalMs;
            return this;
        }

        public Builder registryImpl(RegistryImpl impl) {
            this.registryImpl = impl;
            return this;
        }

        public Builder enableAutoRemoval(boolean enable) {
            this.enableAutoRemoval = enable;
            return this;
        }

        public MembershipConfig build() {
            return new MembershipConfig(healthCheckIntervalMs, failureThreshold, confirmationPeriods,
                    registryCheckIntervalMs, registryImpl, enableAutoRemoval);
        }
    }
}