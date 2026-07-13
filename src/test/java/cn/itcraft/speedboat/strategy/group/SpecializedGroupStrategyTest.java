package cn.itcraft.speedboat.strategy.group;

import cn.itcraft.speedboat.config.SpeedboatConsts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("专用组策略测试")
class SpecializedGroupStrategyTest {

    private DatacenterGroupStrategy datacenterGroupStrategy;
    private GlobalGroupStrategy globalGroupStrategy;

    @BeforeEach
    void setUp() {
        datacenterGroupStrategy = new DatacenterGroupStrategy();
        globalGroupStrategy = new GlobalGroupStrategy();
    }

    @Test
    @DisplayName("DatacenterGroupStrategy - 获取心跳间隔应返回默认心跳间隔")
    void datacenterGetHeartbeatInterval_shouldReturnDefaultHeartbeatInterval() {
        assertEquals(SpeedboatConsts.HEARTBEAT_INTERVAL_MS, datacenterGroupStrategy.getHeartbeatInterval());
    }

    @Test
    @DisplayName("DatacenterGroupStrategy - 不允许跨组通信")
    void datacenterAllowCrossGroupCommunication_shouldReturnFalse() {
        assertFalse(datacenterGroupStrategy.allowCrossGroupCommunication());
    }

    @Test
    @DisplayName("DatacenterGroupStrategy - 获取最小选举超时")
    void datacenterGetMinElectionTimeout_shouldReturnMinElectionTimeout() {
        assertEquals(SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS, datacenterGroupStrategy.getMinElectionTimeout());
    }

    @Test
    @DisplayName("DatacenterGroupStrategy - 获取最大选举超时")
    void datacenterGetMaxElectionTimeout_shouldReturnMaxElectionTimeout() {
        assertEquals(SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS, datacenterGroupStrategy.getMaxElectionTimeout());
    }

    @RepeatedTest(100)
    @DisplayName("DatacenterGroupStrategy - 选举超时应在有效范围内")
    void datacenterGetElectionTimeout_shouldBeInValidRange() {
        long timeout = datacenterGroupStrategy.getElectionTimeout();
        assertTrue(timeout >= SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS,
                "选举超时应大于等于最小值: " + timeout);
        assertTrue(timeout < SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS,
                "选举超时应小于最大值: " + timeout);
    }

    @Test
    @DisplayName("GlobalGroupStrategy - 获取心跳间隔应返回全局心跳间隔")
    void globalGetHeartbeatInterval_shouldReturnGlobalHeartbeatInterval() {
        assertEquals(SpeedboatConsts.GLOBAL_HEARTBEAT_INTERVAL_MS, globalGroupStrategy.getHeartbeatInterval());
    }

    @Test
    @DisplayName("GlobalGroupStrategy - 允许跨组通信")
    void globalAllowCrossGroupCommunication_shouldReturnTrue() {
        assertTrue(globalGroupStrategy.allowCrossGroupCommunication());
    }

    @Test
    @DisplayName("GlobalGroupStrategy - 获取最小选举超时")
    void globalGetMinElectionTimeout_shouldReturnGlobalMinElectionTimeout() {
        assertEquals(SpeedboatConsts.GLOBAL_MIN_ELECTION_TIMEOUT_MS, globalGroupStrategy.getMinElectionTimeout());
    }

    @Test
    @DisplayName("GlobalGroupStrategy - 获取最大选举超时")
    void globalGetMaxElectionTimeout_shouldReturnGlobalMaxElectionTimeout() {
        assertEquals(SpeedboatConsts.GLOBAL_MAX_ELECTION_TIMEOUT_MS, globalGroupStrategy.getMaxElectionTimeout());
    }

    @RepeatedTest(100)
    @DisplayName("GlobalGroupStrategy - 选举超时应在有效范围内")
    void globalGetElectionTimeout_shouldBeInValidRange() {
        long timeout = globalGroupStrategy.getElectionTimeout();
        assertTrue(timeout >= SpeedboatConsts.GLOBAL_MIN_ELECTION_TIMEOUT_MS,
                "选举超时应大于等于最小值: " + timeout);
        assertTrue(timeout < SpeedboatConsts.GLOBAL_MAX_ELECTION_TIMEOUT_MS,
                "选举超时应小于最大值: " + timeout);
    }

    @Test
    @DisplayName("策略对比 - 全局策略的选举超时应大于机房内策略")
    void globalElectionTimeout_shouldBeLargerThanDatacenter() {
        assertTrue(globalGroupStrategy.getMinElectionTimeout() > datacenterGroupStrategy.getMaxElectionTimeout(),
                "全局最小选举超时应大于机房内最大选举超时");
    }

    @Test
    @DisplayName("策略对比 - 全局策略的心跳间隔应大于机房内策略")
    void globalHeartbeatInterval_shouldBeLargerThanDatacenter() {
        assertTrue(globalGroupStrategy.getHeartbeatInterval() > datacenterGroupStrategy.getHeartbeatInterval(),
                "全局心跳间隔应大于机房内心跳间隔");
    }

    @Test
    @DisplayName("策略对比 - 两种策略的跨组通信能力应相反")
    void crossGroupCommunication_shouldBeOpposite() {
        assertNotEquals(datacenterGroupStrategy.allowCrossGroupCommunication(),
                globalGroupStrategy.allowCrossGroupCommunication(),
                "两种策略的跨组通信能力应该相反");
    }
}