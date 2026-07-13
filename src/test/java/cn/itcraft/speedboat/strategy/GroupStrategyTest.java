package cn.itcraft.speedboat.strategy;

import cn.itcraft.speedboat.strategy.group.DefaultGroupStrategy;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("GroupStrategy 组策略测试")
class GroupStrategyTest {

    private GroupStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new DefaultGroupStrategy();
    }

    @Test
    @DisplayName("选举超时在有效范围内")
    void electionTimeoutIsWithinValidRange() {
        for (int i = 0; i < 100; i++) {
            long timeout = strategy.getElectionTimeout();
            assertTrue(timeout >= strategy.getMinElectionTimeout());
            assertTrue(timeout < strategy.getMaxElectionTimeout());
        }
    }

    @Test
    @DisplayName("选举超时具有随机性")
    void electionTimeoutHasRandomness() {
        boolean hasDifferentValues = false;
        long firstValue = strategy.getElectionTimeout();
        
        for (int i = 0; i < 10; i++) {
            if (strategy.getElectionTimeout() != firstValue) {
                hasDifferentValues = true;
                break;
            }
        }
        
        assertTrue(hasDifferentValues, "选举超时应该具有随机性");
    }

    @Test
    @DisplayName("心跳间隔为固定值")
    void heartbeatIntervalIsFixed() {
        assertEquals(50L, strategy.getHeartbeatInterval());
    }

    @Test
    @DisplayName("默认不允许跨组通信")
    void defaultDoesNotAllowCrossGroupCommunication() {
        assertFalse(strategy.allowCrossGroupCommunication());
    }

    @Test
    @DisplayName("最小选举超时返回正确值")
    void minElectionTimeoutReturnsCorrectValue() {
        assertEquals(150L, strategy.getMinElectionTimeout());
    }

    @Test
    @DisplayName("最大选举超时返回正确值")
    void maxElectionTimeoutReturnsCorrectValue() {
        assertEquals(300L, strategy.getMaxElectionTimeout());
    }

    @Test
    @DisplayName("心跳间隔小于最小选举超时")
    void heartbeatIntervalLessThanMinElectionTimeout() {
        assertTrue(strategy.getHeartbeatInterval() < strategy.getMinElectionTimeout());
    }

    @Test
    @DisplayName("最小选举超时小于最大选举超时")
    void minElectionTimeoutLessThanMax() {
        assertTrue(strategy.getMinElectionTimeout() < strategy.getMaxElectionTimeout());
    }

    @Test
    @DisplayName("策略接口可被自定义实现")
    void strategyCanBeCustomized() {
        GroupStrategy customStrategy = new GroupStrategy() {
            @Override
            public long getElectionTimeout() {
                return 200L;
            }

            @Override
            public long getHeartbeatInterval() {
                return 40L;
            }

            @Override
            public boolean allowCrossGroupCommunication() {
                return true;
            }

            @Override
            public long getMinElectionTimeout() {
                return 200L;
            }

            @Override
            public long getMaxElectionTimeout() {
                return 400L;
            }
        };
        
        assertEquals(200L, customStrategy.getElectionTimeout());
        assertEquals(40L, customStrategy.getHeartbeatInterval());
        assertTrue(customStrategy.allowCrossGroupCommunication());
        assertEquals(200L, customStrategy.getMinElectionTimeout());
        assertEquals(400L, customStrategy.getMaxElectionTimeout());
    }

    @Test
    @DisplayName("多次调用选举超时返回不同值")
    void multipleElectionTimeoutCallsReturnDifferentValues() {
        long[] values = new long[20];
        for (int i = 0; i < 20; i++) {
            values[i] = strategy.getElectionTimeout();
        }
        
        int uniqueCount = 0;
        for (int i = 1; i < values.length; i++) {
            if (values[i] != values[0]) {
                uniqueCount++;
            }
        }
        
        assertTrue(uniqueCount > 0, "多次调用应返回不同值");
    }
}