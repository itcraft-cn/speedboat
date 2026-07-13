package cn.itcraft.speedboat.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SpeedboatConsts 常量类测试
 * 
 * <p>测试常量值的合理性和正确性</p>
 *
 * @author speedboat
 * @since 1.0.0
 */
@DisplayName("SpeedboatConsts 常量测试")
class SpeedboatConstsTest {

    @Test
    @DisplayName("选举超时时间范围应合理")
    void testElectionTimeoutRange() {
        assertTrue(SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS > 0, 
            "最小选举超时应大于0");
        assertTrue(SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS > SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS,
            "最大选举超时应大于最小选举超时");
        assertTrue(SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS >= 150,
            "最小选举超时应至少150ms以保证安全性");
        assertTrue(SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS <= 500,
            "最大选举超时应不超过500ms以实现亚秒级切换");
    }

    @Test
    @DisplayName("心跳间隔应小于选举超时")
    void testHeartbeatIntervalLessThanElectionTimeout() {
        assertTrue(SpeedboatConsts.HEARTBEAT_INTERVAL_MS < SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS,
            "心跳间隔应小于最小选举超时，避免不必要的选举");
        assertTrue(SpeedboatConsts.HEARTBEAT_INTERVAL_MS <= 100,
            "心跳间隔应足够小以快速检测故障");
    }

    @Test
    @DisplayName("全局选举超时应大于单节点选举超时")
    void testGlobalElectionTimeoutGreaterThanLocal() {
        assertTrue(SpeedboatConsts.GLOBAL_MIN_ELECTION_TIMEOUT_MS > SpeedboatConsts.MIN_ELECTION_TIMEOUT_MS,
            "全局最小选举超时应大于单节点最小选举超时");
        assertTrue(SpeedboatConsts.GLOBAL_MAX_ELECTION_TIMEOUT_MS > SpeedboatConsts.MAX_ELECTION_TIMEOUT_MS,
            "全局最大选举超时应大于单节点最大选举超时");
    }

    @Test
    @DisplayName("全局心跳间隔应小于全局选举超时")
    void testGlobalHeartbeatIntervalLessThanGlobalElectionTimeout() {
        assertTrue(SpeedboatConsts.GLOBAL_HEARTBEAT_INTERVAL_MS < SpeedboatConsts.GLOBAL_MIN_ELECTION_TIMEOUT_MS,
            "全局心跳间隔应小于全局最小选举超时");
    }

    @Test
    @DisplayName("序列化相关常量应正确")
    void testSerializationConstants() {
        assertEquals(2, SpeedboatConsts.DEFAULT_SERIALIZATION_TYPE,
            "默认序列化类型应为2");
        assertEquals(9, SpeedboatConsts.SERIALIZATION_HEADER_LEN,
            "序列化头长度应为9字节");
        assertEquals(4, SpeedboatConsts.CRC32_LEN,
            "CRC32长度应为4字节");
        assertEquals(1, SpeedboatConsts.TYPE_LEN,
            "类型字段长度应为1字节");
        assertTrue(SpeedboatConsts.SERIALIZATION_HEADER_LEN > SpeedboatConsts.CRC32_LEN + SpeedboatConsts.TYPE_LEN,
            "序列化头长度应大于CRC32和类型字段之和");
    }

    @Test
    @DisplayName("端口常量应有效")
    void testPortConstants() {
        assertTrue(SpeedboatConsts.DEFAULT_PORT > 1024,
            "默认端口应大于1024（避免系统保留端口）");
        assertTrue(SpeedboatConsts.DEFAULT_PORT < 65536,
            "默认端口应小于65536");
        assertTrue(SpeedboatConsts.GLOBAL_PORT > 1024,
            "全局端口应大于1024");
        assertTrue(SpeedboatConsts.GLOBAL_PORT < 65536,
            "全局端口应小于65536");
        assertNotEquals(SpeedboatConsts.DEFAULT_PORT, SpeedboatConsts.GLOBAL_PORT,
            "默认端口和全局端口应不同");
    }

    @Test
    @DisplayName("常量类应有私有构造函数")
    void testPrivateConstructor() throws Exception {
        java.lang.reflect.Constructor<SpeedboatConsts> constructor = 
            SpeedboatConsts.class.getDeclaredConstructor();
        assertFalse(constructor.isAccessible(), 
            "构造函数应不可访问");
        
        constructor.setAccessible(true);
        assertNotNull(constructor.newInstance(),
            "通过反射调用构造函数应返回实例");
    }
}