package cn.itcraft.speedboat.config;

import cn.itcraft.speedboat.strategy.group.DefaultGroupStrategy;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.strategy.voteweight.DefaultVoteWeightStrategy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SpeedboatConfig 配置类测试
 * 
 * <p>测试配置类的默认值和Builder功能</p>
 *
 * @author speedboat
 * @since 1.0.0
 */
@DisplayName("SpeedboatConfig 配置测试")
class SpeedboatConfigTest {

    @Test
    @DisplayName("Builder应创建配置实例")
    void testBuilderCreatesConfig() {
        SpeedboatConfig config = SpeedboatConfig.builder()
            .port(22222)
            .datacenterId("dc1")
            .build();
        
        assertNotNull(config, "配置实例不应为null");
        assertEquals(22222, config.getPort(), "端口应匹配");
        assertEquals("dc1", config.getDatacenterId(), "数据中心ID应匹配");
    }

    @Test
    @DisplayName("Builder应支持所有字段设置")
    void testBuilderWithAllFields() {
        VoteWeightStrategy voteWeightStrategy = new DefaultVoteWeightStrategy();
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        
        SpeedboatConfig config = SpeedboatConfig.builder()
            .port(33333)
            .voteWeightStrategy(voteWeightStrategy)
            .groupStrategy(groupStrategy)
            .datacenterId("dc-test")
            .build();
        
        assertEquals(33333, config.getPort(), "端口应匹配");
        assertEquals(voteWeightStrategy, config.getVoteWeightStrategy(), "投票权重策略应匹配");
        assertEquals(groupStrategy, config.getGroupStrategy(), "分组策略应匹配");
        assertEquals("dc-test", config.getDatacenterId(), "数据中心ID应匹配");
    }

    @Test
    @DisplayName("from方法应复制配置")
    void testFromCopiesConfig() {
        VoteWeightStrategy voteWeightStrategy = new DefaultVoteWeightStrategy();
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        
        SpeedboatConfig original = SpeedboatConfig.builder()
            .port(22222)
            .voteWeightStrategy(voteWeightStrategy)
            .groupStrategy(groupStrategy)
            .datacenterId("original")
            .build();
        
        SpeedboatConfig copied = SpeedboatConfig.from(original).build();
        
        assertEquals(original.getPort(), copied.getPort(), "端口应相同");
        assertEquals(original.getVoteWeightStrategy(), copied.getVoteWeightStrategy(), 
            "投票权重策略应相同");
        assertEquals(original.getGroupStrategy(), copied.getGroupStrategy(),
            "分组策略应相同");
        assertEquals(original.getDatacenterId(), copied.getDatacenterId(),
            "数据中心ID应相同");
        assertNotSame(original, copied, "应为不同实例");
    }

    @Test
    @DisplayName("from方法应支持修改部分配置")
    void testFromWithModification() {
        SpeedboatConfig original = SpeedboatConfig.builder()
            .port(22222)
            .datacenterId("original")
            .build();
        
        SpeedboatConfig modified = SpeedboatConfig.from(original)
            .port(33333)
            .build();
        
        assertEquals(33333, modified.getPort(), "端口应被修改");
        assertEquals("original", modified.getDatacenterId(), "数据中心ID应保持不变");
    }

    @Test
    @DisplayName("Builder链式调用应正常工作")
    void testBuilderChaining() {
        SpeedboatConfig config = SpeedboatConfig.builder()
            .port(22222)
            .datacenterId("dc1")
            .port(33333)
            .datacenterId("dc2")
            .build();
        
        assertEquals(33333, config.getPort(), "应使用最后设置的端口");
        assertEquals("dc2", config.getDatacenterId(), "应使用最后设置的数据中心ID");
    }

    @Test
    @DisplayName("配置应有合理的toString")
    void testToString() {
        SpeedboatConfig config = SpeedboatConfig.builder()
            .port(22222)
            .datacenterId("dc1")
            .build();
        
        String str = config.toString();
        assertNotNull(str, "toString不应返回null");
        assertTrue(str.contains("22222"), "toString应包含端口");
        assertTrue(str.contains("dc1"), "toString应包含数据中心ID");
    }

    @Test
    @DisplayName("配置应有合理的equals和hashCode")
    void testEqualsAndHashCode() {
        SpeedboatConfig config1 = SpeedboatConfig.builder()
            .port(22222)
            .datacenterId("dc1")
            .build();
        
        SpeedboatConfig config2 = SpeedboatConfig.builder()
            .port(22222)
            .datacenterId("dc1")
            .build();
        
        SpeedboatConfig config3 = SpeedboatConfig.builder()
            .port(33333)
            .datacenterId("dc1")
            .build();
        
        assertEquals(config1, config2, "相同配置应相等");
        assertEquals(config1.hashCode(), config2.hashCode(), "相同配置hashCode应相同");
        assertNotEquals(config1, config3, "不同配置应不相等");
    }

    @Test
    @DisplayName("null值的策略字段应正常处理")
    void testNullStrategyFields() {
        SpeedboatConfig config = SpeedboatConfig.builder()
            .port(22222)
            .datacenterId("dc1")
            .build();
        
        assertNull(config.getVoteWeightStrategy(), "未设置的投票权重策略应为null");
        assertNull(config.getGroupStrategy(), "未设置的分组策略应为null");
    }

    @Test
    @DisplayName("配置应支持null数据中心ID")
    void testNullDatacenterId() {
        SpeedboatConfig config = SpeedboatConfig.builder()
            .port(22222)
            .build();
        
        assertNull(config.getDatacenterId(), "未设置的数据中心ID应为null");
    }

    @Test
    @DisplayName("from方法传入null应抛出异常")
    void testFromWithNullShouldThrow() {
        assertThrows(NullPointerException.class, () -> {
            SpeedboatConfig.from(null);
        });
    }

    @Test
    @DisplayName("equals - 自引用返回true")
    void testEqualsSameInstance() {
        SpeedboatConfig config = SpeedboatConfig.builder()
            .port(22222)
            .datacenterId("dc1")
            .build();
        
        assertEquals(config, config);
    }

    @Test
    @DisplayName("equals - null返回false")
    void testEqualsNull() {
        SpeedboatConfig config = SpeedboatConfig.builder()
            .port(22222)
            .datacenterId("dc1")
            .build();
        
        assertNotEquals(null, config);
    }

    @Test
    @DisplayName("equals - 不同类型返回false")
    void testEqualsDifferentType() {
        SpeedboatConfig config = SpeedboatConfig.builder()
            .port(22222)
            .datacenterId("dc1")
            .build();
        
        assertNotEquals("string", config);
        assertNotEquals(123, config);
    }

    @Test
    @DisplayName("equals - 不同端口返回false")
    void testEqualsDifferentPort() {
        SpeedboatConfig config1 = SpeedboatConfig.builder()
            .port(22222)
            .datacenterId("dc1")
            .build();
        
        SpeedboatConfig config2 = SpeedboatConfig.builder()
            .port(33333)
            .datacenterId("dc1")
            .build();
        
        assertNotEquals(config1, config2);
    }

    @Test
    @DisplayName("equals - 不同数据中心返回false")
    void testEqualsDifferentDatacenter() {
        SpeedboatConfig config1 = SpeedboatConfig.builder()
            .port(22222)
            .datacenterId("dc1")
            .build();
        
        SpeedboatConfig config2 = SpeedboatConfig.builder()
            .port(22222)
            .datacenterId("dc2")
            .build();
        
        assertNotEquals(config1, config2);
    }

    @Test
    @DisplayName("equals - 不同策略返回false")
    void testEqualsDifferentStrategy() {
        SpeedboatConfig config1 = SpeedboatConfig.builder()
            .port(22222)
            .voteWeightStrategy(new DefaultVoteWeightStrategy())
            .build();
        
        SpeedboatConfig config2 = SpeedboatConfig.builder()
            .port(22222)
            .voteWeightStrategy(new DefaultVoteWeightStrategy())
            .build();
        
        assertNotEquals(config1, config2);
    }
}