package cn.itcraft.speedboat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * 机房标识解析测试（跨机房级联公共前提 P4）。
 *
 * <p><b>为什么重要：</b>跨机房模式下两个机房使用的是<b>同一份配置文件</b>。
 * 若 {@code datacenter=} 配置值被原样返回，主机房与备机房的所有节点会解析出
 * <b>同一个</b>机房 ID，导致一切按机房归属判权的策略（机房优先级权重、同城提权等）
 * 完全失效——每个节点都会认为对端与自己同机房。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
class SpeedboatDatacenterIdTest {

    // ==================== 单机房模式（向后兼容） ====================

    @Test
    @DisplayName("单机房 - datacenter= 配置值原样生效（既有部署不受影响）")
    void singleDatacenterKeepsConfiguredId() {
        assertEquals("hangzhou001",
            Speedboat.resolveDatacenterId("hangzhou001", false, 0));
    }

    @Test
    @DisplayName("单机房 - 未配置时回退为 dc-<索引>（恒为 dc-0）")
    void singleDatacenterFallsBackToIndexedId() {
        assertEquals("dc-0", Speedboat.resolveDatacenterId(null, false, 0));
    }

    // ==================== 跨机房模式（机房 ID 必须唯一） ====================

    @Test
    @DisplayName("跨机房 - 未配置时两个机房各自获得唯一 ID")
    void crossDatacenterGeneratesUniqueIdsWithoutConfig() {
        String primary = Speedboat.resolveDatacenterId(null, true, 0);
        String backup = Speedboat.resolveDatacenterId(null, true, 1);

        assertEquals("dc-0", primary);
        assertEquals("dc-1", backup);
        assertNotEquals(primary, backup, "两个机房不得解析出同一个机房 ID");
    }

    @Test
    @DisplayName("跨机房 - 配置 datacenter= 后仍按索引区分（关键缺陷回归）")
    void crossDatacenterKeepsConfiguredIdUniquePerIndex() {
        // 两台机器读到的是同一份配置，configured 相同，仅索引不同
        String primary = Speedboat.resolveDatacenterId("hangzhou001", true, 0);
        String backup = Speedboat.resolveDatacenterId("hangzhou001", true, 1);

        assertEquals("hangzhou001-0", primary);
        assertEquals("hangzhou001-1", backup);
        assertNotEquals(primary, backup,
            "跨机房模式下两机房不得解析出同一个机房 ID——否则机房优先级权重等策略完全失效");
    }

    @Test
    @DisplayName("跨机房 - 索引位数不同时依然区分")
    void crossDatacenterDistinguishesMultiDigitIndex() {
        assertNotEquals(
            Speedboat.resolveDatacenterId("dc", true, 1),
            Speedboat.resolveDatacenterId("dc", true, 10));
    }

    // ==================== 两模式对照 ====================

    @Test
    @DisplayName("同一配置在单机房与跨机房模式下语义不同（单机房保持原样）")
    void configuredIdBehaviourDiffersByMode() {
        assertEquals("hangzhou001", Speedboat.resolveDatacenterId("hangzhou001", false, 0));
        assertEquals("hangzhou001-0", Speedboat.resolveDatacenterId("hangzhou001", true, 0));
    }
}
