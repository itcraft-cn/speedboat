package cn.itcraft.speedboat;

import cn.itcraft.speedboat.strategy.consistency.ApConsistencyPolicy;
import cn.itcraft.speedboat.strategy.consistency.ConsistencyPolicy;
import cn.itcraft.speedboat.strategy.consistency.CpConsistencyPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 人工切主模式闸门测试（阶段五 AP 语义）。
 *
 * <p><b>被守护的语义：</b>人工选主（{@code promoteDatacenter} /
 * {@code restoreDefaultPriorities}）<b>仅对 CP 生效</b>。AP 模式下该调用被<b>忽略</b>——
 * 门面只落一条 {@code op=IGNORED} 审计日志，不改状态、不抛异常、不返回"错误"。
 * 若此处误判为 CP 放行，AP 分区窗口内的人工切主会与降级接管叠加，
 * 使"允许短暂双主"的可观测口径彻底失控。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
class SpeedboatManualSwitchGateTest {

    @Test
    @DisplayName("AP 模式：人工切主被忽略")
    void apModeIgnoresManualSwitch() {
        ConsistencyPolicy ap = new ApConsistencyPolicy(
            ApConsistencyPolicy.DEFAULT_DEGRADED_TIMEOUT_MS);

        assertTrue(Speedboat.manualSwitchIgnored(ap),
            "AP 模式下人工切主必须被忽略（只记日志，不执行）");
    }

    @Test
    @DisplayName("CP 模式（缺省）：人工切主照常放行——闸门不误伤运维兜底")
    void cpModeAllowsManualSwitch() {
        assertFalse(Speedboat.manualSwitchIgnored(CpConsistencyPolicy.getInstance()),
            "CP 是人工切主的唯一生效模式，不得被闸门拦截");
    }

    @Test
    @DisplayName("策略未注入（null）：按缺省 CP 放行，与 NodeContext 兜底一致")
    void nullPolicyFallsBackToCp() {
        assertFalse(Speedboat.manualSwitchIgnored(null),
            "提供方未实现 getConsistencyPolicy 时应回落 CP，而非静默忽略运维调用");
    }
}
