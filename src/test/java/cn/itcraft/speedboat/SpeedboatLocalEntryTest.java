package cn.itcraft.speedboat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 单机多进程（同 IP 多端口）身份解析测试。
 *
 * <p><b>被守护的缺陷：</b>节点身份原本仅按 IP 匹配且取首个条目——同机多进程
 * （如阶段六备机房三进程共享 172.22.133.1:21001/21002/21003）会全部命中首个条目，
 * 三个进程声称同一 nodeId 且争绑同一端口，集群身份直接撞车。</p>
 *
 * <p>修复语义（{@code speedboat.local.port} 端口钉定）：</p>
 * <ol>
 *   <li>钉定模式：IP 与端口必须同时精确命中，否则抛错（宁可拒绝启动也不静默错认身份）；</li>
 *   <li>历史模式（未钉定）：按 IP 取首个条目，与引入前逐条等价，既有部署零回归。</li>
 * </ol>
 *
 * @author speedboat
 * @since 1.2.0
 */
class SpeedboatLocalEntryTest {

    /** 阶段六拓扑：dc-0 三虚机（IP 各异），dc-1 同 IP 三进程（端口相异） */
    private static List<List<String>> partitionTopology() {
        List<List<String>> nodes = new ArrayList<List<String>>();
        nodes.add(Arrays.asList(
            "172.22.133.251:21001", "172.22.133.252:21001", "172.22.133.253:21001"));
        nodes.add(Arrays.asList(
            "172.22.133.1:21001", "172.22.133.1:21002", "172.22.133.1:21003"));
        return nodes;
    }

    @Test
    @DisplayName("端口钉定：同 IP 三条目各自精确命中自己的端口与机房")
    void portPinnedResolvesEachSameIpEntryDistinctly() {
        List<List<String>> nodes = partitionTopology();

        Speedboat.NodeMatchResult e0 =
            Speedboat.resolveLocalEntry(nodes, "172.22.133.1", 21001);
        Speedboat.NodeMatchResult e1 =
            Speedboat.resolveLocalEntry(nodes, "172.22.133.1", 21002);
        Speedboat.NodeMatchResult e2 =
            Speedboat.resolveLocalEntry(nodes, "172.22.133.1", 21003);

        assertEquals(1, e0.datacenterIndex, "同 IP 进程应同属 dc-1");
        assertEquals(21001, e0.port);
        assertEquals(21002, e1.port, "不同端口必须解析为不同身份");
        assertEquals(21003, e2.port, "不同端口必须解析为不同身份");
    }

    @Test
    @DisplayName("端口钉定：虚机 IP 命中 dc-0 且端口不变")
    void portPinnedResolvesVmEntry() {
        Speedboat.NodeMatchResult e =
            Speedboat.resolveLocalEntry(partitionTopology(), "172.22.133.252", 21001);
        assertEquals(0, e.datacenterIndex, "虚机应在 dc-0");
        assertEquals(21001, e.port);
    }

    @Test
    @DisplayName("端口钉定：配置中不存在该 ip:port 时抛错（不静默错认身份）")
    void portPinnedThrowsWhenEntryMissing() {
        assertThrows(IllegalArgumentException.class, () ->
            Speedboat.resolveLocalEntry(partitionTopology(), "172.22.133.1", 29999));
        assertThrows(IllegalArgumentException.class, () ->
            Speedboat.resolveLocalEntry(partitionTopology(), "10.0.0.9", 21001));
    }

    @Test
    @DisplayName("历史模式：未钉定端口时按 IP 取首个条目（既有行为逐条等价）")
    void legacyModeMatchesFirstIpEntry() {
        // 单机房扁平三机（IP 各异，历史形态）
        List<List<String>> flat = new ArrayList<List<String>>();
        flat.add(Arrays.asList("127.0.0.1:31001", "127.0.0.2:31002", "127.0.0.3:31003"));

        Speedboat.NodeMatchResult e = Speedboat.resolveLocalEntry(flat, "127.0.0.2", null);
        assertEquals(0, e.datacenterIndex);
        assertEquals(31002, e.port, "历史模式按 IP 命中正确端口");

        // 同 IP 多条目时历史模式取首个（保持引入前的确定性语义）
        Speedboat.NodeMatchResult first =
            Speedboat.resolveLocalEntry(partitionTopology(), "172.22.133.1", null);
        assertEquals(1, first.datacenterIndex);
        assertEquals(21001, first.port, "历史模式取首个条目，与引入前一致");
    }

    @Test
    @DisplayName("历史模式：IP 完全不在配置中时抛错（原语义不变）")
    void legacyModeThrowsWhenIpMissing() {
        assertThrows(IllegalArgumentException.class, () ->
            Speedboat.resolveLocalEntry(partitionTopology(), "10.9.9.9", null));
    }

    @Test
    @DisplayName("端口钉定属性解析：缺省 null、非法值回退 null（走历史模式）")
    void portOverrideParsing() {
        System.clearProperty("speedboat.local.port");
        assertEquals(null, Speedboat.localPortOverride(), "未设置时应为 null");

        System.setProperty("speedboat.local.port", "21002");
        try {
            assertEquals(Integer.valueOf(21002), Speedboat.localPortOverride());

            System.setProperty("speedboat.local.port", "not-a-number");
            assertEquals(null, Speedboat.localPortOverride(), "非法值应回退 null 走历史模式");
        } finally {
            System.clearProperty("speedboat.local.port");
        }
    }
}
