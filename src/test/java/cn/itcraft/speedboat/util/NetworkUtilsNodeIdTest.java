package cn.itcraft.speedboat.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * NetworkUtils nodeId 确定性身份测试。
 *
 * <p>背景：Raft 的安全属性（votedFor/leaderId/成员身份）依赖 nodeId 的
 * 唯一性与稳定性。历史上 nodeId 包含随机后缀且 peer 侧使用另一套生成规则，
 * 导致同一节点在不同进程内有两个名字。本测试锁定确定性生成契约。</p>
 */
class NetworkUtilsNodeIdTest {

    @Test
    @DisplayName("地址生成的 nodeId 必须确定性：多次调用结果一致")
    void generateNodeId_isDeterministic() {
        String a = NetworkUtils.generateNodeId("192.168.193.176:9001");
        String b = NetworkUtils.generateNodeId("192.168.193.176:9001");
        assertEquals(a, b, "同一地址多次生成必须得到相同 nodeId");
    }

    @Test
    @DisplayName("自侧节点（ip+port 直接拼接）与 peer 侧（地址解析）身份一致")
    void selfAndPeerUseSameIdentity() {
        String peerNodeId = NetworkUtils.generateNodeId("192.168.193.176:9001");
        // Speedboot 自侧使用 localIp + ":" + localPort 拼接后走同一入口
        String selfNodeId = NetworkUtils.generateNodeId("192.168.193.176" + ":" + 9001);
        assertEquals(peerNodeId, selfNodeId, "同一 ip:port 在两种构造方式下必须有相同身份");
        assertEquals("node-192.168.193.176-9001", peerNodeId);
    }

    @Test
    @DisplayName("不同地址生成不同身份，同 IP 不同端口不碰撞")
    void differentAddressDifferentIdentity() {
        assertNotEquals(
            NetworkUtils.generateNodeId("192.168.193.176:9001"),
            NetworkUtils.generateNodeId("192.168.193.176:9002"),
            "同 IP 不同端口应为不同节点");

        assertNotEquals(
            NetworkUtils.generateNodeId("192.168.193.176:9001"),
            NetworkUtils.generateNodeId("192.168.193.174:9001"),
            "同端口不同 IP 应为不同身份");
    }

    @Test
    @DisplayName("hostname 变体生成不再包含随机后缀（跨重启稳定）")
    void hostnameVariantHasNoRandomSuffix() {
        String a = NetworkUtils.generateNodeId("server01", "192.168.10.1");
        String b = NetworkUtils.generateNodeId("server01", "192.168.10.1");
        assertEquals(a, b);
        assertEquals("server01-192.168.10.1", a);
    }
}
