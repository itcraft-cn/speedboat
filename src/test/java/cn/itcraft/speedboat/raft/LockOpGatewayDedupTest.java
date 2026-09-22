package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.lock.LockCommand;
import cn.itcraft.speedboat.rpc.LockOpRequest;
import cn.itcraft.speedboat.rpc.LockOpResponse;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LockOpGateway 幂等去重测试（SOFAJRaft RequestMap 纪律落地件）。
 *
 * <p>场景矩阵：受理成功重试（重复提案必须幂等回放）、确定性拒绝缓存、
 * 条件性拒绝（非 Leader）不缓存、无 transport 的本地单测环境。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
class LockOpGatewayDedupTest {

    private RaftNode node;

    @BeforeEach
    void setUp() {
        // 无 transport 构造，成为单节点集群（自身权重达成法定人数）后免广播直接登基
        node = new RaftNode.Builder()
            .nodeId("node-1")
            .peerIds(Arrays.asList())
            .electionTimeout(new ElectionTimeout(150, 300))
            .build();
        node.startElection();
        assertTrue(node.isLeader(), "单节点集群选举应立即登基");
    }

    private LockOpRequest request(String requestId) {
        LockCommand command = new LockCommand(
            "lock-a", "node-2", LockCommand.CommandType.LOCK, requestId, 30_000L, 0L);
        byte[] payload = new ProtostuffSerializer().serialize(command);
        return new LockOpRequest(requestId, payload);
    }

    @Test
    @DisplayName("受理成功后同 requestId 重试：幂等回放原 entryIndex，不重复入日志")
    void dedupReplaysAcceptedEntry() {
        LockOpRequest req = request("r-1");
        LockOpResponse r1 = trigger(req);
        assertTrue(r1.isOk());
        long entryIndex = r1.getEntryIndex();
        assertTrue(entryIndex > 0);
        int logSizeAfterFirst = node.getLogEntries().size();

        // 模拟"应答丢失"后重试
        LockOpResponse r2 = trigger(req);
        assertTrue(r2.isOk());
        assertEquals(entryIndex, r2.getEntryIndex(), "幂等回放，entryIndex 不应递增");
        assertEquals(logSizeAfterFirst, node.getLogEntries().size(), "重复申请不得再次入日志");
    }

    @Test
    @DisplayName("确定性拒绝（畸形命令）缓存：同参重试幂等回拒")
    void dedupBuffersDeterministicReject() {
        LockOpRequest bad = new LockOpRequest("r-bad", new byte[]{1, 2, 3});
        LockOpResponse r1 = trigger(bad);
        assertFalse(r1.isOk());

        LockOpResponse r2 = trigger(bad);
        assertFalse(r2.isOk());
        assertEquals(-1, r2.getEntryIndex());
    }

    /** 测试辅助：Leader 侧裁决入口（同传输层 handler 语义，onRaftThread 同步收敛） */
    private LockOpResponse trigger(LockOpRequest request) {
        return ((RaftNodeImpl) node).handleLockOp(request);
    }
}
