package cn.itcraft.speedboat.lock;

import cn.itcraft.speedboat.raft.CommandLogEntry;
import cn.itcraft.speedboat.raft.LogEntry;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LockStateMachine 检查点恢复测试（P4-M2）：snapshot→restore 后 lockTable/epoch/
 * lastAppliedIndex 确定性一致——这是"重启副本 epoch 边界根治"的最后地基。
 */
class LockStateMachineCheckpointTest {

    @TempDir
    Path dir;

    private final ProtostuffSerializer serializer = new ProtostuffSerializer();

    private LogEntry cmd(long idx, LockCommand c) {
        return CommandLogEntry.create(idx, 1, "leader", serializer.serialize(c));
    }

    @Test
    void snapshotRestoreRoundTripKeepsEpochAndHolder() {
        LockStateMachine sm1 = new LockStateMachine();
        sm1.apply(CommandLogEntry.create(1, 1, "leader",
            serializer.serialize(new LockCommand("forex", "node-A", LockCommand.CommandType.LOCK))));
        sm1.apply(CommandLogEntry.create(2, 1, "leader",
            serializer.serialize(new LockCommand("metals", "node-B", LockCommand.CommandType.LOCK))));
        sm1.apply(CommandLogEntry.create(3, 1, "leader",
            serializer.serialize(new LockCommand("commodity", "node-C", LockCommand.CommandType.LOCK))));

        long epochForex = sm1.getLockEntry("forex").getEpoch();
        String holderForex = sm1.getLockEntry("forex").getNodeId();
        long epochMetals = sm1.getLockEntry("metals").getEpoch();

        String ckpt = dir.resolve("state-machine.ckpt").toAbsolutePath().toString();
        sm1.snapshot(ckpt);

        LockStateMachine sm2 = new LockStateMachine();
        assertEquals(0L, sm2.getLastAppliedIndex(), "新状态机 starting blank");
        sm2.restore(ckpt);

        assertEquals(3L, sm2.getLastAppliedIndex(), "lastAppliedIndex 应随快照恢复（3 条 apply）");
        assertEquals("node-A", sm2.getLockEntry("forex").getNodeId(), "持有者跨重启保真");
        assertEquals(epochForex, sm2.getLockEntry("forex").getEpoch(), "epoch fencing 跨重启保真");
        assertEquals("node-B", sm2.getLockEntry("metals").getNodeId());
        assertEquals(epochMetals, sm2.getLockEntry("metals").getEpoch());
        assertEquals("node-C", sm2.getLockEntry("commodity").getNodeId());
    }

    @Test
    void missingSnapshotFallsBackToBlank() {
        LockStateMachine sm = new LockStateMachine();
        sm.restore(dir.resolve("state-machine.ckpt").toAbsolutePath().toString());
        assertNull(sm.getLockEntry("forex"), "无检查点文件应空白启动");
    }

    @Test
    void corruptSnapshotFallsBackToBlank() throws Exception {
        LockStateMachine sm1 = new LockStateMachine();
        sm1.apply(CommandLogEntry.create(1, 1, "leader",
            serializer.serialize(new LockCommand("forex", "node-A", LockCommand.CommandType.LOCK))));
        String ckpt = dir.resolve("state-machine.ckpt").toAbsolutePath().toString();
        sm1.snapshot(ckpt);

        // 撕裂 CRC：随机涂抹 payload 中一字节
        Path target = java.nio.file.Path.of(ckpt);
        byte[] bytes = Files.readAllBytes(target);
        new Random(42).nextBytes(new byte[0]);
        bytes[bytes.length - 5] = (byte) (bytes[bytes.length - 1 - 4] ^ 0x3F);
        Files.write(target, bytes);

        LockStateMachine sm2 = new LockStateMachine();
        sm2.restore(ckpt);
        assertNull(sm2.getLockEntry("forex"), "CRC 不合法的检查点应退回空白（宁可无主）");
    }
}
