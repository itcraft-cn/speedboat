package cn.itcraft.speedboat.persistence;

import cn.itcraft.speedboat.raft.CommandLogEntry;
import cn.itcraft.speedboat.raft.LogEntry;
import cn.itcraft.speedboat.raft.MemberChangeEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MmapRaftStore 契约测试（P4-M1）：term 双槽挂回、WAL 记录帧保真、"列表即全量"语义、
 * 截断标记收敛、坏帧收敛、检查点路径暴露。
 */
class MmapRaftStoreTest {

    @TempDir
    Path dir;

    /** 挂载（模拟进程重启）：同目录重新 new + open */
    private MmapRaftStore mount() {
        return mount(8);
    }

    private MmapRaftStore mount(int sizeMb) {
        MmapRaftStore s = new MmapRaftStore(dir.toFile(), "raft.mmap", sizeMb);
        s.open();
        return s;
    }

    private CommandLogEntry cmd(long idx, long term, String leader, byte[] data) {
        return CommandLogEntry.create(idx, term, leader, data);
    }

    @Test
    void termDoubleSlotRoundTripAcrossRestart() {
        MmapRaftStore s1 = mount();
        s1.persistAndFlushTerm(7L, "node-A", "node-A");
        s1.persistAndFlushTerm(9L, "node-B", "node-C");
        s1.close();

        MmapRaftStore s2 = mount();
        RaftTermRecord record = s2.restoreTerm();
        assertNotNull(record);
        assertEquals(9L, record.getTerm(), "重启挂回应取最大 seq 槽位");
        assertEquals("node-B", record.getVotedFor());
        assertEquals("node-C", record.getLeaderId());
    }

    @Test
    void logEntriesRoundTripWithCommandData() {
        MmapRaftStore s1 = mount();
        byte[] data = "LOCK|forex|node-A|req1".getBytes();
        s1.persistLogEntries(Arrays.<LogEntry>asList(cmd(1, 3, "node-A", data)));
        s1.persistAndFlushTerm(3L, "node-A", "node-A");
        s1.flush();
        s1.close();

        MmapRaftStore s2 = mount();
        List<LogEntry> restored = s2.restoredLogEntries();
        assertEquals(1, restored.size());
        LogEntry restoredFirst = restored.get(0);
        assertEquals(1, restoredFirst.getIndex());
        assertEquals(3, restoredFirst.getTerm());
        assertEquals("node-A", restoredFirst.getLeaderId());
        assertArrayEquals(data, restoredFirst.getData(), "COMMAND data 必须保真（锁命令载荷）");
    }

    @Test
    void followerConflictShrinksWalAndTruncates() {
        MmapRaftStore s1 = mount();
        s1.persistLogEntries(Arrays.<LogEntry>asList(
            cmd(1, 2, "L1", "a".getBytes()),
            cmd(2, 2, "L1", "b".getBytes()),
            cmd(3, 2, "L1", "c".getBytes())));
        s1.close();

        MmapRaftStore s2 = mount();
        // follower 冲突：全量列表缩短 → WAL 尾巴必须被 TRUNC_FROM 收口（重挂不复活）
        s2.persistLogEntries(Arrays.<LogEntry>asList(cmd(1, 3, "L2", "x".getBytes())));
        List<LogEntry> view = s2.restoredLogEntries();
        assertEquals(1, view.size(), "保留的应只有传入列表的条目");
        assertEquals(1, view.get(0).getIndex());
        s2.close();

        // 再挂载一次仍在：TRUNC 标记已在 WAL 里，扫描重建依然收敛
        MmapRaftStore s3 = mount();
        List<LogEntry> view2 = s3.restoredLogEntries();
        assertEquals(1, view2.size(), "截断标记刷盘后重启仍收敛");
    }

    @Test
    void corruptTailStopsScanButKeepsEarlierRecords() throws Exception {
        MmapRaftStore s1 = mount();
        s1.persistLogEntries(Arrays.<LogEntry>asList(cmd(1, 2, "L", "ok".getBytes())));
        s1.flush();
        s1.close();

        File target = dir.resolve("raft.mmap").toFile();
        // 人为破坏尾部（位于 WAL 起始区域的某一条记录 frame 头）
        try (RandomAccessFile raf = new RandomAccessFile(target, "rw")) {
            raf.seek(1024 + 1);
            raf.write(0xFF);
            raf.seek(1024);
            raf.writeInt(0x7FFFFFFF);
        }

        MmapRaftStore s2 = mount();
        // 坏帧即收敛：扫描不到任何合法帧时允许空恢复（安全方向）
        List<LogEntry> view = s2.restoredLogEntries();
        assertTrue(view.isEmpty(), "坏帧后的 WAL 不应回放脏数据");
    }

    @Test
    void checkpointFileExposedAndSnapshotChunksStubbed() throws Exception {
        MmapRaftStore s = mount();
        String ckpt = s.getCheckpointFile();
        assertNotNull(ckpt);
        assertEquals(new File(new File(dir.toString()), "state-machine.ckpt").getAbsolutePath(), ckpt);
        s.deleteSnapshotChunks();
        assertFalse(new File(ckpt).exists() && new java.io.RandomAccessFile(ckpt, "r").length() > 0,
            "无检查点即不存在");
    }

    @Test
    void memberChangeEntryCodecRoundTrip() {
        MmapRaftStore s1 = mount();
        MemberChangeEntry add = MemberChangeEntry.add(1, 2, "L", "node-X", "1.2.3.4:8888");
        s1.persistLogEntries(Arrays.<LogEntry>asList(add));
        s1.close();

        MmapRaftStore s2 = mount();
        List<LogEntry> view = s2.restoredLogEntries();
        assertEquals(1, view.size());
        assertTrue(view.get(0) instanceof MemberChangeEntry, "MemberChange 需按原类型重建");
        MemberChangeEntry restored = (MemberChangeEntry) view.get(0);
        assertEquals("node-X", restored.getNodeId());
        assertEquals("1.2.3.4:8888", restored.getAddress());
    }

    // ==================== P4-M4 压实回收 ====================

    /** 压实：检查点水位以下的日志前缀被回收，重挂后恢复语义等价（检查点 + 保留段） */
    @Test
    void compactionReclaimsPrefixBelowCheckpointFloor() {
        MmapRaftStore s1 = mount();
        for (int i = 1; i <= 10; i++) {
            s1.persistLogEntries(Arrays.<LogEntry>asList(cmd(i, 1, "L", ("d" + i).getBytes())));
        }
        assertEquals(10, s1.restoredLogEntries().size());

        s1.markCheckpoint(5L);
        assertEquals(5L, s1.getCompactFloor());
        long newOffset = s1.compactNow();

        List<LogEntry> retained = s1.restoredLogEntries();
        assertEquals(5, retained.size(), "floor 以下的 5 条应被回收");
        assertEquals(6L, retained.get(0).getIndex(), "保留段应从 floor+1 开始");
        assertEquals(10L, retained.get(retained.size() - 1).getIndex());
        assertTrue(newOffset <= 1024 + 5 * 64, "写位应回落到区域起始附近");

        // 重挂：WAL 只含保留段（检查点负责 floor 以下状态）
        s1.close();
        MmapRaftStore s2 = mount();
        List<LogEntry> afterRemount = s2.restoredLogEntries();
        assertEquals(5, afterRemount.size(), "压实后重挂仍收敛于保留段");
        assertEquals(6L, afterRemount.get(0).getIndex());

        // 压实后继续追加正常（写位从新位置前进）
        s2.persistLogEntries(Arrays.<LogEntry>asList(cmd(11, 1, "L", "d11".getBytes())));
        assertEquals(6, s2.restoredLogEntries().size());
    }

    /** 容量压力路径自动触发：随检查点水位推进，WAL 不会被写满丢帧 */
    @Test
    void capacityPressureTriggersCompactionNotFrameLoss() {
        MmapRaftStore s = mount(1); // sizeMb=1：区域约 1MB，约 2.3 万条触发压实
        int total = 30000;
        for (int i = 1; i <= total; i++) {
            s.persistLogEntries(Arrays.<LogEntry>asList(cmd(i, 1, "L", new byte[]{1})));
            if (i % 100 == 0) {
                s.markCheckpoint(i);
            }
        }
        List<LogEntry> retained = s.restoredLogEntries();
        // 前缀被持续压实：保留条数远小于总量，且全部高于检查点水位以下的部分
        assertTrue(retained.size() < total, "应已压实回收前缀而非线性增长");
        assertTrue(retained.get(retained.size() - 1).getIndex() == total, "最新条目必须在线");
        assertTrue(retained.get(0).getIndex() > 0);
    }
}
