package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.persistence.PriorityStore;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityVoteWeightStrategy;
import cn.itcraft.speedboat.strategy.voteweight.PriorityCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 阶段五人工升级 API 单测：条目编解码、优先级表收敛、独立持久化、
 * 以及 RaftNodeImpl 的提升/回退编排（换表 → term 跃升 → 登基 → 复制 → 持久化）。
 */
class PriorityPromoteTest {

    // ==================== PriorityChangeEntry 编解码 ====================

    @Test
    @DisplayName("PRIORITY_CHANGE 条目编码后按 data 字节重建应等价")
    void priorityChangeEntryRoundTrip() {
        Map<String, Integer> weights = new LinkedHashMap<>();
        weights.put("dc-a", 4);
        weights.put("dc-b", 2);

        PriorityChangeEntry entry = PriorityChangeEntry.create(
            7, 11, "leader-1", 3, weights, PriorityChangeEntry.SOURCE_PROMOTED,
            "dc-a", "alice", "host dc down");

        assertEquals(LogEntry.EntryType.PRIORITY_CHANGE, entry.getEntryType());
        assertEquals(3, entry.epoch());
        assertEquals("dc-a", entry.targetDatacenter());
        assertEquals("alice", entry.operator());
        assertNotNull(entry.getData());

        PriorityChangeEntry rebuilt = PriorityChangeEntry.fromData(
            entry.getIndex(), entry.getTerm(), entry.getLeaderId(), entry.getData());

        assertEquals(entry.epoch(), rebuilt.epoch());
        assertEquals(entry.source(), rebuilt.source());
        assertEquals(entry.targetDatacenter(), rebuilt.targetDatacenter());
        assertEquals(entry.operator(), rebuilt.operator());
        assertEquals(entry.reason(), rebuilt.reason());
        assertEquals(entry.weights(), rebuilt.weights());
    }

    @Test
    @DisplayName("操作者/原因含分隔符时转义后仍可无损重建")
    void priorityChangeEntryEscapesSeparators() {
        Map<String, Integer> weights = Collections.singletonMap("dc-a", 5);
        String operator = "op;with=commas,and\\slashes";
        String reason = "reason;with,special=chars";

        PriorityChangeEntry entry = PriorityChangeEntry.create(
            1, 1, "l", 1, weights, PriorityChangeEntry.SOURCE_PROMOTED, null, operator, reason);
        PriorityChangeEntry rebuilt = PriorityChangeEntry.fromData(
            entry.getIndex(), entry.getTerm(), entry.getLeaderId(), entry.getData());

        assertEquals(operator, rebuilt.operator());
        assertEquals(reason, rebuilt.reason());
        assertNull(rebuilt.targetDatacenter());
    }

    @Test
    @DisplayName("回退条目（source=CONFIG、target 为 null）编码解码一致")
    void priorityChangeEntryRestoreVariant() {
        Map<String, Integer> weights = new LinkedHashMap<>();
        weights.put("dc-a", 2);
        weights.put("dc-b", 1);

        PriorityChangeEntry entry = PriorityChangeEntry.create(
            9, 20, "leader-1", 4, weights, PriorityChangeEntry.SOURCE_CONFIG, null, "bob", "hand back");

        PriorityChangeEntry rebuilt = PriorityChangeEntry.fromData(
            entry.getIndex(), entry.getTerm(), entry.getLeaderId(), entry.getData());

        assertEquals(PriorityChangeEntry.SOURCE_CONFIG, rebuilt.source());
        assertNull(rebuilt.targetDatacenter());
        assertEquals(weights, rebuilt.weights());
    }

    // ==================== PriorityCodec 线格式 ====================

    @Test
    @DisplayName("PriorityCodec 编解码往返一致")
    void priorityCodecRoundTrip() {
        Map<String, Integer> weights = new LinkedHashMap<>();
        weights.put("hz", 4);
        weights.put("sh", 1);

        String encoded = PriorityCodec.encode(weights);
        assertEquals("hz=4,sh=1", encoded);
        assertEquals(weights, PriorityCodec.decode(encoded));
    }

    @Test
    @DisplayName("PriorityCodec 空表/坏项容错")
    void priorityCodecTolerance() {
        assertEquals("", PriorityCodec.encode(null));
        assertEquals("", PriorityCodec.encode(Collections.<String, Integer>emptyMap()));
        assertTrue(PriorityCodec.decode(null).isEmpty());
        assertTrue(PriorityCodec.decode("").isEmpty());
        // 坏项（非数字权重/无等号）应被跳过而非抛异常
        Map<String, Integer> decoded = PriorityCodec.decode("a=1,bad,b=xx,c=3");
        assertEquals(1, decoded.get("a"));
        assertEquals(3, decoded.get("c"));
        assertFalse(decoded.containsKey("b"));
    }

    // ==================== DatacenterPriorityTable 收敛 ====================

    @Test
    @DisplayName("convergeIfNewer 只升不降：更高 epoch 采纳、更低/相等忽略")
    void convergeOnlyUpgrades() {
        DatacenterPriorityTable table = new DatacenterPriorityTable(
            Collections.singletonMap("dc-a", 2), DatacenterPriorityTable.Source.CONFIG);

        // 更高 epoch：采纳
        assertTrue(table.convergeIfNewer(5, Collections.singletonMap("dc-a", 9),
            DatacenterPriorityTable.Source.PROMOTED));
        assertEquals(5, table.current().epoch());
        assertEquals(Integer.valueOf(9), table.current().weights().get("dc-a"));

        // 更低 epoch：忽略（旧快照不回灌）
        assertFalse(table.convergeIfNewer(3, Collections.singletonMap("dc-a", 1),
            DatacenterPriorityTable.Source.PROMOTED));
        assertEquals(5, table.current().epoch());

        // 相等 epoch：忽略
        assertFalse(table.convergeIfNewer(5, Collections.singletonMap("dc-a", 7),
            DatacenterPriorityTable.Source.PROMOTED));
        assertEquals(Integer.valueOf(9), table.current().weights().get("dc-a"));
    }

    @Test
    @DisplayName("configWeights 记录配置派生初值，供回退目标")
    void configWeightsPreserved() {
        Map<String, Integer> cfg = new LinkedHashMap<>();
        cfg.put("dc-a", 2);
        cfg.put("dc-b", 1);
        DatacenterPriorityTable table = new DatacenterPriorityTable(cfg, DatacenterPriorityTable.Source.CONFIG);

        table.replace(1, Collections.singletonMap("dc-a", 10), DatacenterPriorityTable.Source.PROMOTED);

        assertEquals(cfg, table.configWeights());
        assertEquals(Integer.valueOf(10), table.current().weights().get("dc-a"));
    }

    // ==================== PriorityStore 持久化 ====================

    @Test
    @DisplayName("PriorityStore 落盘后 load 还原同一快照")
    void priorityStoreRoundTrip(@TempDir File dir) {
        PriorityStore store = new PriorityStore(dir);
        assertNull(store.load(), "无文件时应返回 null");

        Map<String, Integer> weights = new LinkedHashMap<>();
        weights.put("dc-a", 4);
        weights.put("dc-b", 2);
        DatacenterPriorityTable.Snapshot saved = new DatacenterPriorityTable(
            weights, DatacenterPriorityTable.Source.PROMOTED).replace(6, weights,
            DatacenterPriorityTable.Source.PROMOTED);
        store.save(saved);

        DatacenterPriorityTable.Snapshot loaded = store.load();
        assertNotNull(loaded);
        assertEquals(6, loaded.epoch());
        assertEquals(DatacenterPriorityTable.Source.PROMOTED, loaded.source());
        assertEquals(weights, loaded.weights());
    }

    @Test
    @DisplayName("PriorityStore 损坏文件 load 返回 null（回退配置派生表）")
    void priorityStoreCorruptFile(@TempDir File dir) throws Exception {
        PriorityStore store = new PriorityStore(dir);
        File target = new File(dir, "parent-priority.properties");
        try (java.io.FileOutputStream out = new java.io.FileOutputStream(target)) {
            // epoch 非数字 → NumberFormatException → 返回 null
            out.write("epoch=abc\nsource=CONFIG\n".getBytes(StandardCharsets.UTF_8));
        }
        assertNull(store.load(), "epoch 非数字的损坏文件应返回 null");
    }

    // ==================== RaftNodeImpl 提升/回退编排 ====================

    @Test
    @DisplayName("单节点提升：换表 + term 跃升 + 成主 + 落盘")
    void promoteSingleNode() {
        Map<String, Integer> weights = new LinkedHashMap<>();
        weights.put("dc-a", 2);
        weights.put("dc-b", 1);
        DatacenterPriorityTable table = new DatacenterPriorityTable(weights, DatacenterPriorityTable.Source.CONFIG);
        DatacenterPriorityVoteWeightStrategy strategy =
            new DatacenterPriorityVoteWeightStrategy(null, "dc-a", 1, table);

        RaftNode node = new RaftNode.Builder()
            .nodeId("node-a1")
            .datacenter("dc-a")
            .peerIds(Collections.<String>emptyList())
            .electionTimeout(new ElectionTimeout(150, 300))
            .voteWeightStrategy(strategy)
            .priorityTable(table)
            .build();
        node.start();
        try {
            long termBefore = node.getTerm().getCurrent();
            assertTrue(node.promoteDatacenter("alice", 100, "test promote"));

            // 换表：dc-a = max(对侧=1) + 2 = 3
            DatacenterPriorityTable.Snapshot snap = node.getPrioritySnapshot();
            assertEquals(1, snap.epoch());
            assertEquals(DatacenterPriorityTable.Source.PROMOTED, snap.source());
            assertEquals(Integer.valueOf(3), snap.weights().get("dc-a"));
            assertEquals(Integer.valueOf(1), snap.weights().get("dc-b"));

            // term 跃升 ≥100
            assertTrue(node.getTerm().getCurrent() >= termBefore + 100,
                "term 应跃升至少 100");
            // 自投成主
            assertTrue(node.isLeader());
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("回退：以配置表为新快照、epoch 递增、来源 CONFIG")
    void restoreDefaultsAfterPromote() {
        Map<String, Integer> weights = new LinkedHashMap<>();
        weights.put("dc-a", 2);
        weights.put("dc-b", 1);
        DatacenterPriorityTable table = new DatacenterPriorityTable(weights, DatacenterPriorityTable.Source.CONFIG);
        DatacenterPriorityVoteWeightStrategy strategy =
            new DatacenterPriorityVoteWeightStrategy(null, "dc-a", 1, table);

        RaftNode node = new RaftNode.Builder()
            .nodeId("node-a2")
            .datacenter("dc-a")
            .peerIds(Collections.<String>emptyList())
            .electionTimeout(new ElectionTimeout(150, 300))
            .voteWeightStrategy(strategy)
            .priorityTable(table)
            .build();
        node.start();
        try {
            assertTrue(node.promoteDatacenter("alice", 100, "promote"));
            assertEquals(Integer.valueOf(3), node.getPrioritySnapshot().weights().get("dc-a"));

            assertTrue(node.restoreDefaultPriorities("alice", "hand back"));
            DatacenterPriorityTable.Snapshot snap = node.getPrioritySnapshot();
            // epoch 递增（提升为 1 → 回退为 2）
            assertEquals(2, snap.epoch());
            assertEquals(DatacenterPriorityTable.Source.CONFIG, snap.source());
            // 权重回配置值
            assertEquals(Integer.valueOf(2), snap.weights().get("dc-a"));
            assertEquals(Integer.valueOf(1), snap.weights().get("dc-b"));
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("无优先级表（单机房/子组）时提升/回退均拒绝")
    void promoteWithoutTableRejected() {
        RaftNode node = new RaftNode.Builder()
            .nodeId("node-solo")
            .peerIds(Arrays.asList("node-x", "node-y"))
            .electionTimeout(new ElectionTimeout(150, 300))
            .build();
        node.start();
        try {
            assertFalse(node.promoteDatacenter("alice", 100, "no table"));
            assertFalse(node.restoreDefaultPriorities("alice", "no table"));
            assertNull(node.getPrioritySnapshot());
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("无代表席位（seatHeld=false）时提升拒绝")
    void promoteWithoutSeatRejected() {
        Map<String, Integer> weights = new LinkedHashMap<>();
        weights.put("dc-a", 2);
        weights.put("dc-b", 1);
        DatacenterPriorityTable table = new DatacenterPriorityTable(weights, DatacenterPriorityTable.Source.CONFIG);
        DatacenterPriorityVoteWeightStrategy strategy =
            new DatacenterPriorityVoteWeightStrategy(null, "dc-a", 1, table);

        RaftNode node = new RaftNode.Builder()
            .nodeId("node-a3")
            .datacenter("dc-a")
            .peerIds(Collections.<String>emptyList())
            .electionTimeout(new ElectionTimeout(150, 300))
            .voteWeightStrategy(strategy)
            .priorityTable(table)
            .build();
        node.setSeatHeld(false);
        node.start();
        try {
            assertFalse(node.promoteDatacenter("alice", 100, "learner"));
        } finally {
            node.shutdown();
        }
    }
}
