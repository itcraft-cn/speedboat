package cn.itcraft.speedboat.raft.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * BoundedCache 契约测试（SOFAJRaft RequestMap 纪律落地件的独立验证）。
 *
 * @author speedboat
 * @since 1.1.0
 */
class BoundedCacheTest {

    @Test
    @DisplayName("硬上界：超容量驱逐最久未访问条目")
    void evictionOnOverflow() {
        BoundedCache<String, Integer> cache = new BoundedCache<>(3);
        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);
        // 访问 a 使其变为最新；b 成为最老
        cache.get("a");
        cache.put("d", 4);

        assertEquals(3, cache.size());
        assertNull(cache.get("b"), "最久未访问的 b 应被驱逐");
        assertEquals(1, cache.get("a"));
        assertEquals(4, cache.get("d"));
    }

    @Test
    @DisplayName("computeIfAbsent 幂等：同 key 只计算一次")
    void computeIfAbsentSingleLoad() {
        BoundedCache<String, Integer> cache = new BoundedCache<>(8);
        AtomicInteger loads = new AtomicInteger();

        Integer v1 = cache.computeIfAbsent("k", k -> loads.incrementAndGet());
        Integer v2 = cache.computeIfAbsent("k", k -> loads.incrementAndGet());

        assertEquals(1, loads.get());
        assertEquals(v1, v2);
    }

    @Test
    @DisplayName("覆盖写入不产生容量溢出；非法容量拒绝")
    void putOverwriteAndGuard() {
        BoundedCache<String, Integer> cache = new BoundedCache<>(2);
        cache.put("k", 1);
        cache.put("k", 2);
        assertEquals(1, cache.size());
        assertEquals(2, cache.get("k"));
        assertThrows(IllegalArgumentException.class, () -> new BoundedCache<String, Integer>(0));
    }

    @Test
    @DisplayName("命中刷新 LRU 位次：get 与 put 同等续期")
    void getRefreshesRecency() {
        BoundedCache<String, Integer> cache = new BoundedCache<>(3);
        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);
        cache.get("b"); // b 最近
        cache.put("d", 4);
        assertNull(cache.get("a"), "a 未被重访问应被驱逐");
        assertEquals(2, cache.get("b"));
        assertEquals(3, cache.size());
    }
}
