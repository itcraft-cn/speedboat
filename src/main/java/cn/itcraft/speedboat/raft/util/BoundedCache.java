package cn.itcraft.speedboat.raft.util;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
/**
 * 有界 LRU 缓存（内部协作器，非公开 API；tiny-rules LruCache 思想对照）。
 *
 * <p>背景（SOFAJRaft 内存纪律教训）：其 Closure/ByteBuffer/RequestMap 未清理
 * 是定级缺陷——"能用但不放心"，长期运行如埋雷。本库所有"对不可控入参建簿记表"
 * 的场景（如锁 requestId 幂等去重）一律使用本类保证：</p>
 * <ul>
 *   <li><b>硬上界</b>：容量超过 {@code maxSize} 时驱逐最久未访问条目</li>
 *   <li><b>访问有序</b>：LinkedHashMap access-order，读命中即续期</li>
 *   <li><b>线程安全</b>：所有方法 synchronized（低频路径可忽略开销）</li>
 * </ul>
 *
 * <p>幂等查取使用 {@link #computeIfAbsent(String, Function)}：同 key 只计算一次。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public final class BoundedCache<K, V> {

    /** 访问序 map（链表尾 = 最近访问；头 = 最老） */
    private final LinkedHashMap<K, V> store;
    /** 硬上界（驱驱逐使用，get 命中亦保护） */
    private final int maxSize;

    public BoundedCache(int maxSize) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("BoundedCache maxSize must be >= 1");
        }
        this.maxSize = maxSize;
        this.store = new LinkedHashMap<>(Math.min(1024, Math.max(16, maxSize / 2)), 0.75f, true);
    }

    /** 查；命中即刷新 LRU 位次 */
    public synchronized V get(K key) {
        return store.get(key);
    }

    /** 同 key 只计算一次（幂等入口），入参为 key */
    public synchronized V computeIfAbsent(K key, Function<K, V> loader) {
        V existing = store.get(key);
        if (existing != null) {
            return existing;
        }
        V computed = loader.apply(key);
        if (computed != null) {
            store.put(key, computed);
            evictIfNeeded();
        }
        return computed;
    }

    /** 写入（覆盖旧值，不丢 LRU 位次） */
    public synchronized void put(K key, V value) {
        store.put(key, value);
        evictIfNeeded();
    }

    /** 当前条目数（观测用途） */
    public synchronized int size() {
        return store.size();
    }

    /** 快照键列表（观测用途；不可变） */
    public synchronized List<K> keysSnapshot() {
        return new ArrayList<>(store.keySet());
    }

    private void evictIfNeeded() {
        if (store.size() > maxSize) {
            Iterator<Map.Entry<K, V>> it = store.entrySet().iterator();
            while (store.size() > maxSize && it.hasNext()) {
                it.next();
                it.remove();
            }
        }
    }
}
