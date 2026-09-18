package cn.itcraft.speedboat.persistence;

import cn.itcraft.speedboat.raft.LogEntry;
import cn.itcraft.speedboat.raft.MemberChangeEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.zip.CRC32;

/**
 * MMAP 持久化实现（三档策略之极稳定环境档：跨进程重启可挂回）。
 *
 * <p><b>核心形态（2026-09-18 P4 拍板）：</b>单固定大小文件 mmap 映射，写消耗等同内存，
 * 崩溃/重启后重新映射同一文件即恢复 term 与日志——"万一丢了，还能挂回"。</p>
 *
 * <p><b>文件布局：</b></p>
 * <pre>
 * [0,128)    term 槽位 0：{magic,version,seq,term,votedFor,leaderId,CRC}
 * [128,256)  term 槽位 1：双槽交替写；读侧取 seq 最大且 CRC 有效者（防半写撕裂）
 * [1024,+∞)  顺序 WAL：len(4)|crc32(4)|body
 * </pre>
 *
 * <p><b>body 帧语义（type 路由）：</b></p>
 * <ul>
 *   <li>0 LEADER_INFO：index/term/leaderId</li>
 *   <li>1 MEMBER_CHANGE：op(0=ADD/1=REMOVE)/nodeId/address + index/term/leaderId</li>
 *   <li>2 COMMAND：index/term/leaderId/data（锁命令等）</li>
 *   <li>3 TRUNC_FROM：index=fromIndex；扫描时裁去 [index, +∞)（follower 冲突回退语义）</li>
 *   <li>4 TRUNC_UNTIL：index=untilIndex；扫描时裁去 (-∞, index]</li>
 * </ul>
 *
 * <p><b>语义边界（必须先懂再改）：</b></p>
 * <ul>
 *   <li>term 双槽 + msync——选主安全性零丢失；</li>
 *   <li>日志 append 入 page cache；{@link #flush()} 契约点（节点 stop / 检查点）强制 msync；
 *       未 msync 尾部断电场景可能丢——raft 未提交尾部丢失属安全域（客户端重试）；</li>
 *   <li>容量压力：超额丢帧并告警（本档依赖 M2 检查点裁前缀，v1 不自动轮转）。</li>
 * </ul>
 *
 * <p><b>线程模型：</b>persist* 仅 raft 单线程调用；restore* 仅启动路径一次；
 * 检查点文件统一 {@code {dir}/state-machine.ckpt}。</p>
 *
 * @author speedboat
 * @since 1.1.0
 * @see RaftStore
 */
public class MmapRaftStore implements RaftStore {

    private static final Logger logger = LoggerFactory.getLogger(MmapRaftStore.class);

    /** superblock 魔数 "RFAV" */
    static final int SLOT_MAGIC = 0x52464156;
    /** 槽位版本（帧结构变更才递增） */
    static final int SLOT_VERSION = 1;
    /** 单槽字节数（含尾部 CRC） */
    static final int SLOT_SIZE = 128;
    /** WAL 起始偏移 */
    static final int LOG_START = 1024;
    /** 默认容量（MB） */
    public static final int DEFAULT_SIZE_MB = 128;

    // term 槽内偏移（SLOT_SIZE=128）
    private static final int OFF_MAGIC = 0;
    private static final int OFF_VERSION = 4;
    private static final int OFF_SEQ = 8;
    private static final int OFF_TERM = 16;
    private static final int OFF_VF_LEN = 24;
    private static final int OFF_VF = 28;
    private static final int VF_CAP = 48;
    private static final int OFF_LEADER_LEN = 88;
    private static final int OFF_LEADER = 92;
    private static final int LEADER_CAP = 30;
    private static final int OFF_CRC = SLOT_SIZE - 4;

    // 帧类型
    static final byte FRAME_LEADER_INFO = 0;
    static final byte FRAME_MEMBER_CHANGE = 1;
    static final byte FRAME_COMMAND = 2;
    static final byte FRAME_TRUNC_FROM = 3;
    static final byte FRAME_TRUNC_UNTIL = 4;

    /** WAL 扫描/追加共同维护的内存镜像（恢复输出/截断收口） */
    private final TreeMap<Long, LogEntry> entryIndex = new TreeMap<>();

    private final File directory;
    private final String fileNameBase;
    private final int sizeMb;

    private MappedByteBuffer mmap;
    private RandomAccessFile rafRef;
    private FileChannel channelRef;
    private File activeFile;
    private long sequence = 0;
    private long writeOffset = LOG_START;
    private volatile boolean opened;
    /**
     * 检查点水位（P4-M4）：状态机已落盘覆盖至该索引，重启恢复时 ≤ 该索引的日志
     * 可由检查点重建——容量压力下允许据此压实回收 WAL 前缀。
     * 仅 raft 单线程写（markCheckpoint/appendFrame 同线程），volatile 供只读观测。
     */
    private volatile long compactFloor = 0L;
    /** 压实进行中标志：压实重写保留段时禁止再次触发压力压实（防递归） */
    private boolean compacting = false;

    /** 双槽扫描取最大合法 seq；无有效槽返回 0（新起） */
    private long recoverTermSlot() {
        long s0 = readSlotSeq(0);
        long s1 = readSlotSeq(1);
        long best = Math.max(s0, s1);
        if (best == Long.MIN_VALUE) {
            logger.info("MmapRaftStore: no valid term slot, blank start");
            return 0L;
        }
        int base = (s0 >= s1 ? 0 : 1) * SLOT_SIZE;
        long term = mmap.getLong(base + OFF_TERM);
        int vfLen = clampLen(mmap.getInt(base + OFF_VF_LEN), VF_CAP);
        String vf = readFixedString(base + OFF_VF, vfLen);
        int leLen = clampLen(mmap.getInt(base + OFF_LEADER_LEN), LEADER_CAP);
        String le = readFixedString(base + OFF_LEADER, leLen);
        logger.info("MmapRaftStore recovered term: term={} votedFor={} leader={} seq={}", term, vf, le, best);
        return best;
    }

    public MmapRaftStore(File directory, String fileNameBase, int sizeMb) {
        this.directory = directory;
        this.fileNameBase = (fileNameBase == null || fileNameBase.isEmpty()) ? "raft.mmap" : fileNameBase;
        this.sizeMb = sizeMb <= 0 ? DEFAULT_SIZE_MB : sizeMb;
    }

    /** 打开并 mmap（幂等）；raft 首次 persist/restore 时 lazy 触发。 */
    public void open() {
        if (opened) {
            return;
        }
        synchronized (this) {
            if (opened) {
                return;
            }
            try {
                directory.mkdirs();
                File target = new File(directory, fileNameBase);
                this.rafRef = new RandomAccessFile(target, "rw");
                this.channelRef = rafRef.getChannel();
                long expect = 1024L * 1024L * sizeMb;
                if (rafRef.length() < expect) {
                    rafRef.setLength(expect);
                }
                this.mmap = channelRef.map(FileChannel.MapMode.READ_WRITE, 0, expect);
                this.opened = true;
                this.sequence = recoverTermSlot();
                scanRecords();
                logger.info("MmapRaftStore attached: file={} sizeMb={} seq={} writeOffset={} entries={}",
                    target.getAbsolutePath(), sizeMb, sequence, writeOffset, entryIndex.size());
            } catch (IOException e) {
                throw new IllegalStateException("MmapRaftStore open failed: " + e.getMessage(), e);
            }
        }
    }

    /** 优雅停机：尾部 msync + 关闭映射；幂等。 */
    public void close() {
        synchronized (this) {
            if (!opened) {
                return;
            }
            try {
                mmap.force();
            } catch (Throwable t) {
                logger.warn("MmapRaftStore final force failed: {}", t.toString());
            }
            try {
                channelRef.close();
            } catch (IOException e) {
                logger.warn("channel close failed: {}", e.toString());
            }
            try {
                rafRef.close();
            } catch (IOException e) {
                logger.warn("raf close failed: {}", e.toString());
            }
            mmap = null;
            channelRef = null;
            rafRef = null;
            opened = false;
        }
    }

    // ==================== RaftStore 契约实现 ====================

    @Override
    public void persistAndFlushTerm(long term, String votedFor, String leaderId) {
        open();
        long seq = ++sequence;
        int slot = (int) (seq & 1);
        int base = slot * SLOT_SIZE;

        byte[] vf = utfFixed(votedFor, VF_CAP);
        byte[] le = utfFixed(leaderId, LEADER_CAP);

        mmap.putInt(base + OFF_MAGIC, SLOT_MAGIC);
        mmap.putInt(base + OFF_VERSION, SLOT_VERSION);
        mmap.putLong(base + OFF_SEQ, seq);
        mmap.putLong(base + OFF_TERM, term);
        mmap.putInt(base + OFF_VF_LEN, vf.length);
        putRaw(mmap, base + OFF_VF, vf);
        mmap.putInt(base + OFF_LEADER_LEN, le.length);
        putRaw(mmap, base + OFF_LEADER, le);

        // CRC 覆盖 [base, base+OFF_CRC)
        byte[] body = new byte[OFF_CRC];
        ByteBuffer r = mmap.duplicate();
        r.position(base).limit(base + OFF_CRC);
        r.get(body, 0, OFF_CRC);
        CRC32 crc32 = new CRC32();
        crc32.update(body);
        mmap.putInt(base + OFF_CRC, (int) crc32.getValue());

        // 铁律：term 槽写必须 msync（选主安全性，绝不丢）
        mmap.force();
    }

    @Override
    public RaftTermRecord restoreTerm() {
        open();
        long s0 = readSlotSeq(0);
        long s1 = readSlotSeq(1);
        int pick = (s0 >= s1) ? 0 : 1;
        long validSeq = (pick == 0) ? s0 : s1;
        if (validSeq == Long.MIN_VALUE) {
            logger.info("MmapRaftStore: no valid term slot, blank start");
            return null;
        }
        int base = pick * SLOT_SIZE;
        long term = mmap.getLong(base + OFF_TERM);
        int vfLen = clampLen(mmap.getInt(base + OFF_VF_LEN), VF_CAP);
        String vf = readFixedString(base + OFF_VF, vfLen);
        int leLen = clampLen(mmap.getInt(base + OFF_LEADER_LEN), LEADER_CAP);
        String le = readFixedString(base + OFF_LEADER, leLen);
        logger.info("MmapRaftStore recovered term: term={} votedFor={} leader={} seq={}",
            term, vf, le, validSeq);
        return new RaftTermRecord(term, vf, le);
    }

    @Override
    public void persistLogEntries(List<LogEntry> entries) {
        open();
        if (entries == null || entries.isEmpty()) {
            return;
        }
        long listFirstIndex = entries.get(0).getIndex();
        long listMaxIndex = entries.get(entries.size() - 1).getIndex();

        // "列表即全量"语义（对齐 InMemoryRaftStore）：列表覆盖 [first..last] 区间；
        // WAL 内首条 >= first 的既有索引若存在（含 term 冲突），必须先收口再重写，
        // 否则截断标记缺失会让旧帧在扫描时复活（defect-20260918-01 同族缺陷）。
        if (!entryIndex.isEmpty() && entryIndex.lastKey() >= listFirstIndex) {
            writeTruncateFrame(FRAME_TRUNC_FROM, listFirstIndex);
            entryIndex.tailMap(listFirstIndex).clear();
        }

        for (LogEntry e : entries) {
            if (e == null) {
                continue;
            }
            if (entryIndex.containsKey(e.getIndex())) {
                // 同索引同内容幂等跳过（term 一致为准；冲突已被上方收口融化）
                continue;
            }
            writeLogFrame(e);
        }

        if (!entryIndex.isEmpty() && entryIndex.lastKey() > listMaxIndex) {
            writeTruncateFrame(FRAME_TRUNC_FROM, listMaxIndex + 1);
            entryIndex.tailMap(listMaxIndex + 1).clear();
        }
    }

    @Override
    public void truncateLogEntriesFrom(long fromIndex) {
        open();
        entryIndex.tailMap(fromIndex).clear();
        writeTruncateFrame(FRAME_TRUNC_FROM, fromIndex);
    }

    @Override
    public void truncateLogEntriesUntil(long untilIndex) {
        open();
        entryIndex.headMap(untilIndex, true).clear();
        writeTruncateFrame(FRAME_TRUNC_UNTIL, untilIndex);
    }

    @Override
    public void deleteSnapshotChunks() {
        new File(directory, "state-machine.ckpt").delete();
    }

    @Override
    public void flush() {
        if (opened && mmap != null) {
            mmap.force();
        }
    }

    @Override
    public List<LogEntry> restoredLogEntries() {
        open();
        return new ArrayList<>(entryIndex.values());
    }

    @Override
    public String getCheckpointFile() {
        return new File(directory, "state-machine.ckpt").getAbsolutePath();
    }

    /**
     * 检查点水位上报（容量回收联动）：状态机快照成功落盘后由 RaftNode 调用。
     * 记录 ceil，仅上不减（水位单调）。
     */
    @Override
    public void markCheckpoint(long appliedIndex) {
        if (appliedIndex > compactFloor) {
            compactFloor = appliedIndex;
            logger.debug("MmapRaftStore checkpoint floor advanced to {}", appliedIndex);
        }
    }

    /** 当前检查点水位（测试/排障用） */
    long getCompactFloor() {
        return compactFloor;
    }

    // ==================== WAL 内部 ====================

    /**
     * 全量扫描 WAL 重构 entryIndex；坏帧/未知帧即止（尾部收敛）。
     * open() 内单线程调用一次；无人竞争。
     */
    private void scanRecords() {
        long pos = LOG_START;
        while (pos + 8 <= mmap.capacity()) {
            int bodyLen = mmap.getInt((int) pos);
            int crc = mmap.getInt((int) (pos + 4));
            long frameEnd = pos + 8L + bodyLen;
            if (bodyLen < 2 || frameEnd > mmap.capacity()) {
                break;
            }
            byte[] body = new byte[bodyLen];
            ByteBuffer r = mmap.duplicate();
            r.position((int) (pos + 8));
            r.get(body, 0, bodyLen);
            CRC32 crc32 = new CRC32();
            crc32.update(body);
            if ((int) crc32.getValue() != crc) {
                logger.warn("MmapRaftStore scan stop at CRC mismatch: pos={}", pos);
                break;
            }
            applyFrame(body);
            pos = frameEnd;
            writeOffset = frameEnd;
        }
    }

    private void applyFrame(byte[] body) {
        ByteBuffer b = ByteBuffer.wrap(body);
        byte type = b.get();
        long index = b.getLong();
        long term = b.getLong();

        switch (type) {
            case FRAME_LEADER_INFO: {
                String leader = readLengthPrefixedString(b);
                entryIndex.put(index, new LogEntry(index, term, leader));
                break;
            }
            case FRAME_MEMBER_CHANGE: {
                byte op = b.get();
                String leader = readLengthPrefixedString(b);
                String nodeId = readLengthPrefixedString(b);
                String address = b.hasRemaining() ? readLengthPrefixedString(b) : "";
                MemberChangeEntry mce = op == 0
                    ? MemberChangeEntry.add(index, term, leader, nodeId, address)
                    : MemberChangeEntry.remove(index, term, leader, nodeId);
                entryIndex.put(index, mce);
                break;
            }
            case FRAME_COMMAND: {
                String leader = readLengthPrefixedString(b);
                int dataLen = b.getInt();
                byte[] data = new byte[dataLen];
                b.get(data);
                entryIndex.put(index, cn.itcraft.speedboat.raft.CommandLogEntry.create(index, term, leader, data));
                break;
            }
            case FRAME_TRUNC_FROM:
                entryIndex.tailMap(index).clear();
                break;

            case FRAME_TRUNC_UNTIL:
                entryIndex.headMap(index, true).clear();
                break;

            default:
                logger.warn("MmapRaftStore unknown frame type {} at index {}, skipped", type, index);
        }
    }

    /** 普通日志帧：type(1)|index(8)|term(8)|type 相关 payload；tail 由截断帧负责。 */
    private void writeLogFrame(LogEntry e) {
        byte frameType = typeByte(e.getEntryType());
        ByteBuffer body = frameBody(e, frameType);
        appendFrame(body);
        entryIndex.put(e.getIndex(), copyOf(e));
    }

    /** 截帧：type|index|term|leader+data 或 op+nodeId+addr。 */
    private ByteBuffer frameBody(LogEntry e, byte frameType) {
        byte[] leader = utf(e.getLeaderId());
        if (frameType == FRAME_MEMBER_CHANGE) {
            MemberChangeEntry m = (MemberChangeEntry) e;
            byte op = m.getChangeType() == MemberChangeEntry.ChangeType.ADD ? (byte) 0 : (byte) 1;
            byte[] nodeId = utf(m.getNodeId());
            byte[] addr = utf(m.getAddress());
            // op(1) 为帧体独立字段（type/index/term 之外）——遗漏会 BufferOverflow
            int bodyLen = 1 + 8 + 8 + 1 + 4 + leader.length + 4 + nodeId.length + 4 + addr.length;
            ByteBuffer b = ByteBuffer.allocate(bodyLen);
            b.put(frameType);
            b.putLong(e.getIndex());
            b.putLong(e.getTerm());
            b.put(op);
            b.putInt(leader.length);
            b.put(leader);
            b.putInt(nodeId.length);
            b.put(nodeId);
            b.putInt(addr.length);
            b.put(addr);
            b.flip();
            return b;
        }
        byte[] data = e.getData() == null ? new byte[0] : e.getData();
        int bodyLen = 1 + 8 + 8 + 4 + leader.length + 4 + data.length;
        ByteBuffer b = ByteBuffer.allocate(bodyLen);
        b.put(frameType);
        b.putLong(e.getIndex());
        b.putLong(e.getTerm());
        b.putInt(leader.length);
        b.put(leader);
        b.putInt(data.length);
        b.put(data);
        b.flip();
        return b;
    }

    private void appendFrame(ByteBuffer body) {
        int bodyLen = body.remaining();
        // 容量压力（剩余不足 1/4）且已有检查点水位 → 先压实回收前缀再写
        if (!compacting && writeOffset + 8L + bodyLen > softThreshold()) {
            maybeCompact(bodyLen);
        }
        int frameEnd = (int) writeOffset + 8 + bodyLen;
        if (frameEnd > mmap.capacity() || (int) writeOffset < LOG_START) {
            logger.warn("MmapRaftStore WAL full after compaction: file={} writeOffset={} compactFloor={}; 帧丢弃（内存日志不受影响，重启由 Leader 重同步）",
                fileNameBase, writeOffset, compactFloor);
            return;
        }
        mmap.putInt((int) writeOffset, bodyLen);
        ByteBuffer w = mmap.duplicate();
        w.position((int) (writeOffset + 8));
        w.put(body.duplicate());
        byte[] copy = new byte[bodyLen];
        ByteBuffer r = mmap.duplicate();
        r.position((int) (writeOffset + 8));
        r.get(copy, 0, bodyLen);
        CRC32 crc32 = new CRC32();
        crc32.update(copy);
        mmap.putInt((int) (writeOffset + 4), (int) crc32.getValue());
        writeOffset = frameEnd;
    }

    /** 软阈值：WAL 区域剩余不足 1/4 即触发压实（留出写入余量） */
    private long softThreshold() {
        long logRegion = mmap.capacity() - LOG_START;
        return mmap.capacity() - logRegion / 4;
    }

    /**
     * WAL 压实回收（P4-M4，容量压力路径）：把索引 > compactFloor 的日志帧原地重写到
     * 区域起始，丢弃已被检查点覆盖的前缀；随后 force 落盘。
     *
     * <p>安全性：仅 raft 单线程调用；重写前内存镜像清空、逐条重编码（自描述重放源语义不变）；
     * 重启恢复走"检查点(≤floor) + WAL 保留段(>floor)"，与压实前语义等价。</p>
     */
    private void maybeCompact(int incomingBytes) {
        if (compactFloor <= 0) {
            logger.warn("MmapRaftStore WAL pressure but checkpoint floor unset: file={} writeOffset={}; 暂不回收（等待首个检查点）",
                fileNameBase, writeOffset);
            return;
        }
        compactNow();
    }

    /** 立即压实（包级可见，供测试与压力路径调用）；返回回收后写位。 */
    long compactNow() {
        long floor = compactFloor;
        List<LogEntry> retained = new ArrayList<>(entryIndex.tailMap(floor, false).values());
        compacting = true;
        try {
            writeOffset = LOG_START;
            entryIndex.clear();
            for (LogEntry e : retained) {
                writeLogFrame(e);
            }
        } finally {
            compacting = false;
        }
        mmap.force();
        logger.info("MmapRaftStore compacted: file={} floor={} retained={} writeOffset={}",
            fileNameBase, floor, retained.size(), writeOffset);
        return writeOffset;
    }

    /** 写一条截断标记帧（restore 扫描按帧裁剪 entryIndex 内存镜像）。 */
    private void writeTruncateFrame(byte frameType, long targetIndex) {
        ByteBuffer body = ByteBuffer.allocate(1 + 8 + 8);
        body.put(frameType);
        body.putLong(targetIndex);
        body.putLong(0L);
        body.flip();
        appendFrame(body);
    }

    private byte typeByte(LogEntry.EntryType t) {
        if (t == LogEntry.EntryType.MEMBER_CHANGE) {
            return FRAME_MEMBER_CHANGE;
        }
        if (t == LogEntry.EntryType.COMMAND) {
            return FRAME_COMMAND;
        }
        return FRAME_LEADER_INFO;
    }

    private LogEntry copyOf(LogEntry src) {
        if (src.getEntryType() == LogEntry.EntryType.MEMBER_CHANGE) {
            MemberChangeEntry m = (MemberChangeEntry) src;
            return m.getChangeType() == MemberChangeEntry.ChangeType.ADD
                ? MemberChangeEntry.add(src.getIndex(), src.getTerm(), src.getLeaderId(), m.getNodeId(), m.getAddress())
                : MemberChangeEntry.remove(src.getIndex(), src.getTerm(), src.getLeaderId(), m.getNodeId());
        }
        LogEntry restored = new LogEntry(src.getIndex(), src.getTerm(), src.getLeaderId());
        restored.setEntryType(src.getEntryType());
        restored.setData(src.getData() == null ? null : src.getData().clone());
        return restored;
    }

    private byte[] utf(String s) {
        return s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
    }

    /** 定长填充写入槽位（_Grid：短补零，长截断） */
    private byte[] utfFixed(String s, int cap) {
        byte[] b = utf(s);
        byte[] out = new byte[cap];
        System.arraycopy(b, 0, out, 0, Math.min(b.length, cap));
        return out;
    }

    /** 读槽数字 (CRC 前置校验) */
    private long readSlotSeq(int slotIdx) {
        if (mmap == null) {
            return Long.MIN_VALUE;
        }
        int base = slotIdx * SLOT_SIZE;
        if (mmap.capacity() < base + SLOT_SIZE) {
            return Long.MIN_VALUE;
        }
        int magic = mmap.getInt(base + OFF_MAGIC);
        if (magic != SLOT_MAGIC) {
            return Long.MIN_VALUE;
        }
        byte[] body = new byte[OFF_CRC];
        ByteBuffer r = mmap.duplicate();
        r.position(base).limit(base + OFF_CRC);
        r.get(body, 0, OFF_CRC);
        CRC32 crc32 = new CRC32();
        crc32.update(body);
        if ((int) crc32.getValue() != mmap.getInt(base + OFF_CRC)) {
            return Long.MIN_VALUE;
        }
        return mmap.getLong(base + OFF_SEQ);
    }

    private void putRaw(MappedByteBuffer buf, int base, byte[] src) {
        for (int i = 0; i < src.length; i++) {
            buf.put(base + i, src[i]);
        }
    }

    private String readFixedString(int base, int len) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = mmap.get(base + i);
        }
        int real = len;
        while (real > 0 && b[real - 1] == 0) {
            real--;
        }
        return new String(b, 0, real, StandardCharsets.UTF_8);
    }

    private int clampLen(int len, int cap) {
        if (len < 0 || len > cap) {
            return 0;
        }
        return len;
    }

    private String readLengthPrefixedString(ByteBuffer b) {
        int len = b.hasRemaining() ? b.getInt() : 0;
        if (len <= 0 || b.remaining() < len) {
            return null;
        }
        byte[] out = new byte[len];
        b.get(out, 0, len);
        return new String(out, StandardCharsets.UTF_8);
    }
}
