package cn.itcraft.speedboat.raft;

/**
 * Raft 日志条目类，表示日志复制中的单个日志项。
 * 
 * <p>包含日志索引、任期、Leader ID、条目类型和序列化数据。</p>
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class LogEntry {

    public enum EntryType {
        LEADER_INFO,      // Leader标识
        MEMBER_CHANGE,    // 成员变更
        COMMAND           // 命令（分布式锁等）
    }

    private long index;
    private long term;
    private String leaderId;
    private EntryType entryType;
    private byte[] data;

    public LogEntry() {
        this.entryType = EntryType.LEADER_INFO;
        this.data = null;
    }

    public LogEntry(long index, long term, String leaderId) {
        this(index, term, leaderId, EntryType.LEADER_INFO, null);
    }

    protected LogEntry(long index, long term, String leaderId, EntryType entryType) {
        this(index, term, leaderId, entryType, null);
    }

    protected LogEntry(long index, long term, String leaderId, EntryType entryType, byte[] data) {
        this.index = index;
        this.term = term;
        this.leaderId = leaderId;
        this.entryType = entryType;
        this.data = data;
    }

    public long getIndex() {
        return index;
    }

    public long getTerm() {
        return term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public EntryType getEntryType() {
        return entryType;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    /** 日志序列化/重建路径的类型回填（MmapRaftStore 扫描与镜像 copy 使用；apply 分派依赖 entryType） */
    public void setEntryType(EntryType entryType) {
        this.entryType = entryType;
    }

    @Override
    public String toString() {
        return "LogEntry{index=" + index + ", term=" + term + 
               ", leaderId='" + leaderId + "', entryType=" + entryType +
               ", dataSize=" + (data != null ? data.length : 0) + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntry logEntry = (LogEntry) o;
        return index == logEntry.index && 
               term == logEntry.term && 
               leaderId.equals(logEntry.leaderId) &&
               entryType == logEntry.entryType &&
               java.util.Arrays.equals(data, logEntry.data);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(index);
        result = 31 * result + Long.hashCode(term);
        result = 31 * result + leaderId.hashCode();
        result = 31 * result + entryType.hashCode();
        result = 31 * result + java.util.Arrays.hashCode(data);
        return result;
    }
}