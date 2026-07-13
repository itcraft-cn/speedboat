package cn.itcraft.speedboat.raft;

import java.util.Objects;

public class MemberChangeEntry extends LogEntry {

    public enum ChangeType {
        ADD,
        REMOVE
    }

    private final ChangeType changeType;
    private final String nodeId;
    private final String address;

    public MemberChangeEntry(long index, long term, String leaderId, ChangeType changeType, String nodeId, String address) {
        super(index, term, leaderId, EntryType.MEMBER_CHANGE);
        this.changeType = changeType;
        this.nodeId = nodeId;
        this.address = address;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public String getNodeId() {
        return nodeId;
    }
    
    public String getPeerId() {
        return getNodeId();
    }

    public String getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MemberChangeEntry that = (MemberChangeEntry) o;
        return changeType == that.changeType &&
                Objects.equals(nodeId, that.nodeId) &&
                Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), changeType, nodeId, address);
    }

    @Override
    public String toString() {
        return "MemberChangeEntry{" +
                "index=" + getIndex() +
                ", term=" + getTerm() +
                ", leaderId='" + getLeaderId() + '\'' +
                ", changeType=" + changeType +
                ", nodeId='" + nodeId + '\'' +
                ", address='" + address + '\'' +
                '}';
    }

    public static MemberChangeEntry add(long index, long term, String leaderId, String nodeId, String address) {
        return new MemberChangeEntry(index, term, leaderId, ChangeType.ADD, nodeId, address);
    }

    public static MemberChangeEntry remove(long index, long term, String leaderId, String nodeId) {
        return new MemberChangeEntry(index, term, leaderId, ChangeType.REMOVE, nodeId, null);
    }
}