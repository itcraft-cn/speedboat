package cn.itcraft.speedboat.raft;

public class CommandLogEntry extends LogEntry {

    public CommandLogEntry(long index, long term, String leaderId, byte[] data) {
        super(index, term, leaderId, EntryType.COMMAND, data);
    }

    public static CommandLogEntry create(long index, long term, String leaderId, byte[] data) {
        return new CommandLogEntry(index, term, leaderId, data);
    }
}