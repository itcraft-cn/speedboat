package cn.itcraft.speedboat.serialize;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * CustomSerializer 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class CustomSerializer {
    
    private static final int HEADER_LEN = 10;
    private final Serializer payloadSerializer;
    private final Map<Class<?>, Integer> typeToId = new HashMap<>();
    private final Map<Integer, Class<?>> idToType = new HashMap<>();
    
    public CustomSerializer(Serializer payloadSerializer) {
        this.payloadSerializer = payloadSerializer;
        registerType(1, cn.itcraft.speedboat.rpc.RequestVoteRequest.class);
        registerType(2, cn.itcraft.speedboat.rpc.RequestVoteResponse.class);
        registerType(3, cn.itcraft.speedboat.rpc.HeartbeatRequest.class);
        registerType(4, cn.itcraft.speedboat.rpc.HeartbeatResponse.class);
        registerType(5, cn.itcraft.speedboat.rpc.AppendEntriesRequest.class);
        registerType(6, cn.itcraft.speedboat.rpc.AppendEntriesResponse.class);
    }
    
    private void registerType(int id, Class<?> clazz) {
        typeToId.put(clazz, id);
        idToType.put(id, clazz);
    }
    
    public byte[] wrap(Object obj) throws SerializationException {
        Integer typeId = typeToId.get(obj.getClass());
        if (typeId == null) {
            throw new SerializationException("Unregistered type: " + obj.getClass().getName());
        }
        
        byte[] payload = payloadSerializer.serialize(obj);
        
        int totalLen = HEADER_LEN + payload.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalLen);
        
        buffer.putInt(payload.length);
        
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        buffer.putInt((int) crc32.getValue());
        
        buffer.put((byte) payloadSerializer.getTypeId());
        buffer.put((byte) (typeId.intValue()));
        buffer.put(payload);
        
        return buffer.array();
    }
    
    @SuppressWarnings("unchecked")
    public <T> T unwrap(byte[] data, Class<T> clazz) throws SerializationException {
        if (data.length < HEADER_LEN) {
            throw new SerializationException("Invalid data length");
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        int payloadLen = buffer.getInt();
        int crc32Value = buffer.getInt();
        int serializerTypeId = buffer.get() & 0xFF;
        int messageTypeId = buffer.get() & 0xFF;
        
        if (payloadLen > data.length - HEADER_LEN) {
            throw new SerializationException("Invalid payload length");
        }
        
        byte[] payload = new byte[payloadLen];
        buffer.get(payload);
        
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        if ((int) crc32.getValue() != crc32Value) {
            throw new SerializationException("CRC32 check failed");
        }
        
        Class<?> targetClass = idToType.get(messageTypeId);
        if (targetClass == null) {
            throw new SerializationException("Unknown message type id: " + messageTypeId);
        }
        
        return (T) payloadSerializer.deserialize(payload, targetClass);
    }
}
