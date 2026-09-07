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
    
    /** length 字段本身的字节数（4B int） */
    private static final int LENGTH_FIELD_LEN = 4;
    /** length 字段之后、payload 之前的子头部字节数：CRC(4) + serializerType(1) + messageType(1) = 6 */
    private static final int SUB_HEADER_LEN = 6;
    /** 整个固定头部字节数：length(4) + subHeader(6) = 10 */
    private static final int HEADER_LEN = LENGTH_FIELD_LEN + SUB_HEADER_LEN;
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
    
    /**
     * 将消息对象序列化为带帧头的字节数组。
     *
     * <p>帧格式（与 LengthFieldBasedFrameDecoder(maxFrame, offset=0, lengthFieldLen=4, adjustment=0, strip=4) 配合）：</p>
     * <pre>
     * +----------------+----------------+------+------+-----------+
     * | length (4B)    | crc32 (4B)     | ser  | msg  | payload   |
     * +----------------+----------------+------+------+-----------+
     *  offset 0         offset 4         8      9      10
     * </pre>
     *
     * <p>length 字段的值 = crc32(4) + ser(1) + msg(1) + payload.length = payload.length + 6，
     * 即从 length 字段之后到帧尾的全部字节数。LengthFieldBasedFrameDecoder 读取 length 后
     * 会从流中取出对应字节数作为完整帧，initialBytesToStrip=4 剥掉 length 字段后剩余交由
     * unwrap 处理。</p>
     *
     * @param obj 待序列化的 RPC 消息对象
     * @return 带帧头的字节数组
     * @throws SerializationException 如果类型未注册或序列化失败
     */
    public byte[] wrap(Object obj) throws SerializationException {
        Integer typeId = typeToId.get(obj.getClass());
        if (typeId == null) {
            throw new SerializationException("Unregistered type: " + obj.getClass().getName());
        }
        
        byte[] payload = payloadSerializer.serialize(obj);
        
        // length 字段 = CRC(4) + serializerTypeId(1) + messageTypeId(1) + payload.length
        // 即 length 字段之后的所有字节数
        int lengthField = SUB_HEADER_LEN + payload.length;
        // 整帧总长度 = length字段(4) + lengthField
        int totalLen = LENGTH_FIELD_LEN + lengthField;
        ByteBuffer buffer = ByteBuffer.allocate(totalLen);
        
        buffer.putInt(lengthField);
        
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        buffer.putInt((int) crc32.getValue());
        
        buffer.put((byte) payloadSerializer.getTypeId());
        buffer.put((byte) (typeId.intValue()));
        buffer.put(payload);
        
        return buffer.array();
    }
    
    /**
     * 从帧字节数组中反序列化消息对象。
     *
     * <p>输入是 LengthFieldBasedFrameDecoder 剥掉 length 字段后的数据，
     * 即从 crc32 字段开始：crc32(4) + serializerType(1) + messageType(1) + payload(N)。</p>
     *
     * @param data  帧数据（已剥掉 length 字段）
     * @param clazz 目标类型（当前未使用，实际类型由 messageType 决定）
     * @return 反序列化后的消息对象
     * @throws SerializationException 如果数据格式错误或校验失败
     */
    @SuppressWarnings("unchecked")
    public <T> T unwrap(byte[] data, Class<T> clazz) throws SerializationException {
        if (data.length < SUB_HEADER_LEN) {
            throw new SerializationException("Invalid data length: " + data.length + " < " + SUB_HEADER_LEN);
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        int crc32Value = buffer.getInt();
        int serializerTypeId = buffer.get() & 0xFF;
        int messageTypeId = buffer.get() & 0xFF;
        
        int payloadLen = data.length - SUB_HEADER_LEN;
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
