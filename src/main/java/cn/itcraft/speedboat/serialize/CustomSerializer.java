package cn.itcraft.speedboat.serialize;
import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.rpc.LockOpRequest;
import cn.itcraft.speedboat.rpc.LockOpResponse;
import cn.itcraft.speedboat.rpc.PreVoteRequest;
import cn.itcraft.speedboat.rpc.PreVoteResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
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
    private final Serializer payloadSerializer;
    private final Map<Class<?>, Integer> typeToId = new HashMap<>();
    private final Map<Integer, Class<?>> idToType = new HashMap<>();
    
    public CustomSerializer(Serializer payloadSerializer) {
        this.payloadSerializer = payloadSerializer;
        registerType(1, RequestVoteRequest.class);
        registerType(2, RequestVoteResponse.class);
        registerType(3, HeartbeatRequest.class);
        registerType(4, HeartbeatResponse.class);
        registerType(5, AppendEntriesRequest.class);
        registerType(6, AppendEntriesResponse.class);
        registerType(7, PreVoteRequest.class);
        registerType(8, PreVoteResponse.class);
        // 命名锁转发协议（2026-09-18 设计）：非 Leader 成员锁操作 → Leader
        registerType(9, LockOpRequest.class);
        registerType(10, LockOpResponse.class);
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
        // 即 length 字段之后的所有字节数；帧布局固定：length(4)+crc(4)+ser(1)+msg(1)+payload
        int lengthField = SUB_HEADER_LEN + payload.length;
        int totalLen = LENGTH_FIELD_LEN + lengthField;
        ByteBuffer buffer = ByteBuffer.allocate(totalLen);

        // 报告002 M-5：CRC 覆盖范围扩为"ser/msg 类型头 + payload"（原先只含 payload，类型头
        // 位翻转不受校验保护；serializerTypeId 曾读出未用——现在参与 CRC 且 unwrap 同步校验）。
        // 帧布局不变，仅计算范围收口；收发两端同步生成，不引入跨版本兼容矩阵。
        byte serByte = (byte) payloadSerializer.getTypeId();
        byte msgByte = (byte) (typeId.intValue());
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        crc32.update(serByte);
        crc32.update(msgByte);

        buffer.putInt(lengthField);
        buffer.putInt((int) crc32.getValue());
        buffer.put(serByte);
        buffer.put(msgByte);
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
        // 报告002 M-5：与 wrap 同步——CRC 覆盖"payload + 类型头"，类型头位翻转可被拦截
        crc32.update((byte) serializerTypeId);
        crc32.update((byte) messageTypeId);
        if ((int) crc32.getValue() != crc32Value) {
            throw new SerializationException("CRC32 check failed");
        }

        // serializerTypeId 此前读取后未使用（报告002 M-5）——收紧为显式校验：
        // 当前唯一 force = Protostuff；未知序列化器直接拒绝，不再半信半疑
        if (serializerTypeId != payloadSerializer.getTypeId()) {
            throw new SerializationException("Unknown serializer type id: " + serializerTypeId);
        }

        Class<?> targetClass = idToType.get(messageTypeId);
        if (targetClass == null) {
            throw new SerializationException("Unknown message type id: " + messageTypeId);
        }
        
        return (T) payloadSerializer.deserialize(payload, targetClass);
    }
}
