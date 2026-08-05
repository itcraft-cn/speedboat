package cn.itcraft.speedboat.serialize;

/**
 * 序列化接口，定义对象与字节数组之间的转换契约。
 * 
 * <p>Serializer 是 Speedboat 序列化层的核心抽象，支持多种序列化协议（Protostuff、JSON、Java 原生等）。
 * 用于网络传输和持久化存储中的对象序列化。</p>
 * 
 * <p>设计原则：</p>
 * <ul>
 *   <li><b>类型安全</b>：泛型方法确保反序列化时的类型正确性</li>
 *   <li><b>异常透明</b>：序列化失败时抛出 {@link SerializationException}</li>
 *   <li><b>协议标识</b>：每个实现有唯一的类型 ID，支持多协议共存</li>
 *   <li><b>性能优先</b>：针对 Raft 日志复制的高频序列化场景优化</li>
 * </ul>
 * 
 * <p>实现要求：</p>
 * <ul>
 *   <li>必须保证序列化/反序列化的幂等性：deserialize(serialize(obj)) == obj</li>
 *   <li>应该处理循环引用和复杂对象图</li>
 *   <li>应该提供良好的性能，特别是对于小对象</li>
 *   <li>应该支持向前/向后兼容性（可选）</li>
 * </ul>
 * 
 * <p>典型实现：</p>
 * <ul>
 *   <li>{@link ProtostuffSerializer}：基于 Protostuff 的高性能序列化</li>
 *   <li>{@link CustomSerializer}：自定义协议封装器，支持多种底层序列化</li>
 *   <li>JsonSerializer：JSON 序列化（未来扩展）</li>
 *   <li>JavaSerializer：Java 原生序列化（兼容性备用）</li>
 * </ul>
 * 
 * <p>使用示例：</p>
 * <pre>{@code
 * // 创建序列化器
 * Serializer serializer = new ProtostuffSerializer();
 * 
 * // 序列化对象
 * byte[] data = serializer.serialize(myObject);
 * 
 * // 反序列化对象
 * MyClass obj = serializer.deserialize(data, MyClass.class);
 * 
 * // 获取序列化器类型 ID
 * int typeId = serializer.getTypeId();
 * }</pre>
 * 
 * @author speedboat
 * @see ProtostuffSerializer
 * @see CustomSerializer
 * @see SerializationException
 * @since 1.0.0
 */
public interface Serializer {
    
    byte[] serialize(Object obj) throws SerializationException;
    
    <T> T deserialize(byte[] data, Class<T> clazz) throws SerializationException;
    
    int getTypeId();
}
