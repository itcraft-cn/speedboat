package cn.itcraft.speedboat.serialize;

public interface Serializer {
    
    byte[] serialize(Object obj) throws SerializationException;
    
    <T> T deserialize(byte[] data, Class<T> clazz) throws SerializationException;
    
    int getTypeId();
}
