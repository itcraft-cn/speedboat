package cn.itcraft.speedboat.serialize;

/**
 * SerializationException 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class SerializationException extends RuntimeException {
    
    public SerializationException(String message) {
        super(message);
    }
    
    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
