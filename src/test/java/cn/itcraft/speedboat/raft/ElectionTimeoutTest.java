package cn.itcraft.speedboat.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ElectionTimeout 随机超时测试")
class ElectionTimeoutTest {

    @Test
    @DisplayName("构造函数初始化正确的范围")
    void constructorInitializesCorrectRange() {
        ElectionTimeout timeout = new ElectionTimeout(150, 300);
        
        assertEquals(150L, timeout.getMinMs());
        assertEquals(300L, timeout.getMaxMs());
    }

    @Test
    @DisplayName("getNext 返回值在范围内")
    void getNextReturnsValueInRange() {
        ElectionTimeout timeout = new ElectionTimeout(150, 300);
        
        for (int i = 0; i < 100; i++) {
            long value = timeout.getNext();
            assertTrue(value >= 150L && value < 300L, 
                "Timeout " + value + " should be in range [150, 300)");
        }
    }

    @Test
    @DisplayName("reset 生成新的随机超时值")
    void resetGeneratesNewValue() {
        ElectionTimeout timeout = new ElectionTimeout(150, 300);
        
        long first = timeout.getNext();
        boolean changed = false;
        
        for (int i = 0; i < 100; i++) {
            timeout.reset();
            if (timeout.getNext() != first) {
                changed = true;
                break;
            }
        }
        
        assertTrue(changed, "reset should generate different values");
    }

    @Test
    @DisplayName("多次调用 getNext 返回相同值")
    void multipleGetNextReturnsSameValue() {
        ElectionTimeout timeout = new ElectionTimeout(150, 300);
        
        long first = timeout.getNext();
        long second = timeout.getNext();
        long third = timeout.getNext();
        
        assertEquals(first, second);
        assertEquals(second, third);
    }

    @Test
    @DisplayName("边界值测试")
    void boundaryTest() {
        ElectionTimeout timeout = new ElectionTimeout(100, 101);
        
        long value = timeout.getNext();
        assertEquals(100L, value);
    }

    @Test
    @DisplayName("较大范围测试")
    void largeRangeTest() {
        ElectionTimeout timeout = new ElectionTimeout(1000, 5000);
        
        for (int i = 0; i < 100; i++) {
            long value = timeout.getNext();
            assertTrue(value >= 1000L && value < 5000L,
                "Timeout " + value + " should be in range [1000, 5000)");
        }
    }
}
