package cn.itcraft.speedboat.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Term 单调性管理测试")
class TermTest {

    private Term term;

    @BeforeEach
    void setUp() {
        term = new Term();
    }

    @Test
    @DisplayName("初始值为 0")
    void initialValueIsZero() {
        assertEquals(0L, term.getCurrent());
    }

    @Test
    @DisplayName("increment 递增 Term")
    void incrementIncreasesTerm() {
        term.increment();
        assertEquals(1L, term.getCurrent());

        term.increment();
        assertEquals(2L, term.getCurrent());
    }

    @Test
    @DisplayName("updateIfHigher 更新更高的 Term")
    void updateIfHigherUpdatesWhenHigher() {
        assertTrue(term.updateIfHigher(5));
        assertEquals(5L, term.getCurrent());
    }

    @Test
    @DisplayName("updateIfHigher 不更新相等或更低的 Term")
    void updateIfHigherDoesNotUpdateWhenNotHigher() {
        term.updateIfHigher(10);
        
        assertFalse(term.updateIfHigher(10));
        assertEquals(10L, term.getCurrent());

        assertFalse(term.updateIfHigher(5));
        assertEquals(10L, term.getCurrent());
    }

    @Test
    @DisplayName("isMonotonicViolation 检测违反单调性")
    void isMonotonicViolationDetectsViolation() {
        term.updateIfHigher(10);
        
        assertTrue(term.isMonotonicViolation(5));
        assertTrue(term.isMonotonicViolation(9));
        assertFalse(term.isMonotonicViolation(10));
        assertFalse(term.isMonotonicViolation(11));
    }

    @Test
    @DisplayName("reset 重置为 0")
    void resetSetsToZero() {
        term.updateIfHigher(100);
        term.reset();
        assertEquals(0L, term.getCurrent());
    }

    @Test
    @DisplayName("多次操作保持单调性")
    void multipleOperationsMaintainMonotonicity() {
        term.increment();
        term.increment();
        term.updateIfHigher(5);
        term.increment();
        
        assertEquals(6L, term.getCurrent());
        assertFalse(term.isMonotonicViolation(6));
        assertTrue(term.isMonotonicViolation(5));
    }
}
