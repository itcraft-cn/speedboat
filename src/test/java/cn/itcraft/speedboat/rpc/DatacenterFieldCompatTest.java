package cn.itcraft.speedboat.rpc;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.Field;
import io.protostuff.runtime.RuntimeSchema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code datacenter} 字段的线格式兼容性测试。
 *
 * <p><b>为什么需要这个测试：</b>Protostuff 的 {@code RuntimeSchema} 按字段<b>声明顺序</b>
 * 分配 field ID。给已有 RPC 消息类新增字段时，若插入到已有字段之前，
 * 后续所有字段的 ID 会整体错位——新旧节点混跑时反序列化将直接失败或字段串位，
 * 表现为选举协议静默失效（term / voteWeight 读到错误值）。</p>
 *
 * <p>因此新增字段<b>必须追加到类的末尾</b>。本测试把该不变式固化为断言，
 * 防止后续有人在类中间插入字段。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
class DatacenterFieldCompatTest {

    private static final String DATACENTER_FIELD = "datacenter";

    @Test
    @DisplayName("RequestVoteRequest.datacenter 必须占据最大的 field ID（追加而非插入）")
    void requestVoteDatacenterMustBeAppended() {
        assertFieldAppended(RequestVoteRequest.class);
    }

    @Test
    @DisplayName("PreVoteRequest.datacenter 必须占据最大的 field ID（追加而非插入）")
    void preVoteDatacenterMustBeAppended() {
        assertFieldAppended(PreVoteRequest.class);
    }

    @Test
    @DisplayName("RequestVoteRequest - 未设置 datacenter 时序列化往返保持 null（旧版本节点兼容）")
    void requestVoteWithoutDatacenterRoundTripsAsNull() {
        RequestVoteRequest original = new RequestVoteRequest(7L, "candidate-9", 3);

        RequestVoteRequest decoded = roundTrip(original, RequestVoteRequest.class);

        assertNull(decoded.getDatacenter(), "旧版本节点发出的请求不应解析出 datacenter");
        assertEquals(7L, decoded.getTerm());
        assertEquals("candidate-9", decoded.getCandidateId());
        assertEquals(3, decoded.getVoteWeight());
    }

    @Test
    @DisplayName("RequestVoteRequest - datacenter 序列化往返一致，且不影响既有字段")
    void requestVoteWithDatacenterRoundTrips() {
        RequestVoteRequest original = new RequestVoteRequest(11L, "candidate-2", 5, "dc-0");

        RequestVoteRequest decoded = roundTrip(original, RequestVoteRequest.class);

        assertEquals("dc-0", decoded.getDatacenter());
        assertEquals(11L, decoded.getTerm());
        assertEquals("candidate-2", decoded.getCandidateId());
        assertEquals(5, decoded.getVoteWeight());
    }

    @Test
    @DisplayName("PreVoteRequest - datacenter 序列化往返一致，且不影响既有字段")
    void preVoteWithDatacenterRoundTrips() {
        PreVoteRequest original = new PreVoteRequest(9L, "candidate-5", 42L, 4L, "dc-1");

        PreVoteRequest decoded = roundTrip(original, PreVoteRequest.class);

        assertEquals("dc-1", decoded.getDatacenter());
        assertEquals(9L, decoded.getTerm());
        assertEquals("candidate-5", decoded.getCandidateId());
        assertEquals(42L, decoded.getLastLogIndex());
        assertEquals(4L, decoded.getLastLogTerm());
    }

    /**
     * 断言 {@code datacenter} 是消息类中 field ID 最大的字段。
     *
     * @param messageType RPC 消息类型
     */
    private void assertFieldAppended(Class<?> messageType) {
        RuntimeSchema<?> schema = RuntimeSchema.createFrom(messageType);

        int datacenterNumber = -1;
        int maxOtherNumber = -1;

        for (Field<?> field : schema.getFields()) {
            if (DATACENTER_FIELD.equals(field.name)) {
                datacenterNumber = field.number;
            } else {
                maxOtherNumber = Math.max(maxOtherNumber, field.number);
            }
        }

        assertTrue(datacenterNumber > 0,
            DATACENTER_FIELD + " 字段未被 RuntimeSchema 识别（检查是否被误标为 static/transient）");
        assertTrue(datacenterNumber > maxOtherNumber,
            DATACENTER_FIELD + " 的 field ID(" + datacenterNumber + ") 必须大于其余字段的最大值("
                + maxOtherNumber + ")，否则说明它是插入到类中间而非追加到末尾，"
                + "会导致既有字段 ID 错位、新旧节点之间反序列化失败");
    }

    @SuppressWarnings("unchecked")
    private <T> T roundTrip(T original, Class<T> type) {
        Schema<T> schema = RuntimeSchema.getSchema(type);
        byte[] bytes = ProtostuffIOUtil.toByteArray(original, schema, LinkedBuffer.allocate(256));
        T decoded = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(bytes, decoded, schema);
        return decoded;
    }
}
