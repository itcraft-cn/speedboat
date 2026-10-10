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
    /** datacenter 在 RequestVoteRequest 中的固定 field ID（基类 requestId=1，term=2，candidateId=3，voteWeight=4，datacenter=5）。 */
    private static final int REQUEST_VOTE_DATACENTER_ID = 5;
    /** datacenter 在 PreVoteRequest 中的固定 field ID（基类 requestId=1，term=2，candidateId=3，lastLogIndex=4，lastLogTerm=5，datacenter=6）。 */
    private static final int PRE_VOTE_DATACENTER_ID = 6;

    @Test
    @DisplayName("RequestVoteRequest.datacenter 必须钉在固定 field ID 5（阶段五新字段追加于其后）")
    void requestVoteDatacenterMustBeAppended() {
        // datacenter 的 field ID 必须保持稳定。阶段五在其后追加了 priorityEpoch/priorityWeights，
        // 属于安全的"追加"；若有人往 datacenter 之前插入字段，钉住的 5 就会漂移而失败。
        assertFieldIdFrozen(RequestVoteRequest.class, REQUEST_VOTE_DATACENTER_ID);
    }

    @Test
    @DisplayName("PreVoteRequest.datacenter 必须钉在固定 field ID 6")
    void preVoteDatacenterMustBeAppended() {
        assertFieldIdFrozen(PreVoteRequest.class, PRE_VOTE_DATACENTER_ID);
    }

    @Test
    @DisplayName("RequestVoteRequest 新增优先级字段必须追加在 datacenter 之后")
    void requestVotePriorityFieldsAppendedAfterDatacenter() {
        assertAppendedAfter(RequestVoteRequest.class, DATACENTER_FIELD,
            "priorityEpoch", "priorityWeights");
    }

    @Test
    @DisplayName("AppendEntriesRequest 新增优先级字段必须追加在既有字段之后")
    void appendEntriesPriorityFieldsAppended() {
        // AppendEntriesRequest 无 datacenter 字段；断言新字段占据末尾即可。
        RuntimeSchema<?> schema = RuntimeSchema.createFrom(AppendEntriesRequest.class);
        int maxOther = -1;
        int priorityEpochId = -1;
        int priorityWeightsId = -1;
        for (Field<?> field : schema.getFields()) {
            if ("priorityEpoch".equals(field.name)) {
                priorityEpochId = field.number;
            } else if ("priorityWeights".equals(field.name)) {
                priorityWeightsId = field.number;
            } else {
                maxOther = Math.max(maxOther, field.number);
            }
        }
        assertTrue(priorityEpochId > maxOther,
            "priorityEpoch 的 field ID(" + priorityEpochId + ") 必须大于其余既有字段最大值(" + maxOther + ")");
        assertTrue(priorityWeightsId > priorityEpochId,
            "priorityWeights 的 field ID(" + priorityWeightsId + ") 必须大于 priorityEpoch(" + priorityEpochId + ")");
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
     * 断言 {@code datacenter} 字段的 field ID 被钉在固定值。
     *
     * <p>这是"新增字段必须追加而非插入"不变式的更强形式：不仅要求 datacenter 存在，
     * 还要求它的 field ID 恒等于给定值。若有人往 datacenter 之前插入新字段，
     * RuntimeSchema 按声明顺序分配的 ID 会让 datacenter 漂移到别的值而触发本断言。</p>
     *
     * @param messageType RPC 消息类型
     * @param expectedId  datacenter 应占据的固定 field ID
     */
    private void assertFieldIdFrozen(Class<?> messageType, int expectedId) {
        RuntimeSchema<?> schema = RuntimeSchema.createFrom(messageType);

        int datacenterNumber = -1;
        for (Field<?> field : schema.getFields()) {
            if (DATACENTER_FIELD.equals(field.name)) {
                datacenterNumber = field.number;
                break;
            }
        }

        assertTrue(datacenterNumber > 0,
            DATACENTER_FIELD + " 字段未被 RuntimeSchema 识别（检查是否被误标为 static/transient）");
        assertEquals(expectedId, datacenterNumber,
            DATACENTER_FIELD + " 的 field ID 必须稳定为 " + expectedId
                + "；漂移说明有人在它之前插入了字段，会导致既有字段 ID 错位、新旧节点反序列化失败");
    }

    /**
     * 断言给定的若干新字段都排在 {@code baseField} 之后（追加而非插入）。
     *
     * @param messageType RPC 消息类型
     * @param baseField   既有字段名（其 field ID 必须小于所有 newFields）
     * @param newFields   新增字段名（须全部排在 baseField 之后）
     */
    private void assertAppendedAfter(Class<?> messageType, String baseField, String... newFields) {
        RuntimeSchema<?> schema = RuntimeSchema.createFrom(messageType);

        int baseNumber = -1;
        java.util.Map<String, Integer> newNumbers = new java.util.HashMap<String, Integer>();
        for (Field<?> field : schema.getFields()) {
            if (baseField.equals(field.name)) {
                baseNumber = field.number;
            } else {
                for (String nf : newFields) {
                    if (nf.equals(field.name)) {
                        newNumbers.put(nf, field.number);
                    }
                }
            }
        }

        assertTrue(baseNumber > 0, baseField + " 字段未被识别");
        for (String nf : newFields) {
            Integer num = newNumbers.get(nf);
            assertTrue(num != null && num > baseNumber,
                nf + " 的 field ID(" + num + ") 必须大于 " + baseField + "(" + baseNumber
                    + ")，说明它是追加到末尾而非插入到中间");
        }
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
