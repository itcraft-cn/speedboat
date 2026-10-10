package cn.itcraft.speedboat.rpc;

/**
 * RequestVoteRequest 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class RequestVoteRequest extends RpcRequest {

    private long term;
    private String candidateId;
    private int voteWeight;

    /**
     * 候选者所在机房标识（跨机房级联用）。
     *
     * <p>权重策略依赖此信息判断候选者机房归属；单机房模式下不设置（null）。</p>
     *
     * <p><b>注意事项：</b>本字段必须追加在类的<b>末尾</b>。Protostuff 的
     * {@code RuntimeSchema} 按字段<b>声明顺序</b>分配 field ID，追加到末尾可保持
     * 既有线格式兼容（旧版本节点收到后该字段为 null）；若插入到已有字段之前，
     * 会导致后续所有字段 ID 错位而反序列化失败。</p>
     */
    private String datacenter;

    /**
     * 优先级表版本号（阶段五人工升级 API 传播载体）。
     *
     * <p>候选者随 RequestVote 携带当前优先级快照的 epoch；接收方仅当该值 &gt; 本表 epoch
     * 才采纳其权重。0 表示不携带（子组/单机房/旧版本节点），接收方忽略。</p>
     *
     * <p><b>注意事项：</b>本字段与 {@link #priorityWeights} 必须追加在类的<b>末尾</b>
     * （Protostuff 按声明顺序分配 field ID，追加到末尾保持线格式兼容）。</p>
     */
    private long priorityEpoch;
    /** 优先级权重表编码（"dc=w,dc=w"；空表示不携带）。与 {@link #priorityEpoch} 成对追加在类末尾。 */
    private String priorityWeights;

    public RequestVoteRequest() {
        super();
    }

    public RequestVoteRequest(long term, String candidateId, int voteWeight) {
        super();
        this.term = term;
        this.candidateId = candidateId;
        this.voteWeight = voteWeight;
    }

    /**
     * 带机房标识的构造（跨机房级联选举用）。
     *
     * @param term        候选者任期
     * @param candidateId 候选者节点 ID
     * @param voteWeight  候选者自报权重
     * @param datacenter  候选者所在机房标识；单机房场景可传 null
     */
    public RequestVoteRequest(long term, String candidateId, int voteWeight, String datacenter) {
        this(term, candidateId, voteWeight);
        this.datacenter = datacenter;
    }

    public long getTerm() {
        return term;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public int getVoteWeight() {
        return voteWeight;
    }

    /**
     * 候选者所在机房标识。
     *
     * @return 机房标识；旧版本节点发出的请求或单机房场景下为 null
     */
    public String getDatacenter() {
        return datacenter;
    }

    /** 优先级表版本号（0 表示不携带） */
    public long getPriorityEpoch() {
        return priorityEpoch;
    }

    /** 优先级权重表编码（"dc=w,dc=w"；空表示不携带） */
    public String getPriorityWeights() {
        return priorityWeights;
    }

    /**
     * 携带优先级传播快照（候选者随 RequestVote 传播用）。
     *
     * @param priorityEpoch   优先级表版本号（&gt; 本表 epoch 才被接收方采纳）
     * @param priorityWeights 权重表编码（"dc=w,dc=w"）；可为 null
     */
    public void setPrioritySnapshot(long priorityEpoch, String priorityWeights) {
        this.priorityEpoch = priorityEpoch;
        this.priorityWeights = priorityWeights;
    }
}