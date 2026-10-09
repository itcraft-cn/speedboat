package cn.itcraft.speedboat.rpc;

/**
 * PreVoteRequest 类（Phase C 预投票协议消息）。
 *
 * <p>预投票（pre-vote）解决"分区分区恢复期任期震荡"问题：
 * 候选者先不递增任期地探测能否获得多数派支持；只有拿到足够
 * 预票后才进入正式选举（RequestVote，term+1）。</p>
 *
 * <p>被 probe 节点的 sticky 判定：若自己仍被"健康现任 leader"
 *（心跳新鲜）覆盖，不管请求 term 多大都拒绝预票——
 * 阻断被分区节点重连后以暴涨 term 打奔现任 leader 的路径。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class PreVoteRequest extends RpcRequest {

    /** 预投票探测的目标任期（当前任期+1，不落盘不生效） */
    private long term;

    /** 发起预投票的候选者节点 ID */
    private String candidateId;

    /** 候选者本地日志的最后索引（选举安全校验用） */
    private long lastLogIndex;

    /** 候选者本地日志最后索引对应的任期 */
    private long lastLogTerm;

    /**
     * 候选者所在机房标识（跨机房级联用）。
     *
     * <p><b>注意事项：</b>必须追加在类的<b>末尾</b>——Protostuff {@code RuntimeSchema}
     * 按字段声明顺序分配 field ID，追加可保持线格式兼容，插入中间会导致字段 ID 错位。</p>
     */
    private String datacenter;

    public PreVoteRequest() {
        super();
    }

    public PreVoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        super();
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    /**
     * 带机房标识的构造（跨机房级联预投票用）。
     *
     * @param term        预投票探测的目标任期
     * @param candidateId 候选者节点 ID
     * @param lastLogIndex 候选者本地日志最后索引
     * @param lastLogTerm  候选者本地日志最后索引对应任期
     * @param datacenter   候选者所在机房标识；单机房场景可传 null
     */
    public PreVoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm, String datacenter) {
        this(term, candidateId, lastLogIndex, lastLogTerm);
        this.datacenter = datacenter;
    }

    public long getTerm() {
        return term;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    /**
     * 候选者所在机房标识。
     *
     * @return 机房标识；旧版本节点发出的请求或单机房场景下为 null
     */
    public String getDatacenter() {
        return datacenter;
    }
}
