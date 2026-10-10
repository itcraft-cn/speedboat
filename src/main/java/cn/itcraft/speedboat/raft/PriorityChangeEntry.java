package cn.itcraft.speedboat.raft;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 机房优先级变更日志条目（阶段五人工升级 API）。
 *
 * <p>承载一次优先级表变更的完整信息：新的 {@code epoch}、变更后的完整权重表、
 * 来源（配置派生 / 人工提升）、被提升的机房、操作者与原因。作为父组
 * {@code PRIORITY_CHANGE} 日志条目经 Raft 复制，使可达 peer 间对优先级表达成强一致。</p>
 *
 * <h3>WAL 帧编码</h3>
 * <p>与 {@code CommandLogEntry} 同款——把结构化字段序列化进 {@code data} 字节，
 * 复用通用"leader + data"帧布局（type|index|term|leaderLen|leader|dataLen|data），
 * 避免为本类型新增专用帧结构。编码为单条 UTF-8 文本，字段以 {@code ;} 分隔、
 * 权重表以 {@code ,} 分隔机房、{@code =} 连接机房与权重。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
public final class PriorityChangeEntry extends LogEntry {

    private static final org.slf4j.Logger logger =
        org.slf4j.LoggerFactory.getLogger(PriorityChangeEntry.class);

    /** 变更来源标记（与 {@code DatacenterPriorityTable.Source} 同名，避免 WAL 依赖策略层） */
    public static final String SOURCE_CONFIG = "CONFIG";
    public static final String SOURCE_PROMOTED = "PROMOTED";

    private final long epoch;
    /** 变更后的完整机房权重表（不可变） */
    private final Map<String, Integer> weights;
    /** 来源：{@link #SOURCE_CONFIG} / {@link #SOURCE_PROMOTED} */
    private final String source;
    /** 被提升的机房标识；回退场景为 null */
    private final String targetDatacenter;
    /** 操作者（审计必填） */
    private final String operator;
    /** 变更原因（审计） */
    private final String reason;

    private PriorityChangeEntry(long index, long term, String leaderId, long epoch,
                                Map<String, Integer> weights, String source,
                                String targetDatacenter, String operator, String reason) {
        super(index, term, leaderId, EntryType.PRIORITY_CHANGE, null);
        this.epoch = epoch;
        this.weights = weights == null
            ? Collections.<String, Integer>emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<String, Integer>(weights));
        this.source = source == null ? SOURCE_CONFIG : source;
        this.targetDatacenter = targetDatacenter;
        this.operator = operator == null ? "" : operator;
        this.reason = reason == null ? "" : reason;
        // 结构化字段同时编码进 data 字节，复用通用 WAL 帧布局
        setData(encode().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 构造一条优先级变更条目。
     *
     * @param index            日志索引
     * @param term             任期
     * @param leaderId         提案者（父组 Leader）
     * @param epoch            变更后优先级表版本号
     * @param weights          变更后完整权重表
     * @param source           来源（{@link #SOURCE_CONFIG}/{@link #SOURCE_PROMOTED}）
     * @param targetDatacenter 被提升机房；回退传 null
     * @param operator         操作者（审计必填）
     * @param reason           原因（审计）
     */
    public static PriorityChangeEntry create(long index, long term, String leaderId, long epoch,
                                             Map<String, Integer> weights, String source,
                                             String targetDatacenter, String operator, String reason) {
        return new PriorityChangeEntry(index, term, leaderId, epoch, weights, source,
            targetDatacenter, operator, reason);
    }

    /**
     * 从 {@code data} 字节重建条目（WAL 读回 / 镜像链接用）。
     *
     * @param data 编码后的字节（{@link #encode()} 产物）
     */
    public static PriorityChangeEntry fromData(long index, long term, String leaderId, byte[] data) {
        return decode(index, term, leaderId, data);
    }

    public long epoch() {
        return epoch;
    }

    /** 变更后完整权重表（不可变视图） */
    public Map<String, Integer> weights() {
        return weights;
    }

    public String source() {
        return source;
    }

    /** 被提升机房；回退场景为 null */
    public String targetDatacenter() {
        return targetDatacenter;
    }

    public String operator() {
        return operator;
    }

    public String reason() {
        return reason;
    }

    /** 宽松解析：非法即 WARN 并返回 0（读回容错，不阻断 WAL 恢复） */
    private static long parseLongSafely(String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            logger.warn("PriorityChangeEntry decode: bad epoch value {}, fallback to 0", value);
            return 0L;
        }
    }

    /** 宽松解析：非法即 WARN 并返回 null（调用方跳过该项，与 PriorityCodec.decode 容错口径一致） */
    private static Integer parseIntSafely(String value) {
        try {
            return Integer.valueOf(value.trim());
        } catch (NumberFormatException e) {
            logger.warn("PriorityChangeEntry decode: bad weight value {}, item skipped", value);
            return null;
        }
    }

    /** 编码为 UTF-8 文本（字段以 ; 分隔；权重表 dc=w,dc=w） */
    private String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("epoch=").append(epoch);
        sb.append(";source=").append(source);
        sb.append(";target=").append(targetDatacenter == null ? "" : targetDatacenter);
        sb.append(";operator=").append(escape(operator));
        sb.append(";reason=").append(escape(reason));
        sb.append(";weights=");
        boolean first = true;
        for (Map.Entry<String, Integer> e : weights.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            sb.append(e.getKey()).append('=').append(e.getValue());
            first = false;
        }
        return sb.toString();
    }

    private static PriorityChangeEntry decode(long index, long term, String leaderId, byte[] data) {
        String text = data == null ? "" : new String(data, StandardCharsets.UTF_8);
        long epoch = 0L;
        String source = SOURCE_CONFIG;
        String target = null;
        String operator = "";
        String reason = "";
        Map<String, Integer> weights = new LinkedHashMap<String, Integer>();

        for (String part : text.split(";")) {
            if (part.isEmpty()) {
                continue;
            }
            int eq = part.indexOf('=');
            if (eq < 0) {
                continue;
            }
            String key = part.substring(0, eq);
            String value = part.substring(eq + 1);
            if ("epoch".equals(key)) {
                // 容错：epoch 非法时按 0 兜底（WAL 为自写内容，仅外部篡改/typically CRC 碰撞才会出现；
                // 读回失败不应让启动中断，见 review L7）
                epoch = parseLongSafely(value);
            } else if ("source".equals(key)) {
                source = value;
            } else if ("target".equals(key)) {
                target = value.isEmpty() ? null : value;
            } else if ("operator".equals(key)) {
                operator = unescape(value);
            } else if ("reason".equals(key)) {
                reason = unescape(value);
            } else if ("weights".equals(key)) {
                for (String pair : value.split(",")) {
                    if (pair.isEmpty()) {
                        continue;
                    }
                    int weq = pair.indexOf('=');
                    if (weq > 0) {
                        Integer weight = parseIntSafely(pair.substring(weq + 1));
                        if (weight != null) {
                            weights.put(pair.substring(0, weq), weight);
                        }
                    }
                }
            }
        }
        return new PriorityChangeEntry(index, term, leaderId, epoch, weights, source, target, operator, reason);
    }

    /** 转义分隔符与控制字符（操作者/原因可能含 ; , = 及换行等字符） */
    private static String escape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace(";", "\\s").replace(",", "\\c").replace("=", "\\e")
            .replace("\r", "\\r").replace("\n", "\\n");
    }

    private static String unescape(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(i + 1);
                if (n == 's') {
                    sb.append(';');
                } else if (n == 'c') {
                    sb.append(',');
                } else if (n == 'e') {
                    sb.append('=');
                } else if (n == 'r') {
                    sb.append('\r');
                } else if (n == 'n') {
                    sb.append('\n');
                } else if (n == '\\') {
                    sb.append('\\');
                } else {
                    sb.append(n);
                }
                i++;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "PriorityChangeEntry{index=" + getIndex() + ", term=" + getTerm()
            + ", epoch=" + epoch + ", source=" + source + ", target=" + targetDatacenter
            + ", operator=" + operator + ", weights=" + weights + "}";
    }
}
