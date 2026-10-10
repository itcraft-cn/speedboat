package cn.itcraft.speedboat.strategy.voteweight;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 机房优先级权重表的<b>线格式编码/解码</b>（阶段五人工升级 API 传播载体）。
 *
 * <p>把 {@code Map<String,Integer>}（机房→权重）编码为单条 {@code "dc=w,dc=w"} 文本，
 * 供 {@code AppendEntriesRequest}/{@code RequestVoteRequest} 的可选字段携带。编码极简且
 * 不含分号/等号歧义（机房标识按约定不含逗号/等号），避免为传播引入嵌套对象的
 * Protostuff schema 风险——旧版本节点收到该字段为 null/0，接收方据此忽略。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
public final class PriorityCodec {

    private PriorityCodec() {
    }

    /**
     * 编码权重表。
     *
     * @param weights 机房→权重；null/空返回空串
     * @return {@code "dc=w,dc=w"}；空表返回空串
     */
    public static String encode(Map<String, Integer> weights) {
        if (weights == null || weights.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
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

    /**
     * 解码权重表（容错：非法项跳过，不抛异常——传播路径上不应因单个坏项中断收敛）。
     *
     * @param encoded {@code "dc=w,dc=w"}；null/空返回空 Map
     * @return 机房→权重；解码失败/空返回空 Map
     */
    public static Map<String, Integer> decode(String encoded) {
        Map<String, Integer> weights = new LinkedHashMap<String, Integer>();
        if (encoded == null || encoded.isEmpty()) {
            return weights;
        }
        for (String pair : encoded.split(",")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            if (eq <= 0 || eq == pair.length() - 1) {
                continue;
            }
            try {
                int w = Integer.parseInt(pair.substring(eq + 1));
                if (w > 0) {
                    weights.put(pair.substring(0, eq), w);
                }
            } catch (NumberFormatException ignored) {
                // 单项解码失败即跳过，保证传播路径健壮
            }
        }
        return weights;
    }
}
