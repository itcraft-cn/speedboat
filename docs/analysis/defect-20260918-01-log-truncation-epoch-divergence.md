# 缺陷报告：maxLogSize 截断 + 无快照/持久化 → 重启副本 epoch 分叉

- 编号：defect-20260918-01
- 级别：P1（正确性：fencing token 前提破坏；但在"NopRaftStore 默认 + 无快照"阶段内已文档化的退化范围）
- 发现：2026-09-18 真实网络三节点测试（本机 192.168.30.39 使用 33001 端口 + 174/176，本机 → 174/176 连通性验证通过）
- 涉及：tests/NamedLockNode（真实网络驱动 harness，sample 测试代码）

## 现象

三节点集群：forex/metals/commodity 三把锁三方持有并存，全网收敛（epoch 各自=1）。kill forex 持有者（本机,非 Leader 转发路径）→ 30s 租约到期 → 接管者（本机 33001 重启进程）acquire 成功且输出 epoch=1；而长驻 Leader 副本（176）对同一 LOCK 命令在含续期历史的锁表上 grant，其 epoch=2。**两份副本对同一所有权变更给出不同 epoch**——epoch 单调性在"重启副本"侧不成立。

## 根因链（真实网络复核后修订）

1. `DEFAULT_MAX_LOG_SIZE = 10`（SpeedboatConsts）：30s 租约 / 15s 续期节奏下，仅 ~2.5 分钟日志即翻过 10 条上限——历史不可信重建；
2. 本质根因：**重启副本的状态机没有重建路径**（lockTable/lastApplied 空起步、lastApplied 无恢复），完全依赖 Leader 的 AppendEntries resync；被截断日志（方案 A 已缓解到 4096）或长续期产生的大历史仍可能超出重放窗口；
3. 后果验证：同一"迁移后的 LOCK"apply 到——长驻副本 = epoch 1→2（174 侧日志：`Lock acquired: forex by ...-33001 (epoch=2)`，10:41:48.706）；重参与副本（进程重启、completion from zero）= epoch 0→1。**两份副本对同一所有权变更结论分裂**；
4. epoch 单调性的"副本重启"边界由此破坏，fencing token 跨"曾重启副本"不可比。

## 破坏的语义

- `LockHandle.getEpoch()` 的跨副本单调性（G5 / 2026-09-18-02 设计 §3.4）在"副本重启（无快照/持久化重建）"边界失效。
- 消费侧"同名目 epoch 单调拒旧"依赖全网 epoch 一致；分叉后由持有者各自发出的 token 在异机重启方不再可比。

## 修复方案矩阵

| 方案 | 有效性 | 成本 | 定位 |
|------|-------|------|------|
| A. 提高 maxLogSize 默认值（如 10 → 4096） | 冻结生命周期内基本消解（每锁 15s 一条 renew，4k 条≈16h 内存日志壁） | 极低（一行常量 + 现有 truncateLogIfNeeded 逻辑） | **本期采纳**（产品默认值校正，非算法改动） |
| B. Speedboat 配置项 `raft.max.log.size` 接 Builder.maxLogSize | 运维级可调 | 低 | 本期顺带（装配点一行） |
| C. 状态机快照（InstallSnapshot / periodic checkpoint） | 根治：重启副本先恢复快照后才可参与判定 | 中（Phase 快照协议） | 已记载为 P4 演进项（原设计 §10 已列） |
| D. RaftStore 真持久化（WAL）接入 | 根治（日志持久恢复） | 中（persistence/RaftStore 契约已前置，缺文件实现） | 已列 P4 演进项 |

契约修正（文档层）：epoch 保证的网络边界从"任意重启副本"收紧为"不重启的副本 + 快照重建后的副本"；MANUAL 声明"状态机快照/持久化上线前，重启节点上的锁表以最新日志重放为准，重启副本的 epoch 以日志重放重建时确定性一致；超出保留窗口的历史不可重放"。

## 相关既有缺陷关联

- 该根因与"Phase D RaftStore 接口前置（b99b52c）"一致：持久化接口已就绪、文件实现在清单上而未接入；快照属已知已知项（MEMORY "后续可选：快照/INSTALL_SNAPSHOT"）。
- 单机不涨的场景（3 节点 + 3 锁 + 15s 续期）通过 A 即可守住无分叉；投放生产前必须至少 C 或 D 其一。

## 建议

1. 本期：A（默认值 4096）+ B（配置暴露 raft.max.log.size）——已落地；长驻副本内 epoch 单调经真实网络验证成立；
2. 排期：快照（C）作为 P4 首项，持久化（D）随 WAL 接入；重启副本参与判定前必须有快照/持久化恢复，否则其锁表从零、epoch 计数全新，不可信；
3. 真网验证口径：迁移 grant 的 epoch 一致性以"长驻副本"为准（已取直接证据：epoch 1→2 且长驻副本一致）；fresh 重启副本在 C/D 落地前不参与 epoch 断言。
