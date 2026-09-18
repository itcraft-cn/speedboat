# 分项设计 02：持久化三档（RaftStore）与检查点

- 编号：2026-09-18-02
- 状态：已实现（提交 e8d307f / 0866889 / 619c6d9；真网复核 defect-20260918-01 关闭）
- 关联：总设计 [2026-09-18-00](2026-09-18-00-overall-design.md)、缺陷报告 docs/analysis/defect-20260918-01-log-truncation-epoch-divergence.md
- 用户拍板：三实现 Nop/Mem/Mmap，**默认 Mem**；Mmap 128MB 单文件 mmap WAL"丢了挂回"；快照阈值 1024 条；配置"默认优先、最小化配置"
- **P4-M5 收敛（2026-09-18 追加拍板）**：比对代价后生产缺省由 mem 改 **mmap**——写路径两者同数量级（低频锁命令均无感），但跨进程重启安全性 mmap 严格占优（mem 重启丢 term/votedFor=重复投票风险、丢锁 epoch）；因此：门面缺省=mmap，`raft.persistence=mem` 降级为显式无盘档（沙箱/容器 ephemeral），库级 Builder 缺省保持 Nop（零副作用）。InMemoryRaftStore 类保留不删（公共 API + 测试利用）

---

## 1. 三档定位与选型理由

| 档 | 实现 | 定位 | 重启语义 |
|----|------|------|----------|
| `NopRaftStore` | Nop | 测试基线 / 显式关闭 | 全空（行为与历史版本一致） |
| `InMemoryRaftStore`（显式档，原默认） | TreeMap 镜像 + 读写锁 | **进程内**可恢复（raft node 重启语义）；无磁盘；**跨进程重启丢 term/epoch（重复投票风险）** | 无盘沙箱/容器 ephemeral |
| `MmapRaftStore` | 单文件 mmap | **跨进程重启可挂回**，锁/epoch 保真 | 极稳定环境 |

选型铁律：三档实现同一 `RaftStore` 契约，算法层（RaftNodeImpl）零感知；档位切换不改变任何共识语义。这也是"重启副本 epoch 边界根治"的结构基础——判定的重建只能来自 pair（WAL 日志 + 状态机检查点），不依赖任何档位差异。

## 2. RaftStore 契约（含本分项新增口）

```
persistAndFlushTerm(term, votedFor, leaderId)   // 铁律：原子 + 返回后内存才可见
persistLogEntries(List<LogEntry>)               // "调用方全量列表"语义（见 §4）
truncateLogEntriesFrom(from)                    // leader 冲突截断
truncateLogEntriesUntil(until)                  // 快照后前缀收缩（预留）
deleteSnapshotChunks() / flush()
restoreTerm() -> RaftTermRecord
restoredLogEntries() -> List<LogEntry>
getCheckpointFile() [default null]              // 本分项新增：状态机检查点路径暴露口
```

- **无 RaftStore 由算法层再分配内存**（内存镜像 + 谁恢复语义）；RaftNodeImpl 构造缺省装配 Mem（`builder.raftStore` 未指定时回 InMemoryRaftStore）；
- 写侧只允许 raft 单线程调用（契约铁律）；restore* 仅启动路径一次。

## 3. MmapRaftStore：单文件挂回形态（按 128MB 拍板）

**文件布局（全部大端序，复用 CustomSerializer 帧格式 CRC 惯例）**：

```
[0,128)     term 槽位 0：{magic=0x52464156, version=1, seq, term, votedFor(≤48B), leaderId(≤30B), CRC}
[128,256)   term 槽位 1：双槽交替写；读侧取 seq 最大且 CRC 有效者（防半写撕裂）
[1024,+∞)   顺序 WAL：bodyLen(4) | crc32(4) | body
```

**WAL 帧 body（type 路由）**：

| type | 帧 | 语义 |
|------|-----|------|
| 0 | LEADER_INFO：idx/term/leaderId | LeaderInfo（占位） |
| 1 | MEMBER_CHANGE：op(ADD/REMOVE)/nodeId/address + idx/term/leaderId | 成员变更恢复 |
| 2 | COMMAND：idx/term/leaderId/data | 锁命令等业务载荷（保真） |
| 3 | TRUNC_FROM：index=fromIdx | 扫描时裁去 [idx, +∞)——follower 冲突收口 |
| 4 | TRUNC_UNTIL：index=untilIdx | 快照后前缀收缩 |

**term 槽铁律**：`persistAndFlushTerm` 写完双槽之一的 `{magic,version,seq,term,votedFor,leaderId,CRC}` 后必须 `mmap.force()`（msync）——**选主安全性优先，绝不丢**；翻转由 `seq++`、`slot = seq & 1` 驱动；恢复扫描取"seq 最大且 CRC 合法"的槽。term 写仅在选主/投票变化时发生，force 频次可接受。

**日志 append 语义**：page cache 承载（写消耗等同内存），`flush()` 精确点（节点 stop / 检查点 / 测试）才 `force()` 全区域；未 msync 尾部断电可丢——raft 未提交尾部丢失属安全域（客户端重试）。

**容量策略（P4-M4 已闭环）**：软阈值 = 区域剩余不足 1/4 时触发**压实回收**——若已上报检查点水位（`markCheckpoint(appliedIndex)`，由 RaftNode 在检查点落盘 + flush 后调用），则把索引 > 水位 的日志帧原地重写到区域起始（`compactNow`），丢弃已被检查点覆盖的前缀，随后 `force()`。压实语义与"检查点(≤floor) + WAL(>floor)"恢复等价。无检查点水位时仅告警不回收（等待首个检查点）；压实后仍不足才丢帧并告警（内存日志不受影响，重启由 Leader 重同步）。

## 4. "列表即全量" reconcile（关键语义——对齐 InMemoryRaftStore）

`persistLogEntries(entries)` 的列表被 RaftNode 以"当前全量尾列表"传入（正常 propose 单条 / follower 冲突重建多列表）。WAL 需与**列表完全对齐**：

收口规则（防旧帧复活）：

1. 若 `entryIndex.lastKey() >= listFirstIndex` → 先写 `TRUNC_FROM(listFirstIndex)` 标记帧 + 内存镜像 `tailMap(listFirst).clear()`——term 冲突的旧帧必须被融化，否则重挂扫描会复活旧 term 条目（defect-20260918-01 同族缺陷，真网已覆盖测试）；
2. 循环写入列表中缺失的条目（同索引已存在则幂等跳过）；
3. 尾部高于 `listMaxIndex` 的残留（follower 缩短列表）→ 再写 `TRUNC_FROM(listMaxIndex+1)`。

`truncateLogEntriesFrom/Until` 亦写标记帧（自我描述的收口），扫描按帧序收口——**WAL 是自描述的重放源**，这与"内存镜像"双写对齐。

## 5. 状态机检查点（LockStateMachine）

- 序列化布局（自描述、防半写）：

```
crc32(4) 覆盖 payload
payload = magic(0x4C4B4350) | entryCount | {name UTF | holderFlag 1 | holder UTF |
          holdCount 4 | leaseExpireTime 8 | epoch 8}* | lastAppliedIndex(8)
```

- 原子替换：`{ckpt}.tmp` 写成 + `FileChannel.force` + `Files.deleteIfExists(target)` + `renameTo(target)`；
- 恢复：CRC mismatch / magic mismatch → 任 by 空白启动（宁可无主，不脏挂起锁表）；
- 检查点文件统一 `{dir}/state-machine.ckpt`，由 `RaftStore.getCheckpointFile()` 暴露（Mmap 档 path；Nop/Mem 返回 null → 检查点跳过）；
- 触发：
  - `RaftNodeImpl.applyCommittedEntries()` 后判定 `lastApplied - lastCheckpointApplied >= 1024` → `stateMachine.snapshot(path)` + `raftStore.flush()`；
  - 优雅停机（`RaftNodeImpl.shutdown()`）在 executor 关停前一次性 snapshot（若增量存在）+ flush —— "均有最顺手锚点"；
- `LockEntry.restoreFromCheckpoint(holder, holdCount, lease, epoch)`：绕过判定直达持有态（快照即"当机时刻合法权益"）；epoch/租约忠实回写，**leaseExpireTime 为墙钟值**，重启后若已过期按过期处理（安全方向：宁可无主）。

## 6. 读回链（RaftNodeImpl.start），defect-20260918-01 的根治点

启动次序（全在 start raft 线程）：

```
① restoreTerm()  —— term/votedFor/leaderId（Nop 时 null）
② restoredLogEntries() —— WAL（或 mem）重建 log/logIndexMap；entryType 精确回填
                         （COMMAND 经 CommandLogEntry.create 保 data；MEMBER_CHANGE 经 MemberChangeEntry.add/remove；
                          LEADER_INFO 经 setEntryType 回填）
③ getCheckpointFile() 非空 → stateMachine.restore(path)，lastApplied 跳到
   min(smLastApplied, lastLogIndex)（超越日志则告警丢弃，退回全量重放）
④ commitIndex 仍由 Leader 心跳 leaderCommit 推进 → applyCommittedEntries 从 lastApplied 全序
   重放 → 锁表/epoch/滑动判定确定性重建
shutdown()：先 snapshot(checkpoint)（支持档）→ flush() → executor 关停
```

Speedboat 门面（两模式共用 `buildRaftStore(config)`）：档位解析 + `raftStoreRef` 保存 + 停机 `MmapRaftStore.close()`（msync 收口）；`raft.persistence` 键值映射 `{none, mem, mmap}`，dir 默认 `speedboat-data/{nodeId}/`，文件名支持 `{nodeId}` 占位。

**防御性注意（maxLogSize 与 WAL 的交互口径）**：内存日志与 WAL 是两套裁剪源——
- 内存日志 `maxLogSize` 走**双安全路径**：未提交积压（含 follower）/ 已提交前缀（仅 Leader 且全员 matchIndex 覆盖）。`raft.max.log.size` 不决定持久层内容；
- **WAL 不随内存裁剪**，只由"检查点水位 + 容量压力压实"回收前缀；因此 WAL 条目数可多于内存日志条数；
- 重启恢复 = `检查点(≤floor)` + `WAL(>floor)` 重放；若 WAL 中也缺失（压实后仍不足、丢帧告警），新上任/回归节点由 Leader 的 `AppendEntries` 按 nextIndex 补齐（这是把"日志长度逐差"当正常协同路径的设计，也是没有 INSTALL_SNAPSHOT 时的兜底）。

## 7. 验收（defect-20260918-01 关闭记录）

- 缺陷was：Leader 自 restart 后 log 截断（无 ARCHIVE）→ follower 冲突 indiscriminate 追 root cause = WAL 空洞即 apply 漂移 → epoch 分叉；
- 本分项修复后真网（loopback 三 JVM + mmap 档，32MB 测试件）：
  - 三锁并存 → kill holder 节点 → **同身份重启** → 日志锚点 `restored term` + `rebuilt log entries=98`；
  - restart 副本 acquire 老名目：**epoch=1（重入语义正确）**，长驻侧一致；
  - kill 兼 Leader/Holder → 迁移接管 epoch=2：**重启副本与长驻副本一致**；
- 单元：MmapRaftStoreTest 6 条（term 双槽 / COMMAND 保真 / follower 冲突收口 / 坏帧收敛 / 检查点路径 / MEMBER_CHANGE codec）+ LockStateMachineCheckpointTest 3 条（round-trip / 缺失回退 / CRC 破坏→空白）；
- 全量 519 绿。

## 8. 演进边界与已知容量核算（非本期）

- **WAL 前缀回收（已落地 P4-M4）**：`markCheckpoint` 水位 + 容量压力压实（`compactNow`）联动；压实与检查点次序铁律=先快照落盘+flush → 再上报水位（顺序颠倒会导致回收后无可恢复锚点）；
- **内存日志裁剪（已修正 P4-M4）**：`maxLogSize` 双安全路径——①未提交积压（含 follower，Leader 会重发）②已提交前缀（仅 Leader 且所有 peer matchIndex 已覆盖，或单节点)；修正前"遇已提交即 break"导致已提交区域永不裁剪；
- INSTALL_SNAPSHOT RPC（落后副本快照追平）不做——读回链已让副本自建；
- **fsync/durability 边界**：force 点 = term / 检查点 / 停机 / 压实；已提交但未 force 的尾部在全体同时断电下可能回退（表现为零持有者→安全方向）；power-loss 级保证需提交后 force 或 WAL fsync 策略，列为演进项；
- 磁盘 IO 契约：mmap 档 force 只在上述精确点；page cache ✓；raft 单线程写、启动单线程读——无锁。
