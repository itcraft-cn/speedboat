# 代码审查报告：speedboat（增量审查）

## 项目概述

- **审查时间**：2026-10-10
- **项目语言**：Java 8（`<release>8</release>`）+ Bash 编排脚本
- **审查范围**：`965cce9..35a9cbd` 共 20 个提交、52 个文件、+8297/-101 行
  - 主代码：30 个文件（+约 4100 行 diff）
  - 测试：10 个文件；工具：4 个 bash 编排脚本；文档与 pom：8 个文件
- **审查模式**：增量（git diff），规则 = 通用框架 + `spec.java.md` / `review.java.md`，辅以 ast-grep 自动扫描
- **审查内容主题**：跨机房双层级联（父组/子组）、CP/AP 一致性双模式、同 IP 多进程身份钉定、阶段五人工升级 API（含本次"仅 CP 生效"AP 闸门）

## 审查结论（先行）

整体质量高：策略接口收敛决策点、席位门控三入口一致、权重数学注释链完整、
时钟回拨审查通过、新增行零硬性违例（无 System.out / printStackTrace / while(true) / 硬编码密钥）。
发现 **1 项必须修正**（三机房拓扑下提升公式数学失效）、4 项应当修正、若干低优先建议。

---

## 通用审查结果

### 安全问题

| 项 | 结论 |
|---|---|
| 硬编码敏感信息 | 无（ast-grep 扫描零命中；vbox 配置文件为纯测试环境参数） |
| System.out / printStackTrace | 新增行零命中（既有命中全在 `src/test/*/sample/` 旧样例，不在本范围） |
| SQL 注入 / XSS | 无数据库、无 Web 层，不适用 |
| 日志注入(CRLF) | **发现 M4**（见下）：`operator`/`reason` 自由文本未净化换行，可破坏单行审计契约 |
| 消息通道清单 | 无 MQ 中间件；跨进程通道即父组/子组 TCP 端点（`cross.port.offset`），双向都有生产/消费，无幽灵/孤儿通道 |

### 性能问题

- **P1（通过）**：AP 降级判定、check-quorum、选举超时全部使用 `System.nanoTime()` 差值计算，
  无时钟回拨风险（`review.java.md` 强制项审查通过）。
- **P2（通过，低档优化项 L8）**：epoch>0 后每条心跳/投票都会 `PriorityCodec.decode` + `equals(configWeights())`，
  字符串极小、convergeIfNewer 为 false 时零落盘，实测成本可忽略；可选缓存上次解码结果。
- **P3（通过）**：连通性闸门探测串行、单端点 1s 超时上限，`try-with-resources` 关闭 socket，无句柄泄漏。

### 代码质量问题

- **Q1（中）**：`Speedboat.java` 已达 **1241 行**，承担单机启动/级联编排/双层观测/人工切主/持久化收尾五种职责，
  呈上帝类趋势（详见 M1）。
- **Q2（中）**：`QuorumCalculator` 中"selfWeight + 机房去重聚合"模式在 4 个方法中重复（详见 M2）。
- **Q3（低）**：生产代码大量内联全限定类名 `cn.itcraft.speedboat...`（本次新增 30+ 处），应改 import（详见 L1）。

---

## 语言特定审查结果（Java 规则应用）

| 规则 | 结果 |
|---|---|
| 并发契约（Actor 单线程 + volatile 快照） | 通过：`DatacenterPriorityTable.current` volatile 不可变快照写读分离；`seatHeld` volatile；`ApConsistencyPolicy` 降级集合不可变快照 |
| 静态初始化交叉依赖（类初始化死锁） | 通过：新增类无 static 块跨类依赖；`CpConsistencyPolicy` 单例自持无环 |
| 线程创建 | 通过：生产代码无 `new Thread`（新增行零命中） |
| 资源关闭 | 通过：`PriorityStore` 全部 try-with-resources；socket 探测同 |
| 原子写 / 持久化完整性 | 通过：临时文件 + rename，rename 失败有"先删再试"兜底 |
| Protostuff 字段追加纪律 | 通过：`datacenter`/`priorityEpoch`/`priorityWeights` 全部追加在类末尾，且有 `DatacenterFieldCompatTest` 钉死 field ID 不变式 |
| 日志三参数异常形式 | 通过：`logger.warn("...file={}", file, e)` 形式正确 |
| 空 catch 块 | **发现 M3**（1 处，有注释无日志） |
| 方法长度(>50 行)/JIT 325B | 抽查通过：`doPromoteDatacenter` 约 45 行、编排分段清晰；`buildParentGroup` 约 130 行偏长（编rega排性质，可接受，随 M1 一并考虑） |
| 缩进 >4 层 | 未发现 |
| 魔法数值 | 基本通过：`+2` 权重步进为算法常量（有注释说明数学含义，可接受）；`1000` 端口偏移等有常量/配置 |

---

## 优秀设计

1. **`ConsistencyPolicy` 策略接口**：把"问策略该怎么办"的决策点收敛，
   CP 以 `excludedDatacenters()` 恒空集达成"逐字节等价"向后兼容，接口注释把取舍讲透。
2. **席位门控的一致性**：`doStartElection` / `doBecomeLeader` / `runElectionTimeout` 三入口同一不变式，
   并补上 `RaftNode.startElection()` 公开入口的 learner 防线，注释把"静默无主"的后果链讲清。
3. **投票授予闸门**（`shouldGrantVote` + 预票同口径）与"闸门必须放在 term 对齐之后"的顺序论证，
   直接封死"term 膨胀反向收编主机房"的缺陷路径。
4. **原子优先级收敛**：`convergeIfNewer` 只升不降 + 双传播载体（心跳/投票）+ WAL `PRIORITY_CHANGE` 强一致复制，
   三层写入口（人工提升 / apply 分派 / RPC 收敛）共享同一表实例。
5. **`PriorityStore` 独立文件持久化**：不污染 `RaftStore` 接口与契约测试；损坏文件返回 null 回退配置派生表，有专项单测。
6. **编排脚本**：`vm-test-lib.sh` 断言计数 + 结构化日志解析复用；分区脚本有 `cleanup()`，`set -uo pipefail`。

---

## 改进建议

### 必须修正（优先级：高）

#### H1 提升权重公式只在两机房拓扑下成立：`max(对侧)+2` 在 ≥3 机房时自投成主可能失败

- **位置**：`src/main/java/cn/itcraft/speedboat/raft/RaftNodeImpl.java:686`（`computePromotedWeights`）
- **事实**：公式取 `本房 = max(对侧) + 2`。两机房下 total = W_p + W_o ⇒ required = W_p，自投即成主，成立。
  但权重派生（`deriveWeightsByIndex`）与配置（`datacenter.weight.*`）均支持任意机房数。反例（对侧两房各 5）：
  `W_p = 7`，`total = 17`，`required = 9`，`7 < 9` —— `doStartElection()` 自投失败。
- **后果**：方法注释与 MANUAL 均宣称"自投即成主/双主在权重数学上不可能"；≥3 房时
  **换表与 term 跃升已经生效、且不回滚**，API 却返回 false 并打 ERROR——留下一个已跃 term 的不对称中间态，
  依赖下一次自然选举兜底。唯一性（不双主）在 ≥3 房仍成立（AP 剔除至两活跃房时亦然），
  但"提升必然达成目的"的承诺被打破。
- **建议**：公式改为 `本房 = Σ(其他房权重) + 1`（两机房下与 `max+1` 等价、当前 `max+2` 亦可统一修正）。
  验证：`total = 2·W_p - 1`，`required = floor(total/2)+1 = W_p` ⇒ 自投恰达标；对侧合计 `W_p - 1 < required` 无法成主。
  同时补一条三机房 `promote` 单测（模拟对侧 `5,5`，断言提升后 self ≥ required 且返回 true）。

### 应当修正（优先级：中）

#### M1 `Speedboat` 上帝类趋势（1241 行、五职责）

- **位置**：`src/main/java/cn/itcraft/speedboat/Speedboat.java`
- 单机启动 / 级联编排（`buildParentGroup`）/ 双层观测 / 人工切主（`doPromote*` + 审计）/ 持久化收尾（`raftStoreRefs`）混充同类。
- 建议：拆 `ParentGroupBuilder`（构造父组全家）与 `PromoteGateway`（校验+连通性闸门+审计），
  门面静态方法薄委托。下一迭代内完成即可，不阻塞当前功能。

#### M2 `QuorumCalculator` 聚合循环重复 4 次，且"已收票分支"未与分母同口径剔除

- **位置**：`QuorumCalculator.java:89`（`totalWeightByDatacenter`）、`:167`（`receivedWeightByDatacenter`）、
  `:260`（`freshWeight`）、`grantedWeight`、`matchedWeight` 五处同构"selfWeight + HashSet 去重"循环。
- 附带：分母侧（`totalWeightByDatacenter`）与分子侧（`freshWeight`）都已剔除 AP 降级机房，
  但 **`receivedWeightByDatacenter` 未调用 `isExcludedDatacenter`**——分子分母口径分裂的静态缺口
  （实际影响受限：降级评估先于自投且对侧失联不可能投票，但该缺口使论文级的"同口径"论证不完整）。
- 建议：抽一个 `aggregateByDatacenter(Set<String> eligiblePeers 或谓词)` 收敛四处循环，
  并在已收票分支补同一剔除判断。

#### M3 空 catch 块（违反"禁止异常吞没"）

- **位置**：`Speedboat.java:1176`（`captureAuditState` 内 `catch (Exception ignored)`，只有注释无日志）。
- 建议：降级为 `logger.debug/warn("term read failed for audit", e)` 即可，语义不变、审计可追溯。

#### M4 审计链路未净化换行符（CRLF 注入破坏单行结构化审计）

- **位置**：`Speedboat.java`（`auditPromote`，~:1237）与
  `PriorityChangeEntry.java:178`（`escape` 只转义 `\ , ; , =`）。
- **事实**：`operator`/`reason` 是调用方自由文本；含 `\r\n` 时 `PROMOTE-AUDIT` 单行被折断，
  "固定字段顺序便于 grep"的自身契约失效；`\n` 经 WAL 往返（escape 不处理换行）原样保留。
- **后果**：审计链是本 API 的核心价值（"完整操作留痕"），注入恰破坏审计本身。风险级别不高
  （调用方为可信运维进程），但修复成本极低。
- **建议**：`auditPromote` 打印前对四个文本字段做 `replaceAll("[\\r\\n]", "\\\\n")` 净化；
  `escape` 补充 `\r`/`\n` 转义（`\\r`/`\\n`）保持 WAL 往返无损。

### 建议改进（优先级：低）

- **L1** 本次新增/修改文件大量内联 FQN（`cn.itcraft.speedboat.strategy.consistency.ConsistencyPolicy` 等），
  虽与项目部分既有风格一致，但本批 30+ 处集中出现，建议统一改 import。
- **L2** `PriorityStore.load()` 借 `new DatacenterPriorityTable(weights, source).replace(...)` 跨包"搭桥"产出包内可见的 `Snapshot`——建议 `DatacenterPriorityTable` 提供公共 `Snapshot` 工厂，消除对 replace 语义的借用依赖。
- **L3** `PriorityStore.save()` 的 tmp 残留清理只覆盖 `IOException` 路径；建议 finally 统一清理。
- **L4** `doStop()` 拆卸序（`parentRaftNode = null` 先于 `running = false`）与静态观测 API 之间存在窄竞态：
  并发的 `promoteDatacenter` 会以 `not-cross-datacenter-mode` 为因 REJECTED，cause 语义失真（不影响正确性）。可先置 `running=false` 再拆件，或注释接受现状。
- **L5** `tools/vm/vm-partition-test.sh` 未注册 `trap cleanup EXIT`——中途 Ctrl-C 会残留 `vboxnet0 down` / host 侧 `lo` 地址分区态；建议补 trap。
- **L6** `HeartbeatWatchdog.markResponse`（`HeartbeatWatchdog.java` ~:38）对未登记 dc 的 peer（`""`）会误判为"对侧"并刷新 `lastOppositeSeenNanos`。实际影响约零（单机房 AP 恒无对侧；父组路径 dc 逐个登记），建议空串判同房处理。
- **L7** WAL 恢复路径 `PriorityChangeEntry.decode`（`Long.parseLong`/`Integer.parseInt`）无容错——CRC 前置校验使触发需"内容损坏且 CRC 碰撞"，概率可忽略；即便触发也是启动 fail-fast（可接受）。如追求完备，parse 包 try-catch 返回 CONFIG 空 table。
- **L8** 传播路径每次 RPC 重复 decode 权重表（可缓存"上次编码串→上次解码结果"），微优化、可不做。

---

## 总结

| 维度 | 评价 |
|---|---|
| 语义正确性 | 两机房主用例正确且经过 615 项测试；**≥3 机房拓扑的 promote 公式存在数学缺陷（H1）** |
| 并发安全 | Actor 单线程契约执行严谨，volatile 快照用得规范 |
| 资源管理 | try-with-resources 贯彻，唯一残留是 stop 拆卸序的窄竞态（L4） |
| 可观测性 | 审计/降级/席位/收敛日志结构完整——恰好 M4 揭示审计行自身可被注入破坏 |
| 测试 | 新增多组专项测试：`PriorityPromoteTest` 16、`ConsistencyPolicyTest` 14、`ParentGroupQuorumTest` 12、`DatacenterFieldCompatTest` 7、`SpeedboatManualSwitchGateTest` 3 等；AP 忽略/CP 放行、WAL 编解码、字段兼容、席位门控全覆盖；缺三机房 promote 用例（随 H1 补） |
| 脚本与文档 | 编排库复用良好；bash 部分缺 EXIT trap（L5） |

**关键风险**：H1 在两机房部署（当前唯一在用拓扑）下不触发；一旦用户按配置能力开出三机房，
人工提升 API 会在最需要它的场景（主机房全灭）下静默失效并留下变异的 term/优先级表。

**实施建议**：H1 + M3 建议本迭代修复（合计改动约 20 行 + 1 个三机房单测）；M2/M4 随下次触达该文件时顺带修复；L 级按需。

---

**审查工具**：ast-grep-mcp 自动扫描 + OpenCode（GLM-5.3-Flash）
**规则来源**：${AI_SPEC_ROOT}/lang-spec/spec.java.md、${AI_SPEC_ROOT}/lang-spec/review.java.md
**审查完成时间**：2026-10-10
