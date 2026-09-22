# Speedboat 对比四个老牌 Java Raft 实现

> 日期：2026-09-22
> 数据来源：四个项目各自的 docs 分析报告（assessment/deconstruct/detect，2026-09 同批产出）+ speedboat 自身 README/CHANGELOG/评估报告
> 对比对象：
> - [raft-java](/home/helly/open_source/github/raft-java)（个人教学级实现，B+ / 51 分两评）
> - [Apache Ratis](/home/helly/open_source/github/ratis)（Apache 顶级项目，7.75/10）
> - [SOFAJRaft](/home/helly/open_source/github/sofa-jraft)（蚂蚁集团，81.65/100）
> - [MicroRaft](/home/helly/open_source/github/MicroRaft)（Hazelcast 系纯库，8.125/10）
> - Speedboat（本轮自评依据：20260913 定性评估 + 2026-09 两次 CHANGELOG 实测数据）

---

## 一、标题结论（先说透）

1. **不在同一赛道，但同一条算法地基上**：四个对比项目全是"复制状态机 / 复制日志"定位，唯一把"主节点选举 + 命名锁"做成 API 的只有 Speedboat。**这是差异，不是缺陷**——但差异必须配得上"极简"二字，否则就只是欠缺。
2. **算法特性面，Speedboat 已完成一轮 MicroRaft 对标**（Actor 化执行器、PreVote、leader stickiness、check-quorum、RaftNodeReport、RaftStore 三档），在"选主正确性特性"上与 MicroRaft 处于同一梯队，**领先 raft-java 半代，落后 SOFAJRaft/Ratis 一代**（后者有成员变更 joint consensus、快照安装、线性一致读）。
3. **工程成熟度是全线短板**：零认证（四家里 raft-java 也零认证，Ratis/SOFAJRaft/MicroRaft 至少有契约与测试覆盖太硬）。Speedboat 的护城河是 API 与心智，薄弱处在运维闭环与防御性工程。
4. **规模与代码健康**：Speedboat 80 文件主代码约 5k 行量级、RaftNodeImpl 1828 行（已膨胀为上帝类）、重复度 1.76%、519 测试全绿 —— 体量是四家最小，重复度是四家最低，但上帝类问题正在向 SOFAJRaft/Ratis 的老路滑。

---

## 二、五项目速览

| 项目 | 定位 | 规模（估） | 评估得分 | 成熟档位 |
|---|---|---|---|---|
| raft-java | 教学/原型 Raft 库（brpc-java + Protobuf） | ~2.5k 行 / RaftNode 1012 行 | B+（78）/ 51/100 两评分裂 | **教学级，禁产** |
| Ratis | Apache 顶级项目，可插拔复制日志库（gRPC/Netty 双传输，高吞吐 DataStream） | 16-18 Maven 模块 | 7.75/10，REFACTOR | 生产级（Ozone 等背书） |
| SOFAJRaft | 蚂蚁，MULTI-RAFT-GROUP 共识库 + RheaKV，从 braft 移植 | ~105k 行 / jraft-core ~50k | 81.65/100 | 生产级（蚂蚁/SOFABolt） |
| MicroRaft | 纯库（零网络零存储依赖，Actor 单线程，接口契约化） | 171 文件（125 main / 39 test） | 8.125/10 | 库级最优实践 |
| **Speedboat** | **主节点选举组件 + 命名锁，极简 API 一行启动** | **80 文件主代码 / RaftNodeImpl 1828 行** | **4/4.5/2/3/4.5/3.5/2（20260913）** | **实验级 → 工程组件演进中** |

---

## 三、能力矩阵硬对比

| 能力 | raft-java | Ratis | SOFAJRaft | MicroRaft | **Speedboat** |
|---|---|---|---|---|---|
| 两阶段选举（PreVote） | ✅ | —（未见文档） | ✅ | ✅ | ✅（2026-09 吸收） |
| leader stickiness | ❌ | — | ✅ | ✅ | ✅（2026-09 吸收） |
| check-quorum 主动降级 | ❌ | — | 未见专门文档 | ✅ | ✅（2026-09 吸收） |
| 加权选举 | ❌ | ❌ | ❌ | ❌ | ✅（**独有**：Even/Prefer/Datacenter/Composite） |
| 跨机房级联选举 | ❌ | ❌ | ❌（multi-group 不等于跨机房） | ❌ | ✅（**独有**：父子 Group + 双套超时） |
| 单线程 Actor 化 | ❌（brpc 同步 + 锁） | 部分（线程池分工） | Disruptor 双队列 | ✅（教科书级） | ✅（RaftNodeExecutor，2026-09 吸收） |
| 日志复制 | ✅ 逐条、无批量 | ✅ 批量+异步 | ✅ 批量 32/窗口 16 + Pipeline | ✅ 自适应批处理+背压 | ✅（无批量/流控，锁场景够用） |
| 日志回退提示 | ❌ | ✅ | ✅ | ✅ expectedNextIndex | 部分（逐条退） |
| 持久化/WAL | ✅ SegmentedLog+mmap | ✅ 可插拔 RaftLog | ✅ RocksDB+WAL+Checksum | ✅ RaftStore 抽象+SQLite | ✅ 三档（mmap 单文件 WAL / mem / none）（2026-09-18 落地） |
| 快照（成员状态机） | ✅ InstallSnapshot | ✅（手动触发） | ✅ 分块流式安装 | ✅ 分块 INSTALL + 并行拉取 | ✅ 检查点 snapshot/restore（crc+原子替换），无分块 |
| 成员变更 | ✅ 配置日志条目 | ✅ 显式 API | ✅ joint consensus + Learner | ✅ LEARNER | ✅ MemberChangeEntry（单步） |
| 线性一致读 | ❌ | — | ✅ ReadIndex + LeaseRead | ✅ quorum 读 + 租约读 | ❌（isMain 心智读，无 ReadIndex） |
| 分布式锁 | ❌ | ❌ | ❌（RheaKV 可上层组） | ❌ | ✅（**独有**：命名锁 + epoch fencing + 非抢占迁移） |
| 传输 | brpc-java，无 TLS | gRPC/Netty 双实现 + TLS | Bolt + 长连接池 | 刻意外置（Transport 两方法契约） | Netty 内置，无 TLS，契约已部分 Javadoc 化 |
| 序列化 | Protobuf 强类型 | Protobuf | Protobuf/Hessian2 可插拔 | 消息模型与序列化解耦 | Protostuff + 自定义帧 |
| 可观测性 | ❌ 零指标 | ✅ metrics-* 多实现 + JMX | ✅ Dropwizard 内置（缺 p99 类） | ✅ RaftNodeReport + Micrometer 外挂 | 部分：RaftNodeReport 钩子已落，指标未暴露 |
| 安全（TLS/认证） | ❌ | ✅ TlsConf 集中管理（9/10） | 95/100（代码层面） | ❌（**最大风险项**，与 Speedboat 同病） | ❌（协议要求可信网络，未声明假设） |
| 测试 | ❌ 覆盖率 0% | ✅ 高 + SpotBugs | ✅ Jepsen + 故障注入 | ✅ 故障注入（无 JMH） | ✅ **519 测试全绿 + 真网 loopback 三 JVM 验证** |
| 性能基准 | 无 | 有体系（性能 5/5） | 有 Benchmark 工具（>5 万 QPS 目标） | 无 JMH | JMH 依赖已列，尚未产出公开基准 |

**注**：Ratis 表格中"—"表示本项目 docs 内未呈现该项，不等于代码不支持；Ratis docs 侧重质量/重复度/运维评估，能力描述偏概念层。

---

## 四、逐项点评：各家能教 Speedboat 什么

### 4.1 从 raft-java 学：反面教材的清醒剂

- raft-java 是**四家里唯一与 Speedboat 同体量级别**的实现（约 2.5k 行 vs ~5k 行），两评分数撕裂（78 vs 51）恰恰证明"无测试即无法自证"。Speedboat 519 测试全绿这一项，已经越过它的天花板。
- 它的 Critical 五连（零测试、RPC 资源泄漏、snapshot 安装非原子、无认证、NPE）中，"快照安装非原子"值得 Speedboat 警惕：我们的 checkpoint 做了 crc + temp·rename 原子替换，**这个点已比它正确**。
- 结论：raft-java 处于被超越位置，仅保留其"PreVote 两阶段"与"SegmentedLog 分段"两个思想参照。

### 4.2 从 Apache Ratis 学：插件化边界 + 性能天花板

- Ratis 用 16-18 模块把 StateMachine / RaftLog / RPC / Metrics 全部接口化——这验证了 Speedboat "五套策略 + TransportLayer + RaftStore 三层抽象"的架构方向是**行业共识路径**，不是过度设计。
- Ratis 的 TLS 评分 9/10（TlsConf 集中管理）是四家里唯一把安全做满的；它证明 **TLS 归口一个配置类是可行的工程方案**，Speedboat 将来补认证可参照其形态而非重造。
- 反面教训更值钱：代码重复 14.38%、18 模块、100+ 配置项、运维全手动、"过度工程化"。Speedboat 当前 12 个配置项、重复度 1.76%，**"极简"正是我们对 Ratis 的差异化回答**——一旦配置面也膨胀到 100 项，差异化即归零。
- 它的运维自动性短板（快照/leader 转移需手动）与 Speedboat"SlaveNothing"问题同源：Speedboat 唯一比它好的一面是启动即懂（properties 即拓扑）。

### 4.3 从 SOFAJRaft 学：工业化上限 + 内存纪律

- 四家里工业完成度最高：joint consensus、Pipeline 复制、ReadIndex/LeaseRead、快照分块流式安装、Jepsen 验证、Disruptor 双队列、Bolt 长连接池。**这是一条完整的"复制状态机"路线图，Speedboat 不必追**——追齐即变成第二个 jraft。
- 真正该抄的是两件"纪律"：
  1. **内存安全隐患清单**（Closure/ByteBuffer/RequestMap 未清理是它的定级缺陷）：Speedboat 锁状态机的 request-id 去重表、失败记录表 FailureRecord、长驻 member 表——每个都应该配上界与驱逐；这是 SOFAJRaft 用 24h 压测换来的教训。
  2. **语义完备的线性读**：Speedboat 的 `isMain()` 是进程内观点读，跨节点调用有假阳性窗口。若锁场景出现"远端先验主"诉求，ReadIndex 是必须项，SOFAJRaft 的 LeaseRead 是流量友好版参照。

### 4.4 从 MicroRaft 学：契约化的终点（已完成大半）

- Speedboat 2026-09 已按《microraft-experience-adoption》中策吸收：RaftNodeExecutor 单线程化、check-quorum、PreVote/stickiness、RaftNodeReport、RaftStore 抽象、Transport 契约化雏形。**这一轮对标已基本兑付**。
- 剩余高价值未吸收项（C 级留档）：expectedNextIndex 一步回退、Firewall 故障注入测试框架、LEARNER 灰度。
- 盲区警示（各家 documents 复核后确认）：MicroRaft 同样无 TLS/认证、无 JMH——抄它不等于抄安全与性能，这两个维度 MicroRaft 也不比 Speedboat 先进多少（其可比优势在契约与单线程）。

### 4.5 四家横向定排名（以 Speedboat 视角）

| 参照维度 | 最强 | 最弱 |
|---|---|---|
| 算法完备性 | SOFAJRaft > MicroRaft > Ratis > raft-java | |
| 工程契约/接口设计 | MicroRaft > Ratis > SOFAJRaft > raft-java | |
| 性能工程 | Ratis ≈ SOFAJRaft > MicroRaft > raft-java | Speedboat 未测 |
| 可观测性 | Ratis > SOFAJRaft ≈ MicroRaft > raft-java | |
| 安全 | Ratis > SOFAJRaft > MicroRaft ≈ raft-java ≈ **Speedboat**（垫底梯队） |
| API 极简度 | **Speedboat（独一档）** > MicroRaft > SOFAJRaft > Ratis > raft-java |
| 专题广度（跨机房/权重/锁） | **Speedboat（独一档）** | 其他四家均无原生支持 |

---

## 五、差距与行动映射（从对比收敛到 speedboat 路线）

把对比结果翻成判据（按 AGENTS.md "上/中/下三策" 叙事法）：

### 共性差距（相对四家的共有短板）

| # | 差距 | 对标参照 | 残留风险 |
|---|---|---|---|
| 1 | 传输层零认证/零 TLS | Ratis(TlsConf)、SOFAJRaft | 任何可触达端口的主机可灌投票/日志/锁操作 |
| 2 | 可观测性未闭环 | Ratis(metrics-*)、SOFAJRaft(Dropwizard)、MicroRaft(RaftNodeReport→Micrometer) | 钩子已在（RaftNodeReportListener），缺 Sink 与 dashboard |
| 3 | RaftNodeImpl 1828 行上帝类 | MicroRaft handler/task 分层（SOFAJRaft 同有此病） | 修改半径、回归成本持续上升 |

### 按 ROI 的三档改造成次

**下策（~1 人日，正确性兜底）**
1. 结构化异常三件套（NotLeaderException/CannotReplicateException），锁 API 从布尔/异常字符串升级为可程序化重试协议；
2. maxFrame 上限 + 每对消息 INFO→DEBUG 降噪；
3. 状态机去重表（LockOp request-id 表 / FailureRecord）补上界与驱逐策略（抄 SOFAJRaft 教训）。

**中策（3~5 天，推荐 ⭐）**
4. RaftNodeReport → Micrometer 外挂模块（独立 artifact，核心零依赖，抄 MicroRaft 形态）；
5. AppendEntriesFailureResponse 增加 expectedNextIndex 一步回退（复制路径修正确实性）；
6. handler/task 从 RaftNodeImpl 拆出（拆 MicroRaft 的 3 方法 executor 契约已就位，此次拆的是"类"不是"线程"）。

**上策（8~12 天，进入下一定位）**
7. TLS/认证归口设计（抄 Ratis TlsConf 管控面 + MicroRaft Transport 接口层收口）——在此之前 speedboat 只能声明"私网可信部署假设"，这一点必须写进 README 假设栏；
8. ReadIndex/LeaseRead（抄 SOFAJRaft），为锁的"远程先验主"场景铺路；
9. 异常通道统一错误码体系（当前 Barrier 式 IllegalStateException 与四家的差距被评估报告点名为唯一开发侧不满）。

### 长线定位建议（非今天做）

- 不要向"通用复制状态机"漂移——SOFAJRaft/Ratis 已用 10 万行把这条路铺满，Speedboat 约 5k 行不可能在其赛道上取胜；
- Speedboat 的可防守阵地是**"跨机房选主 + 命名锁 + 极简接入"**这一个细分：四家均无原生跨机房级联、加权选举和"日志共识即锁"的语义，这是独立价值；
- 快照/批量复制/成员变更复杂度，只在"锁日志"成为高频路径时才升级——目前 4096 日志上限 + 检查点已够用，避免 Ratis 式过早工程化。

---

## 六、结论（一句话版）

**Speedboat 不是四家里的"更小版本"，而是"另一个物种"——它用约 1/20 的代码量换来了四家都没有的跨机房选主 + 权重选举 + 命名锁。但这个物种要活到生产，欠的不是算法（已与 MicroRaft 对齐），而是四家共性给了反面教材的三样工程底盘：TLS/认证、可观测性闭环、错误协议化。中策（3~5 天）即可把这三样中最便宜的两样（可观测性、错误协议化）补上，同时借 SOFAJRaft 的内存纪律把状态机表加上界；剩余的家底（ReadIndex、TLS）确认需求后按上策分批上。**

---

## 附：对比使用的数据文件清单

| 项目 | 本轮引用文档 |
|---|---|
| raft-java | docs/full-analysis-report.md、docs/assessment/qualitative-assessment-20260922-001.md、docs/deconstruct/design-document.md、docs/deconstruct/module-design.md、docs/detect/problem-report.md |
| Ratis | docs/assessment/qualitative-assessment-20260922-001.md、docs/detect/detect-20260922-01.md、docs/refactor/refactor-20260922-01.md、docs/rebuild/rebuild-20260922-01.md |
| SOFAJRaft | docs/assessment/qualitative-assessment-20250922-001.md、docs/detect/detect-20250922-001.md、docs/deconstruct/01-ARCHITECTURE_SUMMARY.md、docs/deconstruct/03-ALGORITHMS.md、docs/deconstruct/06-NETWORK_ANALYSIS.md、docs/refactor/refactor-20250922-001.md |
| MicroRaft | docs/assessment/qualitative-assessment-20260922-001.md、docs/detect/architecture-20260922-001.md、docs/detect/detect-20260922-001.md、docs/review/code-review-20260922-000-java.md、docs/deconstruct/README.md |
| Speedboat（自评基线） | README.md、CHANGELOG.md、docs/assessment/qualitative-assessment-20260913-002.md、docs/analysis/microraft-experience-adoption-20260914-001.md |
