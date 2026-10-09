# Speedboat 六节点双机房（跨机房级联）实机验证报告

- 日期：2026-10-10
- 结论：**33/33 断言通过**；4 个场景全部按预期表现，同时用运行期证据确认了一个关键架构事实——**当前"跨机房级联"实际上是两个互不相识的独立 Raft 组**
- 验证入口：官方门面路径 `Speedboat.start(PropertiesConfigProvider)`

## 1. 目标

按四个故障场景验证双机房拓扑下的行为：

1. 杀主节点 → 选举是否优先在主机房
2. 杀光主机房 → 备机房是否正常选举
3. 再启动全部主机房 → 是否正常加入
4. 杀光备机房 → 主机房是否仍能选举

并在每个场景附带**拓扑探测**：判定两机房之间是否真的存在跨机房联动。

## 2. 环境

| 主机 | IP | 机房 | 架构 | 默认 JDK | javac |
|------|----|------|------|----------|-------|
| h174 | 192.168.193.174 | 机房0 主 | aarch64 | 1.8.0_492 | ✗ |
| h175 | 192.168.193.175 | 机房0 主 | aarch64 | 11.0.22 | ✗ |
| h176 | 192.168.193.176 | 机房0 主 | aarch64 | 11.0.28 | ✗ |
| h51 | 192.168.193.51 | 机房1 备 | x86_64 | 1.8.0_152 | ✓ |
| h53 | 192.168.193.53 | 机房1 备 | x86_64 | 1.8.0_152 | ✓ |
| h55 | 192.168.193.55 | 机房1 备 | x86_64 | 1.8.0_152 | ✓ |

- 六台已单向信任、两两可达；均为容器（无 sudo、无密码）
- 端口 21001 六台均空闲；本机身份由 `-Dspeedboat.local.ip` 显式钉定
- 持久化：生产缺省 `mmap`，目录 `~/speedboat-test/data/{nodeId}/`

### 环境差异带来的适配

- **双架构 + 主机房无 javac**：测试节点不能像 vbox 那样远端编译，改为**宿主机预编译字节码**（架构无关）打包 `vm-runner.jar` 下发，目标机只需 `java`
- **JDK 版本混合**：`vm_node_ctl.sh` 增加 Java 解析（`SB_JAVA` → Dragonwell8 → `/usr/lib/jvm` 下 Java8 → PATH 上 `java`），实测 174 落到 JRE 8、51 落到 `/bin/java`
- **副产物结论**：被测 jar 以 `--release 8` 构建，因此能在 **Java 8 与 Java 11 混跑**的集群中正常工作（这也是对前次 ABI 修复的跨 JDK 版本回归验证）

## 3. 方法与资产

| 文件 | 说明 |
|------|------|
| `config-vm6-crossdc.properties` | 双机房配置（`nodes.0.*`=主机房、`nodes.1.*`=备机房） |
| `tools/vm/vm-test-lib.sh` | 编排公共库：SSH 通道、结构化日志解析、存活探测、唯一 Leader 判定、断言计数 |
| `tools/vm/vm-crossdc-test.sh` | 六节点双机房编排：P0 基线 → P1~P4 四场景 → P5 拓扑探测 |
| `tools/vm/vm-cluster-test.sh` | vbox 三机编排（已抽取公共库，回归 20/20 通过） |
| `tools/vm/vm_node_ctl.sh` | 远端启停控制（新增 Java 自动解析、支持预编译 `vm-runner.jar`） |

复跑：`bash tools/vm/vm-crossdc-test.sh all`（可分步 `deploy` / `run` / `stop`）。

判定口径：唯一 Leader 以 **`isMain=true`** 计数，而非 `leader` 字段——kill 之后残留的 `leader` 字段可能仍指向已死节点，仅按该字段判唯一会把"全网仍同意一个已死节点"误判为已选出新主。

## 4. 关键架构发现：跨机房级联是半成品

代码核查（`Speedboat.doStartCrossDatacenterMode`）：

- `nodes.{机房索引}.*` 分组数 > 1 即进入跨机房模式，但建组时只取 `allNodes.get(datacenterIndex)`，**peer 列表仅含本机房节点**；
- `election.cross.timeout.min/max` 配置被读取，但建组只用了 intra 超时，**cross 超时未接入**；
- 单机房模式会传入 `voteWeightStrategy`，**跨机房模式漏传**；
- 设计文档描述的"子组选 Leader → 子组 Leader 参与父组选举"的**级联父组未在门面实现**（`RaftGroup` 未被 `Speedboat` 使用）。

项目自身评估报告 `docs/assessment/qualitative-assessment-20260913-002.md` 亦已标注"跨机房模式半成品"。

**运行期证据**（本次 P0/P1/P2/P5 实测）：

| 证据 | 观测 |
|------|------|
| 基线 | 全网同时存在 **2 个 Leader**（机房0 一个、机房1 一个），term 均为 1 |
| 杀主机房 Leader | 机房1 的 Leader 节点与 term **完全不变** |
| 杀光主机房 | 机房1 Leader/term 仍不变，`isMain` 由 2 降为 1 |
| 杀光备机房 | 机房0 Leader/term 仍不变，`isMain` 由 2 降为 1 |

即：机房间无成员关系、无跨机房选举、无跨机房 quorum，两组各自独立演进。

## 5. 场景结果

| 场景 | 断言 | 结果 |
|------|------|------|
| P0 基线 | 主/备机房各选出唯一 Leader；六节点 nodeId 按 ip:port 推导；机房归属 `dc-0`/`dc-1` 正确；全网 `isMain`=2 | 通过 |
| P1 杀主节点 | 主机房崩溃后重新选出唯一 Leader；新 Leader 仍在主机房内且非被杀节点；term 递增（1→3）；**备机房 Leader 与 term 均未被牵动** | 通过 |
| P2 杀光主机房 | 主机房三台均停；备机房仍维持唯一 Leader；Leader 与 term 均未变；`isMain`=1 | 通过 |
| P3 重启主机房 | 三台存活；重新加入并选出唯一 Leader（仍落在主机房内）；term 不低于灭房前（4 ≥ 3，mmap 挂回）；恢复 `isMain`=2；备机房 Leader 未被打断 | 通过 |
| P4 杀光备机房 | 备机房三台均停；主机房仍维持唯一 Leader；Leader 与 term 均未变；`isMain`=1 | 通过 |
| P5 拓扑探测 | 两机房各自独立成组；主机房故障未向备机房传播 term；主机房全灭后备机房未被牵动 | 通过 |

**合计 33 通过 / 0 失败。**

## 6. 附带发现：dependabot 错配升级阻断 Java 8

本次验证首次跑通用的是 10-03 构建的 jar。为了确认结果对应当前 HEAD，从 HEAD 重建后**在 Java 8 上直接崩溃**：

```
java.lang.UnsupportedClassVersionError: ch/qos/logback/core/joran/spi/JoranException
has been compiled by a more recent version of the Java Runtime (class file version 55.0)
```

成因：上游 dependabot 在 10-08 把两个 logback 组件**各自独立升级**，造成配对错配：

| 组件 | PR#2 / PR#3 之前 | 之后 | class major | 要求 |
|------|------------------|------|-------------|------|
| `logback-classic` | 1.2.9 | **1.2.13** | 50 | Java 6/8 + SLF4J 1.7 |
| `logback-core` | 1.2.9 | **1.5.34** | **55** | **Java 11** + SLF4J 2.x |

classic 仍钉在 1.2.13（面向 SLF4J 1.7），core 却跳到 1.5.34（面向 SLF4J 2.x），两者既**跨 Java 基线**又**跨 SLF4J 主线**，而项目目标是 Java 8 + SLF4J 1.7.32。两个 dependabot 提交信息中均**未引用任何安全通告**，属于常规版本跳跃，因此按 Java 8 基线把 core 对齐回 classic 的 1.2.13。

回归：`mvn clean test` **532/532 通过**；随后用修正后的 jar 重跑六机验证，仍 **33/33 通过**。

> 启示：依赖树里"同一 groupId 下互相依赖的组件"必须成对升级；单看 dependabot 的单组件 PR 无法发现这类错配。若日后要上 logback 1.5.x，需同时升级 classic 与 slf4j-api 到 2.x，并把 Java 基线提升到 11。

## 7. 结论与建议

四个场景"全部通过"的**真实原因**是两机房各自独立成组，而非跨机房级联容错生效——这一点必须与"跨机房能力已具备"区分开。若业务诉求是"主机房优先 + 备机房独立存活"，当前实现可满足；若诉求是"单个集群跨两机房、机房级故障仍有多数派"，当前实现**不满足**，需补齐级联父组或机房权重 quorum。

后续可选：

- 补齐级联父组（子组 Leader 组成父组，父组用 cross 超时），或改用机房加权 quorum；
- 修正跨机房模式漏传 `voteWeightStrategy`，使 `DatacenterVoteWeightStrategy` / `PreferNodeVoteWeightStrategy` 在跨机房场景生效；
- 明确 `datacenter=` 配置语义：当前设置后两个机房会拿到**同一个** ID，`resolveDatacenterId` 未按索引区分。

## 8. 环境注意事项

- 六台主机**时区不一致**（17x 为 UTC+8、5x 为 UTC），日志时间戳相差 8 小时；本验证不依赖跨机时间比较，但若后续要做跨机时序断言需先统一时钟。
- vbox 三机环境在本次验证期间处于关机状态，为回归公共库抽取而临时拉起，验证完成后未主动关机。
