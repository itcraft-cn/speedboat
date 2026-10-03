# Speedboat 三机 VirtualBox 实机验证报告

- 日期：2026-10-03
- 结论：**20/20 断言通过**；期间暴露并修复 1 个阻断级构建缺陷
- 验证入口：官方门面路径 `Speedboat.start(PropertiesConfigProvider)`

## 1. 目标

在真实三机、真实网络、真实 Java 8 运行时下，验证 Speedboat 的：

1. 三节点选主一致性与身份推导
2. 分布式命名锁互斥语义
3. Leader 故障转移（崩溃重选）
4. 原 Leader 重新加入
5. mmap 持久化档"跨重启挂回"

> 与单元测试的分工：单测跑在**构建 JDK**上，无法证明产物在**目标 Java 8** 上可运行；本次实机的核心价值即补齐这一缺口。

## 2. 环境

| 主机 | IP | 角色 |
|------|----|----|
| vboxdeb001 | 192.168.77.101 | Raft 节点 |
| vboxdeb002 | 192.168.77.102 | Raft 节点 |
| vboxdeb003 | 192.168.77.103 | Raft 节点 |

- 三机互信（ssh BatchMode）+ sudo；互相可达（两网段均通）
- JDK：Alibaba Dragonwell 8.29.28（`~/lang`），x86_64
- 拓扑：单机房扁平模式（`nodes.0.0/1/2`），统一端口 21001
- 本机身份由 `-Dspeedboat.local.ip=192.168.77.10x` 显式钉定，规避多网卡枚举顺序漂移
- 持久化：显式 `raft.persistence=mmap`（生产缺省档），目录 `~/speedboat-test/data/{nodeId}/`

## 3. 方法与资产

测试节点走**正式用户路径**，而非直接构建 `RaftNode`：

- `src/test/java/cn/itcraft/speedboat/sample/VmClusterNode.java`
  走 `Speedboat.start`，按周期输出结构化 key=value 日志（`event=STATE/LOCK_ACQUIRE/LOCK_RELEASE/...`），供编排脚本断言；含 SIGTERM 优雅停机钩子。
- `config-vm3.properties`：三机单机房扁平配置（192.168.77.10x，mmap）
- `tools/vm/vm-cluster-test.sh`：编排 P1–P5，逐项断言并汇总
- `tools/vm/vm_node_ctl.sh`：远端启停/崩溃模拟（`stop`=SIGTERM、`kill9`=SIGKILL）
- `tools/vm/logback-vm.xml`：远端日志级别收敛到 INFO

复跑：`bash tools/vm/vm-cluster-test.sh all`（或分步 `deploy` / `run` / `stop`）。

## 4. 阻断级缺陷（实机暴露）

### 现象

mmap 档（门面生产缺省）下三节点均停在 `term=1, leader=none`，**永不产生 Leader**；改用 `raft.persistence=none` 立即选主成功。线程转储显示 raft 单线程处于空闲态（非阻塞），即选举任务**已静默死亡**。

### 根因

产物用现代 JDK（9+）编译，`pom` 仅设置 `maven.compiler.source/target=8`。这**不约束 API 链接**：`MmapRaftStore` 中 `mmap.duplicate()` 被链接到 JDK9+ 的协变签名（返回 `MappedByteBuffer`）。该签名在 Java 8 不存在，运行期抛：

```
java.lang.NoSuchMethodError: java.nio.MappedByteBuffer.duplicate()Ljava/nio/MappedByteBuffer;
```

后果链：`persistAndFlushTerm` 崩溃 → `ElectionCoordinator.doStartElection` 抛异常 → 被 `ScheduledExecutorService` 静默吞掉 → 无后续选举超时重排 → 节点永久 CANDIDATE。

单测未发现：surefire 运行在构建 JDK（9+）上，协变签名存在，故全绿。

### 修复

- `pom.xml` 引入 `maven-compiler-plugin 3.11.0` 并配置 `release=8`，强制按 Java 8 API 签名编译；
- 顺带修正两处测试源码的非 Java 8 API：`Optional.orElseThrow()`（Java 10+）与 `Path.of()`（Java 11+）。

## 5. 验证结果

| 阶段 | 断言 | 结果 |
|------|------|------|
| P1 选主 | 25s 内唯一 Leader；三节点 nodeId 按 ip:port 确定性推导；leader 字段全网一致；恰好一个 isMain | 通过 |
| P2 命名锁 | 产生获取事件；fencing epoch 全局唯一；≥2 个成员成功持锁；各节点获取-释放差 ≤1 | 通过 |
| P3 故障转移 | kill -9 主节点后重选唯一 Leader；新 Leader 不同；任期递增（term 1→4）；存活节点恰好一个 isMain | 通过 |
| P4 重加入 | 被重启原主存活；集群仍唯一 Leader；原主降级为 Follower | 通过 |
| P5 mmap 挂回 | 数据文件存在且非空；优雅停/重启后 term 不回退（term=4）；集群仍唯一 Leader | 通过 |

**合计：20 通过 / 0 失败。**

回归：受影响测试类 `NamedLockMultiHolderTest`(7)、`LockStateMachineCheckpointTest`(3)、`MmapRaftStoreTest`(8) 全绿。

## 6. 经验与后续

- **Java 8 目标项目必须 `release=8`**：`source/target=8` 不足以阻止新 API 协变签名被链接；建议 CI 显式以 Java 8 运行产物或增加 ABI 检查。
- **真实异构运行时必须实测**：pc 单测全绿不等于产物可在目标 JVM 运行。
- 已验证档位为 mmap；建议后续补充 `mem` / `none` 档对比与命名锁租约到期接管（失联迁移）场景。
