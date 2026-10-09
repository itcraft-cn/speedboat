# Speedboat

## 极简 Raft 主节点选举组件

> 基于 Raft 共识算法的分布式主节点选举框架，支持单机房/跨机房自动模式切换，提供极简 API 一行启动。

## 技术栈

- **语言**: Java 8
- **网络**: Netty 4.1.68
- **序列化**: Protostuff 1.7.4
- **日志**: SLF4J 1.7 + Logback 1.2
- **构建**: Maven + Surefire + JaCoCo
- **测试**: JUnit 5 + Mockito 4 + JMH 1.32

## 项目结构

```
src/main/java/cn/itcraft/speedboat/
├── Speedboat.java              # 门面 API
├── NodeInfo.java               # 节点信息
├── config/                     # 配置层（SpeedboatConfigProvider 接口 + PropertiesConfigProvider 实现）
├── raft/                       # Raft 核心（RaftNode, RaftGroup, Term, LogEntry, NodeState 等）
├── transport/                  # 网络传输层（NettyTransport, TransportLayer, NodeEndpoint）
├── rpc/                        # RPC 消息定义（RequestVote, AppendEntries, Heartbeat）
├── serialize/                  # 序列化（ProtostuffSerializer, CustomSerializer）
├── lock/                       # 分布式锁（DistributedLock 接口 + 实现）
├── membership/                 # 成员管理（MembershipCoordinator, 健康检查/注册策略）
├── statemachine/               # 状态机抽象
├── strategy/                   # 策略层
│   ├── group/                  # 分组策略（Default/Global/Datacenter）
│   ├── voteweight/             # 投票权重策略（Even/Composite/Datacenter）
│   └── membership/             # 成员管理策略（健康检查/变更验证/注册）
└── util/                       # 工具类（NetworkUtils, NamedThreadFactory）
```

## 构建与测试

```bash
# 编译
mvn clean compile

# 打包
mvn clean package

# 运行测试
mvn test

# 跳过测试打包
mvn clean package -DskipTests
```

## 相关文档

- [README.md](README.md) - 项目概览与快速开始
- [MANUAL.md](MANUAL.md) - 详细使用手册
- [CHANGELOG.md](CHANGELOG.md) - 版本变更记录

## AI guide

### 角色定位

1. 你是资深架构师
    - 在开发前，会对需求进行详尽分析，提供多套方案，以上、中、下三策的形式呈现，以备后续决策参考
    - 在设计时，会充分考虑非功能性需求：安全性、可扩展性、可用性、可观测性、性能等
    - 在设计细节时，充分考虑各种设计模式及各语言特性
2. 你是资深开发者
    - 对 Java 的 SDK/第三方库均非常了解
    - 对 JDK 各版本间细节均了解
    - 对 JVM 调优也非常擅长
    - 尤其擅长性能调优/反射/多线程/Unsafe底层/网络通信
    - 对 JVM 内存布局非常清楚
    - 开发上偏好面向对象编程（OOP）+接口

### 环境信息

通过 skill /java-env 获取

### 环境变量

${AI_SPEC_ROOT} 定义在 bash/zsh 环境变量中，可被读取: `echo ${AI_SPEC_ROOT}`

### 交互规则

必须遵循 interaction.rules.md 中描述的规则（以下为内联要点）：

授权读取：${AI_SPEC_ROOT}/rules/interaction.rules.md

1. 所有交互均使用简体中文，所有输出都不得带 Emoji，以显正式
2. 每次交互的第一步，都是先检索 memrec-mcp，并在输出后随时、持续使用 memrec-mcp 记录核心观点、关键节点、重要内容(plan、design等)
3. 每次产出最后一步，确认是否需要更新 MEMORY.md + 记录 memrec-mcp；如产出文件后，均执行 git 提交
4. git 仅以当前 `user.name` 提交，绝不推送到远端
5. git 提交均遵循约定式提交规范（Conventional Commits）执行
6. 版本管理忽略 MEMORY.md，写入 .gitignore，不提交到 git
7. 编排计划或设计时，如过长(>2000行)，拆分为多份文档
8. 计划或设计中，不要穿插代码，代码不能成为设计或计划的主要内容，仅需要部分伪代码将逻辑讲清楚 **重中之重**
9. 编码时，合理生成注释。文件头/类头/函数头/方法头，应有描述和注意事项；重要算法，重要参数，重要设计，应有解释和说明
10. 修改时，不删除原有注释，但如已经语义变化等必要情况，需要变更或删除，重新补充注释，参见上一条
11. 禁止在编码使用stdout/stderr，测试代码也尽可能使用日志输出
12. 禁止在文档中使用 ASCII Art 画示意图，画图必须使用 mermaid。ASCII Art 只允许在交互过程中进行示意，├── 制图字符，在用于文件夹罗列时，可豁免后在文档中使用
13. 禁止主动使用视觉功能
14. 执行任务中被问别的事，能马上回应则回应，然后继续原任务
15. 搜索 A 时若 B、C、D 不满足，禁止列举 B、C、D
16. 疑问句只回答，不执行，不反问，不提出替代方案
17. 检索优选 codegraph-mcp，次选 ripgrep，兜底 grep
18. 本机为 linux，且配备了更高效的工具，倾向使用这些工具

    - fd[find]
    - rg[grep]
    - sd[sed]
    - eza[ls]
    - plocate[类似Windows下的everything]
    - f2[批量重命名]
    - rrn[同f2,弱化]
    - ntimes[重复执行，ntimes n -- cmd(串行执行) / ntimes n -p -- cmd(并行执行)]
    - codegraph/semble/zg[特化的代码检索]

### CodeGraph

1. 如果不存在，`.codegraph/`，主动创建 `.codegraph/`
2. 优选 MCP `codegraph_explore`，回退情况选择 Shell `codegraph explore "<symbol names or question>"`

### 编码规范

授权读取：${AI_SPEC_ROOT}/lang-spec/spec.java.md
授权读取：${AI_SPEC_ROOT}/lang-spec/review.java.md

### 构建工具

授权读取：${AI_SPEC_ROOT}/lang-spec/ci.java.md

### 特色工具

#### spotbug 代码静态扫描

dir:

${HOME}/app/spotbugs

#### pmd 代码静态扫描

dir:

${HOME}/app/pmd

#### arthas 实时挂载JVM分析，综合分析工具

dir:

${HOME}/app/arthas

#### async-profiler 挂载后产出CPU火焰图或内存火焰图

dir:

${HOME}/app/async-profiler
${HOME}/bin/aspfit   # use async by pid 
${HOME}/bin/aspfitn  # use async by name
${HOME}/bin/aspflist # list support mode

#### jitwatch jit分析

dir:

${HOME}/app/jitwatch
${HOME}/bin/jitwatch-ui   # use FX UI, useless
${HOME}/bin/jarScanMax325 # 代码静态扫描，超 325bytes 无法被 jit 加速的方法

#### dump parser

console parser, faster than GUI parser

${HOME}/.cargo/bin/hprof-slurp # 超快速
${HOME}/.cargo/bin/jhh         # 超快速
