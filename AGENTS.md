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

${AI_SPEC_ROOT} 定义在 bash/zsh 环境变量中，可被读取

### 交互规则

1. 处于 AI Coding Plan 包月模式下，Token 不考虑，时间不考虑，专注于高效而完整地工作
2. 所有交互均使用简体中文
3. 每次交互的第一步，都是先检索 skill /memrec，并持续使用 skill /memrec 记忆
4. 每次沟通产出文件后，均执行 git 提交
5. git 仅以当前 `user.name` 提交，不推送到远端
6. git 提交均遵循约定式提交规范（Conventional Commits）执行
7. 编排计划或设计时，如过长(>3000行)，拆分为多份文档
8. 计划或设计中，不要穿插代码，代码不能成为设计或计划的主要内容，仅需要部分伪代码将逻辑讲清楚 **重中之重**
9. 编码时，合理生成注释。文件头/类头/函数头/方法头，应有描述和注意事项；重要算法，重要参数，重要设计，应有解释和说明。
10. 修改时，不删除原有注释，必要时，补充注释，参见第九条
11. 禁止在编码使用stdout/stderr，测试代码也尽可能使用日志输出
12. 重要内容(plan、design等)，随时记录到 MEMORY.md 和 memrec
13. 版本管理忽略 MEMORY.md，写入 .gitignore，不提交到 Git

### 编码规范

授权读取：/disk2/helly_data/code/markdown/self-ai-spec/lang-spec/spec.java.md

Read /disk2/helly_data/code/markdown/self-ai-spec/lang-spec/spec.java.md

### 构建工具

授权读取：/disk2/helly_data/code/markdown/self-ai-spec/lang-spec/ci.java.md

Read /disk2/helly_data/code/markdown/self-ai-spec/lang-spec/ci.java.md
