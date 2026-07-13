# Config 模块设计文档

## 模块概述

`cn.itcraft.speedboat.config` 模块是 Speedboat 的配置层，负责提供统一的配置抽象和默认实现。采用策略模式定义配置提供者接口，允许用户从 Properties 文件、JSON、YAML、数据库等任意来源加载配置。模块内置了基于 `java.util.Properties` 的零依赖实现，同时通过 `SpeedboatConsts` 集中管理所有系统级常量。

## 核心类职责

### SpeedboatConfigProvider (接口)

**职责**：定义集群配置的获取契约，是配置层的核心抽象。

**关键方法**：

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `getDatacenter()` | `String` | 获取本机房ID，单机房场景返回 null |
| `getNodes()` | `List<List<String>>` | 获取集群节点列表，外层按机房分组，内层为 "ip:port" 格式 |
| `getIntraDatacenterElectionTimeoutMin()` | `int`（默认 1000） | 机房内选举超时下限（ms） |
| `getIntraDatacenterElectionTimeoutMax()` | `int`（默认 2000） | 机房内选举超时上限（ms） |
| `getCrossDatacenterElectionTimeoutMin()` | `int`（默认 3000） | 机房间选举超时下限（ms） |
| `getCrossDatacenterElectionTimeoutMax()` | `int`（默认 5000） | 机房间选举超时上限（ms） |

**设计模式**：接口隔离原则（ISP）——配置接口仅暴露 Speedboat 所需的最小方法集。4 个超时相关方法使用 Java 8 `default` 方法提供默认值，实现类可按需覆盖。

### PropertiesConfigProvider

**职责**：基于 `java.util.Properties` 文件的配置提供者，零外部依赖。

**构造方式**：
- `PropertiesConfigProvider(String configFile)` — 从文件路径加载
- `PropertiesConfigProvider(InputStream inputStream)` — 从流加载

**核心算法**：`nodes.{dcIndex}.{nodeIndex}` 格式解析。采用两层 `while(true)` 循环遍历：
1. 外层循环按 `dcIndex` 递增，调用 `parseDatacenterNodes(dcIndex)` 获取每个机房的节点列表
2. 内层循环按 `nodeIndex` 递增，拼接 `nodes.{dcIndex}.{nodeIndex}` 键名，从 Properties 中取值
3. 当 `properties.getProperty(key)` 返回 null 时，内层循环终止
4. 当 `parseDatacenterNodes` 返回空列表时，外层循环终止

**错误处理**：超时参数解析失败时输出 WARN 日志并回退到接口默认值；节点列表为空时抛出 `IllegalArgumentException`。

**覆盖行为**：4 个超时方法优先读取 Properties 文件中的对应键（`election.intra.timeout.min/max`、`election.cross.timeout.min/max`），若不存在则通过 `SpeedboatConfigProvider.super.getXxx()` 回调到接口默认值。

### SpeedboatConfig

**职责**：不可变配置对象，聚合 Speedboat 运行所需的核心配置项。

**设计模式**：Builder 模式 — 构造器私有，通过内部 `Builder` 类构建。同时提供 `from(SpeedboatConfig)` 静态工厂方法用于从已有配置对象创建 Builder（深拷贝语义）。

**持有字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `port` | `int` | 监听端口 |
| `voteWeightStrategy` | `VoteWeightStrategy` | 投票权重策略 |
| `groupStrategy` | `GroupStrategy` | 分组策略 |
| `datacenterId` | `String` | 数据中心ID |

**不可变性**：所有字段均为 `final`，无 setter 方法，通过构造器一次性注入。正确实现 `equals`/`hashCode`/`toString`。

### SpeedboatConsts

**职责**：集中管理所有系统级常量，采用 `final class` + `private` 构造器防止实例化。

**常量清单**：

| 常量 | 值 | 用途 |
|------|------|------|
| `MIN_ELECTION_TIMEOUT_MS` | 150 | 机房内选举超时下限 |
| `MAX_ELECTION_TIMEOUT_MS` | 300 | 机房内选举超时上限 |
| `HEARTBEAT_INTERVAL_MS` | 50 | 机房内心跳间隔 |
| `GLOBAL_MIN_ELECTION_TIMEOUT_MS` | 500 | 全局（跨机房）选举超时下限 |
| `GLOBAL_MAX_ELECTION_TIMEOUT_MS` | 1000 | 全局（跨机房）选举超时上限 |
| `GLOBAL_HEARTBEAT_INTERVAL_MS` | 200 | 全局心跳间隔 |
| `DEFAULT_SERIALIZATION_TYPE` | 2 | 默认序列化器类型ID |
| `SERIALIZATION_HEADER_LEN` | 9 | 序列化头部长度 |
| `CRC32_LEN` | 4 | CRC32 校验长度 |
| `TYPE_LEN` | 1 | 类型标识长度 |
| `DEFAULT_PORT` | 22222 | 默认端口 |
| `GLOBAL_PORT` | 33333 | 全局组端口 |
| `SHUTDOWN_TIMEOUT_SECONDS` | 1 | 关闭超时时间 |
| `DEFAULT_MAX_LOG_SIZE` | 10 | 默认日志最大条目数 |

### MembershipConfig

**职责**：成员管理配置，控制健康检测、故障确认、注册中心、自动移除等行为。

**设计模式**：Builder 模式 — 提供 `Builder` 内部类，支持链式构建。同时提供无参构造器使用全部默认值。

**RegistryImpl 枚举**：`NONE` / `NACOS` / `REDIS`，定义注册中心实现类型。

**配置项**：

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `healthCheckIntervalMs` | `int` | 1000 | 健康检测间隔（ms） |
| `failureThreshold` | `int` | 3 | 故障计数阈值 |
| `confirmationPeriods` | `int` | 2 | 确认周期数 |
| `registryCheckIntervalMs` | `int` | 30000 | 注册中心检查间隔（ms） |
| `registryImpl` | `RegistryImpl` | `NONE` | 注册中心实现 |
| `enableAutoRemoval` | `boolean` | `true` | 是否启用自动移除 |

**关键方法**：
- `isMembershipChangeEnabled()`：当 `enableAutoRemoval` 为 true 或 `registryImpl` 不为 `NONE` 时返回 true
- `getConfirmationNanos()`：`confirmationPeriods * healthCheckIntervalMs * 1_000_000L`，用于故障确认时间窗口

## 线程安全

- `SpeedboatConfig` 不可变，天然线程安全
- `PropertiesConfigProvider` 构造后字段不可变，线程安全
- `SpeedboatConsts` 仅含静态常量，线程安全
- `MembershipConfig` 不可变（Builder 构建后所有字段为 final），线程安全

## 扩展点

1. **自定义配置源**：实现 `SpeedboatConfigProvider` 接口，可从 JSON/YAML/数据库/配置中心等任意来源加载配置
2. **自定义超时策略**：覆盖 `SpeedboatConfigProvider` 接口的 4 个 default 方法即可
3. **自定义注册中心**：扩展 `RegistryImpl` 枚举并实现对应的 `RegistryStrategy`