# Changelog

All notable changes to Speedboat will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.0.1] - 2026-07-13

### Added

- **README_cn.md**：中文版 README，方便中文用户快速上手
- **README.md 英文版**：重写为英文版 README，面向国际用户

### Changed

- **README.md**：从中文改为英文，与 README_cn.md 形成中英文双文档体系

---

## [1.0.0] - 2026-07-09

### Added

#### 极简 API 重构

- **SpeedboatConfigProvider 接口**：配置接口化，支持用户自定义配置源（JSON/YAML/数据库）
- **PropertiesConfigProvider**：默认实现，零依赖，解析 Properties 文件
- **NetworkUtils**：自动检测 hostname/IP，生成 nodeId（格式：`hostname-ip-random`）
- **集群视角配置**：用户只需配置 `datacenter + nodes`，系统自动匹配本节点
- **极简门面 API**：
  - `Speedboat.start(config)` - 一行启动
  - `Speedboat.stop()` - 停止并释放资源
  - `Speedboat.isMain()` - 判断是否主节点
  - `Speedboat.getLeaderId()` - 获取主节点 ID
  - `Speedboat.getTerm()` - 获取当前任期
  - `Speedboat.getNodeId()` - 获取本节点 ID
  - `Speedboat.getDatacenterId()` - 获取机房 ID
  - `Speedboat.isRunning()` - 检查运行状态

#### 两套选举超时配置

- **机房内选举超时**：`getIntraDatacenterElectionTimeoutMin/Max()`（默认 1000-2000ms）
- **机房间选举超时**：`getCrossDatacenterElectionTimeoutMin/Max()`（默认 3000-5000ms）
- **Properties 配置**：
  - `election.intra.timeout.min/max`
  - `election.cross.timeout.min/max`

#### 自动模式判断

- **单机房扁平模式**：`nodes.size() == 1`，所有节点平等选举，使用 intra 超时
- **跨机房级联模式**：`nodes.size() > 1`，本机房选举使用 intra 超时，跨机房选举使用 cross 超时

#### 配置文件示例

- **config.properties**：单机房示例
- **config-cross-dc.properties**：跨机房示例

#### 使用示例

- **SimpleExample.java**：极简使用示例

### Changed

- **Speedboat.java**：
  - 重构为单例模式 + 静态方法
  - 添加 `crossDatacenterMode` 自动判断
  - 分离 `doStartSingleDatacenterMode()` 和 `doStartCrossDatacenterMode()`
  - 移除旧 API（`init/destroy/initCascade`）

### Removed

- **SpeedboatTest.java**：使用旧 API 的测试文件
- **ClusterExample.java**：使用旧 API 的示例文件
- **ClusterExampleMain.java**：使用旧 API 的示例文件
- **RealClusterExample.java**：使用旧 API 的示例文件
- **ClusterElectionIntegrationTest.java**：使用旧 API 的集成测试

### Fixed

- 修复测试编译错误（方法名冲突、旧 API 调用）

---

## [0.9.0] - 2026-07-08

### Added

#### Raft 核心实现

- **RaftNode**：Raft 节点核心实现，支持 Leader/Follower/Candidate 状态转换
- **ElectionTimeout**：随机选举超时，防止选举冲突
- **LogEntry**：Raft 日志条目
- **Term**：任期管理

#### RPC 通信

- **NettyTransport**：基于 Netty 的 RPC 传输层
- **ProtostuffSerializer**：Protostuff 序列化器
- **CustomSerializer**：自定义序列化包装器
- **NodeEndpoint**：节点地址封装

#### 级联选举

- **RaftGroup**：Raft 组管理器，支持级联架构
- **GroupStrategy**：分组策略接口
- **DefaultGroupStrategy**：默认分组策略实现

#### 成员变更

- **MembershipCoordinator**：成员变更协调器
- **HealthCheckStrategy**：健康检测策略接口
- **PassiveHealthCheckStrategy**：被动健康检测实现
- **RegistryStrategy**：注册中心策略接口
- **ChangeValidationStrategy**：变更验证策略接口

#### 投票权重

- **VoteWeightStrategy**：投票权重策略接口
- **DefaultVoteWeightStrategy**：默认投票权重实现

#### 测试

- **单元测试**：核心组件测试覆盖
- **集成测试**：集群选举测试
- **ClusterFailoverTest**：故障转移测试

---

## [0.1.0] - 2023-02-01

### Added

- 项目初始化
- 基础 Raft 概念验证