# 执行计划：成员变更实现 (P2-002)

## 计划概述
实现基于被动健康检测 + 注册中心核验的 Raft 成员变更功能，分 5 个阶段完成，预计总工作量 120 分钟。

## Phase 1: 基础数据结构 (20分钟)
### 目标
定义成员变更所需的核心数据结构。

### 步骤
| 序号 | 步骤 | 输出物 | 验证方式 |
|------|------|--------|----------|
| 1.1 | 创建 `MemberChangeEntry` 类 | `MemberChangeEntry.java` | 编译通过，单元测试 |
| 1.2 | 创建 `FailureRecord` 类 | `FailureRecord.java` | 编译通过，单元测试 |
| 1.3 | 创建 `MembershipConfig` 类 | `MembershipConfig.java` | 编译通过，配置加载测试 |
| 1.4 | 更新 `LogEntryType` 枚举 | `LogEntryType.java` | 编译通过，向后兼容测试 |
| 1.5 | 更新 `SpeedboatConsts` 常量 | `SpeedboatConsts.java` | 编译通过，常量使用测试 |

### 预估工作量
20 分钟

## Phase 2: 策略接口设计 (25分钟)
### 目标
定义可插拔的策略接口。

### 步骤
| 序号 | 步骤 | 输出物 | 验证方式 |
|------|------|--------|----------|
| 2.1 | 创建 `HealthCheckStrategy` 接口 | `HealthCheckStrategy.java` | 编译通过，接口定义完整 |
| 2.2 | 创建 `RegistryStrategy` 接口 | `RegistryStrategy.java` | 编译通过，接口定义完整 |
| 2.3 | 创建 `ChangeValidationStrategy` 接口 | `ChangeValidationStrategy.java` | 编译通过，接口定义完整 |
| 2.4 | 创建默认实现类 | `DefaultHealthCheckStrategy.java` 等 | 编译通过，基本功能测试 |
| 2.5 | 更新 `SpeedboatConfig` 支持成员变更配置 | `SpeedboatConfig.java` | 编译通过，配置解析测试 |

### 预估工作量
25 分钟

## Phase 3: RaftNode 集成 (30分钟)
### 目标
在 `RaftNode` 中集成成员变更逻辑。

### 步骤
| 序号 | 步骤 | 输出物 | 验证方式 |
|------|------|--------|----------|
| 3.1 | 在 `RaftNode` 添加成员变更字段 | `RaftNode.java` | 编译通过，字段初始化测试 |
| 3.2 | 实现 `applyMemberChange` 方法 | `RaftNode.java` | 单元测试验证状态更新 |
| 3.3 | 实现 `handleMemberChangeRequest` 方法 | `RaftNode.java` | 单元测试验证请求处理 |
| 3.4 | 更新 `becomeLeader` 初始化成员变更组件 | `RaftNode.java` | 编译通过，初始化测试 |
| 3.5 | 更新 `Builder` 支持成员变更配置 | `RaftNode.java` | 编译通过，构建测试 |

### 预估工作量
30 分钟

## Phase 4: 协调器实现 (25分钟)
### 目标
实现变更协调器逻辑。

### 步骤
| 序号 | 步骤 | 输出物 | 验证方式 |
|------|------|--------|----------|
| 4.1 | 创建 `MembershipCoordinator` 类 | `MembershipCoordinator.java` | 编译通过，基本功能测试 |
| 4.2 | 实现健康检测逻辑 | `MembershipCoordinator.java` | 单元测试验证检测逻辑 |
| 4.3 | 实现注册中心核验逻辑 | `MembershipCoordinator.java` | 单元测试验证核验逻辑 |
| 4.4 | 实现变更提案逻辑 | `MembershipCoordinator.java` | 单元测试验证提案生成 |
| 4.5 | 集成到 RaftNode 定时任务 | `RaftNode.java` | 集成测试验证调度 |

### 预估工作量
25 分钟

## Phase 5: 测试与验证 (20分钟)
### 目标
完成全面测试验证。

### 步骤
| 序号 | 步骤 | 输出物 | 验证方式 |
|------|------|--------|----------|
| 5.1 | 编写 `MemberChangeEntryTest` | `MemberChangeEntryTest.java` | 单元测试通过 |
| 5.2 | 编写 `MembershipCoordinatorTest` | `MembershipCoordinatorTest.java` | 单元测试通过 |
| 5.3 | 编写 `MembershipIntegrationTest` | `MembershipIntegrationTest.java` | 集成测试通过 |
| 5.4 | 运行完整测试套件 | 测试报告 | 所有测试通过 |
| 5.5 | 覆盖率验证 | 覆盖率报告 | 分支覆盖率 ≥ 80% |

### 预估工作量
20 分钟

## 验证策略
### 单元验证
- 每个类有对应的单元测试
- 覆盖正常流程和异常场景
- 使用 Mock 隔离外部依赖

### 集成验证
- 3 节点集群场景测试
- 成员变更端到端流程验证
- 网络分区场景测试

### 端到端验证
- 启动多节点集群
- 模拟节点故障
- 验证自动移除功能
- 验证注册中心核验功能

## 回滚策略
1. 代码回滚：使用 Git 版本控制
2. 配置回滚：保留原始配置文件
3. 数据回滚：故障信息不持久化，重启即重置