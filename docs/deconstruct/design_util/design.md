# Util 模块设计文档

## 模块概述

`cn.itcraft.speedboat.util` 是 Speedboat 的工具类模块，提供网络检测和线程工厂两个基础设施工具。这些工具类在 Speedboat 启动时自动调用，负责节点身份识别和线程命名管控。

## NetworkUtils

### 职责

提供自动检测本机 hostname、IP 地址，生成唯一 nodeId，以及解析/匹配网络地址的静态工具方法。

### 核心方法

#### detectHostname()

通过 `InetAddress.getLocalHost().getHostName()` 获取本机 hostname，失败时返回 "unknown"。

**返回值**：hostname 字符串，异常时返回 "unknown"。

#### detectLocalIp()

**算法**（两层遍历 + 两层回退）：

1. **第一层**：遍历所有 `NetworkInterface`，过滤条件：
   - `ni.isUp()` — 网络接口已激活
   - `!ni.isLoopback()` — 非回环
   - `!ni.isVirtual()` — 非虚拟接口
2. **第二层**：遍历接口的 `InetAddress`，过滤条件：
   - `!addr.isLoopbackAddress()` — 非回环地址
   - `!addr.isLinkLocalAddress()` — 非链路本地地址
   - 返回第一个符合条件的 IPv4/IPv6 地址
3. **回退一**：`InetAddress.getLocalHost().getHostAddress()` 获取回退地址
4. **回退二**：返回 "127.0.0.1"

**设计要点**：优先返回非回环、非链路本地的已激活网卡地址，适合多网卡环境。

#### generateNodeId()

**格式**：`{hostname}-{ip}-{random4位}`，例如 `server01-192.168.10.1-0012`。

**生成过程**：调用 `detectHostname()` + `detectLocalIp()` + `Random.nextInt(10000)` 格式化为 4 位数字。

**重载版本**：`generateNodeId(String hostname, String ip)` 接受显式参数而非自动检测。

#### parsePort(String address)

传入 "ip:port" 格式字符串，返回端口号。包含严格的格式校验：必须包含 ":"，分割后必须恰好 2 部分，端口部分必须可解析为整数。非法格式抛出 `IllegalArgumentException`。

#### parseIp(String address)

传入 "ip:port" 格式字符串，返回 IP 地址部分。格式校验同 `parsePort()`。

#### ipMatchesAddress(String ip, String address)

判断 IP 是否匹配地址字符串。提取 address 中的 IP 部分与传入 IP 进行 `equals()` 比较。支持精确 IP 匹配（如 "192.168.10.1" 匹配 "192.168.10.1:3000"）。

**注意**：当前实现仅支持精确 IP 匹配，不支持 hostname 匹配（`parseIp()` 提取的是地址字符串中冒号前的部分，若该部分为 hostname 则直接比较 hostname 字符串）。

### 线程安全

所有方法均为静态无状态方法，`RANDOM` 为 `Random` 实例（非 `ThreadLocalRandom`），在多线程环境下调用 `nextInt()` 是线程安全的。

## NamedThreadFactory

### 职责

自定义线程工厂，为 Speedboat 内部线程提供统一的命名规范，便于 JStack 诊断和日志追踪。

### 命名规范

**格式**：`Speedboat-{component}-{nodeId}-{counter}`

**示例**：`Speedboat-Heartbeat-node-001-1`、`Speedboat-LeaseRenewer-node-001-2`

### 核心字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `namePrefix` | `String`（final） | 线程名前缀（不含计数器） |
| `counter` | `AtomicInteger` | 自增计数器，初始值 1 |
| `daemon` | `boolean`（final） | 是否守护线程 |

### 构造器

`NamedThreadFactory(String component, String nodeId, boolean daemon)` 构造 `namePrefix = "Speedboat-" + component + "-" + nodeId`。

### newThread(Runnable)

创建新线程，设置名称 `namePrefix + "-" + counter.getAndIncrement()`，设置 daemon 标志。

### 静态工厂方法

- `forComponent(String component, String nodeId)`：创建 daemon 类型工厂（默认行为）
- `forComponentNonDaemon(String component, String nodeId)`：创建非 daemon 类型工厂

**设计要点**：默认使用 daemon 线程，确保 JVM 退出时不会因 Speedboat 线程而阻塞。

### 线程安全

`AtomicInteger` 保证计数器线程安全。

## 设计模式

| 模式 | 应用 |
|------|------|
| 工厂方法模式 | `NamedThreadFactory.forComponent()` / `forComponentNonDaemon()` |
| 工具类模式 | `NetworkUtils` 纯静态方法 + 私有构造函数 |

## 扩展点

1. `NetworkUtils` 可扩展支持 IPv6 优先检测
2. `NetworkUtils.ipMatchesAddress()` 可扩展支持 DNS 解析匹配
3. `NamedThreadFactory` 可扩展支持线程优先级、ThreadLocal 初始化等