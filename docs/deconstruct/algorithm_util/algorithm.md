# 工具类算法

## 概述

Speedboat 工具模块提供 **基础工具支持**，包括网络检测和线程命名工厂。

**工具类**：
- NetworkUtils：网络地址检测与生成
- NamedThreadFactory：线程命名工厂

---

## NetworkUtils

### 功能列表

| 方法 | 功能 |
|------|------|
| detectHostname() | 检测本机 hostname |
| detectLocalIp() | 检测非回环 IP |
| generateNodeId() | 自动生成节点 ID |
| parsePort(address) | 解析端口 |
| parseIp(address) | 解析 IP |
| ipMatchesAddress(ip, address) | IP 匹配检查 |

---

### detectHostname 算法

```
detectHostname():
    try:
        hostname = InetAddress.getLocalHost().getHostName()
        return hostname
    catch Exception:
        return "unknown"
```

**注意事项**：
- 依赖 DNS 解析
- 失败返回 "unknown"

---

### detectLocalIp 算法

```
detectLocalIp():
    // 1. 遍历所有网卡
    interfaces = NetworkInterface.getNetworkInterfaces()
    while interfaces.hasMoreElements():
        ni = interfaces.nextElement()
        
        // 2. 过滤：跳过回环、未激活、虚拟网卡
        if !ni.isUp() || ni.isLoopback() || ni.isVirtual():
            continue
        
        // 3. 遍历网卡地址
        addresses = ni.getInetAddresses()
        while addresses.hasMoreElements():
            addr = addresses.nextElement()
            
            // 4. 过滤：跳过回环、链路本地地址
            if addr.isLoopbackAddress() || addr.isLinkLocalAddress():
                continue
            
            // 5. 返回第一个有效 IPv4
            ip = addr.getHostAddress()
            if ip != null && !ip.isEmpty():
                return ip
    
    // 6. Fallback：使用 getLocalHost
    return InetAddress.getLocalHost().getHostAddress()

catch Exception:
    return "127.0.0.1"
```

**优先级**：
1. 非回环、非链路本地、已激活的 IPv4
2. InetAddress.getLocalHost() 回退
3. "127.0.0.1" 最终回退

---

### generateNodeId 算法

```
generateNodeId():
    hostname = detectHostname()
    ip = detectLocalIp()
    random = String.format("%04d", RANDOM.nextInt(10000))
    
    nodeId = hostname + "-" + ip + "-" + random
    return nodeId
```

**格式**：`{hostname}-{ip}-{random4位}`

**示例**：
- `server01-192.168.10.1-0012`
- `unknown-127.0.0.1-5678`

---

### parsePort / parseIp 算法

```
parsePort(address):
    // 格式："ip:port"
    if !address.contains(":"):
        throw IllegalArgumentException
    
    parts = address.split(":")
    return Integer.parseInt(parts[1])

parseIp(address):
    if !address.contains(":"):
        throw IllegalArgumentException
    
    parts = address.split(":")
    return parts[0]
```

---

### ipMatchesAddress 算法

```
ipMatchesAddress(ip, address):
    if ip == null || address == null:
        return false
    
    addressIp = parseIp(address)
    return ip.equals(addressIp)
```

---

## NamedThreadFactory

### 功能

创建具有统一命名前缀的线程，便于日志追踪和问题定位。

### 实现算法

```
NamedThreadFactory {
    String namePrefix
    AtomicInteger counter
    boolean daemon
}

newThread(runnable):
    threadName = namePrefix + "-" + counter.getAndIncrement()
    thread = new Thread(runnable, threadName)
    thread.setDaemon(daemon)
    return thread
```

### 命名格式

```
Speedboat-{component}-{nodeId}-{seq}
```

**示例**：
- `Speedboat-Election-node1-1`
- `Speedboat-Heartbeat-node2-1`
- `Speedboat-LeaseRenewer-node1-1`

---

### 工厂方法

```
forComponent(component, nodeId):
    return NamedThreadFactory(component, nodeId, daemon=true)

forComponentNonDaemon(component, nodeId):
    return NamedThreadFactory(component, nodeId, daemon=false)
```

---

## 线程命名规范

| Component | 用途 | 守护线程 |
|-----------|------|----------|
| Election | 选举定时器 | 是 |
| Heartbeat | 心跳发送 | 是 |
| LeaseRenewer | 租约续约 | 是 |
| MembershipDetector | 成员检测 | 是 |

---

## 边界情况处理

### NetworkUtils 边界情况

| 情况 | 处理方式 |
|------|----------|
| 无有效网卡 | 返回 "127.0.0.1" |
| DNS 解析失败 | hostname 返回 "unknown" |
| 多网卡 | 返回第一个符合条件的 |
| IPv6 地址 | 当前实现可能返回 IPv6 |

### NamedThreadFactory 边界情况

| 情况 | 处理方式 |
|------|----------|
| counter 溢出 | AtomicInteger 循环 |
| namePrefix 过长 | 无限制（依赖 JVM） |

---

## 性能分析

| 操作 | 时间复杂度 | 说明 |
|------|------------|------|
| detectHostname | O(1) | DNS 查询 |
| detectLocalIp | O(n) | n = 网卡数量 |
| generateNodeId | O(n) | 组合两次检测 |
| parsePort/parseIp | O(1) | 字符串分割 |
| newThread | O(1) | 线程创建 |

---

## 使用示例

### 自动检测节点信息

```java
String nodeId = NetworkUtils.generateNodeId();
String ip = NetworkUtils.detectLocalIp();
int port = 22222;
```

### 创建命名线程池

```java
ExecutorService executor = Executors.newFixedThreadPool(
    4,
    NamedThreadFactory.forComponent("Worker", nodeId)
);
```

---

## 设计权衡

| 决策点 | 选择方案 | 优点 | 缺点 |
|--------|----------|------|------|
| IP 检测 | 多网卡遍历 | 自动发现 | 可能选错网卡 |
| nodeId 生成 | hostname-ip-random | 可读性好 | 冲突概率非零 |
| 线程命名 | 统一前缀 | 便于追踪 | 名称较长 |

---

## 参考

- Java NetworkInterface API
- Java ThreadFactory 接口
- 相关源码：`src/main/java/cn/itcraft/speedboat/util/`
