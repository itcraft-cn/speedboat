# 通用分布式命名锁设计文档（单组）

- 编号：2026-09-18-02
- 状态：**已实现**（详细设计存档见 docs/design/2026-09-18-01-named-lock-design.md；总设计索引 docs/design/2026-09-18-00-overall-design.md）
- 关联：撤销 2026-09-18-01（一进程多组，ad7969e）
- 已拍板：单组 + 通用命名锁；非抢占公平竞争；互斥为硬约束；业务接管不归框架

---

## 1. 需求重述

报价场景：A/B/C 三机一个 Raft 组。A 报外汇须持 forex 锁，B 报贵金属须持 metals 锁，C 报商品须持 commodity 锁。三把锁并存，持有者互不相同。

**概念纠偏（本次设计的根）**：主节点（Leader）与分布式锁是两个正交抽象。

| | 主节点 | 分布式锁 |
|---|---|---|
| 本质 | 共识角色，每组至多一个 | 资源互斥原语 |
| 归属 | 选举产生 | 任意成员申请，日志共识成功即持有 |
| 生命周期 | 选举/降级 | 申请→持有→续期→释放；失联则租约过期 |
| 数量 | 每组 1 | 每名目 1 把，N 把并存 |

Leader 在锁体系中的唯一职责：**锁命令的日志复制与全序化**。锁持有者与 Leader 无必然关系。

**硬约束**：同名目锁，任意时刻至多一个持有者；由日志全序 + 状态机按序 `tryAcquire` 天然保证（每条 LOCK 命令要么成功要么失败，结果全网一致）。

**抢占策略（已拍板）**：非抢占公平竞争——先到先得续期即长持；持有者不失联就不释放；A 恢复后只能等 B 失联或 B 主动释放。不引入 REVOKE。

---

## 2. 现状差距清单

| # | 现状 | 问题 | 目标 |
|---|------|------|------|
| G1 | `tryLockInternal` 要求本节点 `isLeader()`（DistributedLockImpl.java:166） | 非 Leader 永远拿不到锁 | 任意成员可申请 |
| G2 | `propose` 仅 Leader 生效，无转发（RaftNodeImpl.java:1330） | follower 无法提交命令 | 新增 follower→Leader 转发协议 |
| G3 | `applyCommand` 忽略 `tryAcquire` 返回值（LockStateMachine.java:75） | 获取失败静默，调用方超时盲猜 | 命令结果可查询（成功/失败+原因） |
| G4 | `leaseExpireTime` = 各节点 apply 时本地 `currentTimeMillis`（LockEntry.java:39） | 多持有者下各副本到期点漂移 | Leader 打点确定性时间戳，随命令传播 |
| G5 | 无 fencing token | 旧持有者分区自认持有，下游无法拒旧 | 成功持锁发单调 epoch |
| G6 | 锁结果只有进程内等待 | 无重试幂等 | requestId 幂等，转发重试安全 |

违反 MEMORY.md「时钟安全：必须用 nanoTime」的既有结论的地方（G4 修正时一并对齐：对外租约绝对时刻用 Leader 打点的毫秒时间戳传播，节点本地判定用 nanoTime 基准换算）。

---

## 3. 协议设计

### 3.1 命令集（LockCommand 扩展）

```
LOCK(name, clientNodeId, requestId, leaseMs)      // 申请
RENEW(name, clientNodeId, requestId)              // 续期
UNLOCK(name, clientNodeId, requestId)             // 释放
```

- `clientNodeId` = 真实申请者（不再隐含 = Leader）；Leader 转发时保留原值——这是多持有者的身份基石
- `requestId` 全局唯一（UUID），状态机侧以 `name+requestId` 记录已应用的命令，重放/重试幂等
- `leaseMs` 由申请方携带（当前硬编码 30s，改为可配，默认不变）

### 3.2 转发协议（新增 RPC）

```
LockOpRequest { requestId, lockCommand }
LockOpResponse { requestId, ok, epoch, errCode }
```

- follower 收到本地锁申请 → 选 Leader（`getLeaderId()`）→ 发 `LockOpRequest`；Leader `propose` 成功并 apply 后回执
- 无 Leader / Leader 失联：转发失败重试，直至新 Leader 产生或客户端超时（现状 tryLock 重试循环复用）
- Leader 收到转发后：**先本地状态机预检**（`isLockAvailable`）再 propose？——不。预检只是优化，**判定以 apply 后结果为准**（见 3.3），预检失败可直接回执省一次日志（快速失败路径）

### 3.3 状态机判定（G3/G4/G5 落点）

`LockStateMachine.applyCommand` 改造：

```
switch LOCK:
    result = entry.tryAcquire(client, leaseMs, nowFromLeaderStamp)
    results.record(name+requestId, result ? GRANTED(epoch) : DENIED(reason))
    // epoch = entry 新分配的单调序号（见 3.4）
switch RENEW:
    result = entry.renew(client, leaseMs, leaderStamp)
    results.record(...)
switch UNLOCK:
    result = entry.release(client)
    results.record(...)
```

- **判定只发生在状态机 apply 时**，全组按同一日志序得出同一结果——互斥由此保证，转发层不做任何裁权
- 结果表 `results` 随日志重放天然重建（重启后从日志恢复，配合快照/日志截断策略：结果表只需保留"最近窗口"，等待中的调用方超时后自然放弃）

### 3.4 fencing token（G5）

- `LockEntry` 增加 `epoch` 字段：每次 `tryAcquire` 成功 +1（组内单调，持久化语义随锁表）
- `tryLock` 成功返回的 `LockHandle` 携带 `epoch`；`getEpoch()` 进 API
- 下游拒旧规则（写入 MANUAL）：信封携带 (lockName, holderNodeId, epoch)，消费侧对同名目只认 epoch 单调递增
- epoch 与 term 的关系：epoch 是锁级序号，独立于组 term；leader 切换不重置（锁表随日志恢复）。简化且够用：fencing 的本质是"同名目单调"，锁级 epoch 即满足

### 3.5 租约时间基（G4）

- LOCK/RENEW 命令成功判定时，由 **Leader 在 propose 前打点** `leaseGrantTimeMs`（随命令携带）——同一命令全网到期点一致
- 各副本 apply 时记录 `(leaseGrantTimeMs, 本地 nanoTime)` 锚点；本地判定 `isLeaseExpired` = `nanoTime - anchorNano > (leaseGrantTimeMs + leaseMs - grantPoint)` 换算——消除多副本时钟漂移，符合 nanoTime 约束
- 租约语义不变：续期断流 → 过期 → 可被他人申请；持有者本地同样按此判定自行停止业务发布

---

## 4. 客户端语义（DistributedLock API 扩展）

```java
LockHandle handle = lock.tryLock(5000);     // 内部：预检→(转发)→propose→等apply→取结果
handle.isSuccess();
handle.getEpoch();                          // fencing token（新增）
handle.getHolderNodeId();
// 续期：现状后台自动续（renewTask），语义保留；RENEW 走同一转发通道
lock.isLocked(); lock.getHolderNodeId();    // 读本地 applied 状态（最终一致，读旧可接受）
lock.unlock();
```

- `tryLock` 失败语义不变：超时未获锁返回失败 handle
- 非 Leader 申请时延 = 转发 RTT + 日志复制 RTT + apply，同机房预期 < 50ms 量级（现状 Leader 本地 < 10ms；差异文档化）
- 锁实例缓存（`lockCache`）不变；同一节点可持多把锁（A 同时持 forex 与 metals 完全合法）

## 5. 主从联动（业务可见语义，非框架强制）

- "A 是外汇主" = "A 持 forex 锁"；与谁是 Leader 无关
- 框架不提供锁与 Leader 的任何绑定/联动；业务侧若想"主节点优先持有某锁"，自行在申请顺序上处理（非抢占策略下即公平竞争）
- 迁移时延承诺：持有者失联 → 租约到期（leaseMs，建议报价场景配 3~10s）→ 竞争者获锁；Leader 宕机额外增加 ≤1 个选举超时的命令不可用窗口（1~2s，已有 Phase C 机制收敛）

---

## 6. 兼容性

- `DistributedLock` 现有方法全部保留语义；`LockHandle` 新增 `getEpoch()`（接口加默认方法避免破坏实现）
- Leader 本地申请路径 = 转发路径的零距离特例（转发给自己），代码单一通路
- 配置：`lock.lease.ms`（默认 30000，原值）、`lock.op.timeout.ms`（默认 5000，原 tryLock 默认）——原有测试零改动通过

---

## 7. 实施分期

**P1 判定与时间基修正（不引入转发，行为兼容）**
- LockCommand 加 requestId/leaseMs；LockStateMachine 记录判定结果表；LockEntry.epoch + Leader 打点时间基；LockHandle.getEpoch
- 回归：现有锁测试全绿（Leader 申请路径结果语义不变）

**P2 转发协议**
- LockOpRequest/Response RPC + 序列化注册；DistributedLockImpl 双路合一（本地/转发）；重试与幂等
- 测试：单进程 3 节点，非 Leader 节点申请/续期/释放；kill Leader 后转发重试恢复

**P3 互斥与迁移验证**
- 并发争锁：N 节点同时抢同一锁，断言全网唯一持有者与 epoch 单调
- 迁移链路：持有者宕机 → 租约到期 → 竞争者接管（非抢占语义验证：原持有者回归不得夺回，须等新持有者失联/释放）
- 分区注入：持有者分区自认持有 → lease 过期 → 对端接管 → 恢复后 epoch 校验拒旧
- 文档：MANUAL 锁章节重写（多持有者/epoch/非抢占）、README 撤销"单集群"约束中锁相关表述、CHANGELOG

**P4（按需演进，不在本次）**：公平队列（争抢激烈时的先来先得排队）、锁监听回调（onAcquired/onLost）、批量锁命令日志压缩

---

## 8. 风险与开放问题

| 风险 | 处置 |
|------|------|
| 转发增加锁操作时延 | 快速失败预检省日志；同机房 RTT 小；文档化预期值 |
| 结果表内存增长 | 只记"最近窗口"（如 60s），调用方超时 5s 远小于窗口 |
| 转发目标 Leader 刚换届 | requestId 幂等 + 重试自然收敛；client 超时兜底 |
| epoch 与 term 双序号认知负担 | MANUAL 明确：下游只认 lockName+epoch；term 是 Leader 层概念 |
| 非 Leader 申请期间 Leader 失联，锁状态变更窗口 | 与现状 Leader 宕机窗口一致（≤1 选举超时），非新风险 |
