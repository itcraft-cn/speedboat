# 分项设计 01：通用分布式命名锁

- 编号：2026-09-18-01
- 状态：已实现（提交 6ef0303 / 76b6369 / f33f824；真网验证 loopback + 174/176）
- 关联：总设计 [2026-09-18-00](2026-09-18-00-overall-design.md)、计划稿 docs/plans/2026-09-18-02-named-lock-plan.md
- 需求基线：非抢占公平竞争（用户拍板）；互斥为硬约束；业务接管不归框架，框架只承诺"锁的迁移"

---

## 1. 背景与概念纠偏

报价场景：A/B/C 一个 Raft 组，A 持 forex 锁、B 持 metals 锁、C 持 commodity 锁，三把锁并存。

**主与锁正交**（本次设计之根）：

| | 主节点 | 分布式锁 |
|---|---|---|
| 本质 | 共识角色，每组至多一个 | 资源互斥原语 |
| 归属 | 选举产生 | 任意成员申请、日志共识成功即持有 |
| 迁移 | 选举切换 | 租约到期 + 竞争申请 |

Leader 在锁体系中的唯一职责：**锁命令的日志复制与全序化**。历史实现把"持锁"绑定在"是 Leader"上（tryLockInternal 限 isLeader + propose 无转发），是本分项要拆除的错误前提。

**硬约束**：同名目锁任意时刻至多一个持有者；由日志全序 + 状态机按序 tryAcquire 保证（每条 LOCK 命令要么成功要么失败，结果全网一致）。

## 2. 判定收口：LockStateMachine

裁决铁律：**互斥的唯一裁判在状态机 apply**。转发层、调用方本地预检均只有快速失败/优化意义。

- `LockCommand` 扩展字段：
  - `requestId`：UUID 幂等键（转发重试/日志重放去重；`appliedRequestIds` 表拦截重复应用）；
  - `leaseMs`：申请方指定租约时长（0 → 状态机默认 30s）；
  - `grantTimestampMs`：**Leader 授权打点**（propose 前由 Leader 侧写入），随命令复制后全网到期点一致（`leaseExpireTime = grantPoint + leaseMs`，消除各副本本地打点漂移）；
- `LockEntry` 新增 `epoch`（fencing token）：每次**所有权变更**成功 tryAcquire +1；重入/续期不推进；release 也不消耗（新持有者才推进代际）；
- **判定结果表 `opResults`**：`requestId → LockOpResult{success, epoch, errCode}`（GRANTED / DENIED_HELD_BY / NOT_HOLDER / RENEW_LOST）；软容量 4096、60s 窗口惰性清理——调用方等 apply 后读结果，**消除"被拒仍等满超时"的盲等**；
  - **契约边界（重要）**：等待方 `waitForApplyResult` 上限 1s，远小于 60s TTL，因此"结果被清理而调用方还在等"不会发生；结果表仅在超容量时才按时间清理。业务如自行扩展等待时长，须自行保证 < 60s；
- **失锁可观测（P4-M4）**：`DistributedLock.isLost()` —— `RENEW` 被拒或本地 applied 视图已不持有时为 true；**业务发布前必须校验**（框架不提供回调，轮询是本版本唯一通道）。成功获取时清零；
- APPLY 顺序安全：applyCommand 仅 raft 单线程执行，无锁竞争面（synchronized 仅防御测试直接调用）。

## 3. 转发协议：任意成员可申请

非 Leader 成员无法本地 propose（Raft 限制），新增锁操作转发 RPC：

```
LockOpRequest  { requestId(继承 RpcRequest), routingNodeId, command: byte[] }
LockOpResponse { requestId, ok, entryIndex }
```

- 消息类型 9/10 注册进 `CustomSerializer`（协议帧 len|crc|ser|msg 不变）；
- `TransportLayer` 新契约：`sendLockOp(peer, req)` / `setLockOpHandler(h)`（NettyTransport 与 MockTransport 双实现）；
- **Leader 裁决点 `doHandleLockOp`**（raft 线程）：校验身份 → 解码 LockCommand → 重编排打点（原 lockName/申请者/requestId 原样保留）→ propose → 回执 `entryIndex`；**不做互斥判定**（判定只在 apply）；
- 非 Leader 收到转发：直接回 `ok=false`（客户端改投新 Leader）；
- 客户端双路合一 `proposeOrForward(command)`：isLeader → 本地打点 propose；否则转发等待（上限 6s，略大于传输层 5s 兜底）。

幂等链：client 生成 requestId → Leader 原样带入日志 → 状态机 `appliedRequestIds.putIfAbsent` 去重 → 重试安全（不会重累计数）。

## 4. 客户端路径（DistributedLockImpl）

```
tryLock(timeout):
  loop until deadline:
    attempt++（每次新 requestId）
    epoch = tryLockInternal()                      — 提案（本地/转发双路）
    if epoch >= 0: startRenew(); return handle(epoch)
    sleep(RETRY_INTERVAL)
  return 失败 handle

tryLockInternal:
  Leader 预检 isLockAvailable（仅优化；follower 不预检防本地视图误拒）
  proposeOrForward(commands) → entryIndex
  waitForApplyResult(entryIndex, requestId):
     结果表读到 GRANTED → 验证本地持锁 → 返回 epoch
     DENIED → 立即 -1（外层重试）
     requestId 缺失（旧格式兜底）→ 退化为 applied>=idx 且 isHeldBy 本地判定
unlock / renewLease：同一 proposeOrForward；waitForRelease（1000ms）确认释放语义
```

- `renewLease` 每 leaseMs/2 触发；**RENEW 被拒（RENEW_LOST）→ 立即停续期线程**（宁可误停，不可双持）；
- `unlock` propose/转发失败 → 停续期，交由租约过期自然失效；
- 锁实例缓存 lockCache（同锁名复用 DistributedLockImpl）；同一节点多把锁并存合法。

## 5. API 面与兼容

```java
Speedboat.getLock("forex")                      // 语义不变：锁缓存 + 单例
LockHandle {
    isSuccess(); getNodeId(); getLockName();
    getEpoch();            // 新增 default 方法（fencing token，未持锁 -1）
    close();               // 释放
}
DistributedLockImpl(leaseTimeoutMs) 构造重载   // 测试/短租约迁移验证
LockCommand 新增字段向后兼容（老工厂函数继续可用）
```

下游 fencing 契约（写入 MANUAL）：发布信封携带 `(lockName, holderNodeId, epoch)`；消费侧对同名目只认 epoch 单调递增，epoch 相同时 nodeId 稳定（同 term 同 leader 保证）。

## 6. 锁迁移（用户点名要"考虑清楚"的部分）

| 场景 | 语义 |
|------|------|
| 正常释放 | UNLOCK 经日志 apply（线性化，无重叠） |
| 持有者失联 | 续期断流 → 本地 applied 视图 leaseExpireTime 到期 → 其他成员公平竞争接管（非抢占，**无 REVOKE**；A 恢复后不得夺回） |
| Leader 宕机 | 已持有租约在副本上继续倒计时（不受影响）；**锁状态变更**暂停 ≤1 选举超时，新 Leader 上任恢复；选举期转发以 ok=false 快速失败、客户端重试收敛 |
| 分区旧主 | 自认持有靠 epoch fencing 在消费侧拒旧（框架提供 getEpoch + getTerm 双 token；term 为 Leader 层概念，下游只认 lockName+epoch） |
| 迁移时序承诺 | 失联 → ≤ leaseMs 接管；Leader 宕机额外 ≤ 选举超时（check-quorum 已收紧） |

**严格遵守口径**：任意瞬时"恰好一个"不可实现（检测窗）；可实现承诺 = **至多一个有效持有者**（epoch 判定）+ 有限时间恢复恰好一个（选举收敛）。真网取证：迁移 grant 双长驻副本一致 epoch=2。

## 7. 验证清单（实现时证据）

- 单元（`NamedLockMultiHolderTest` 6 条）：follower 转发持锁 / 并发争抢窗口不重叠（互斥） / epoch 跨所有权变更单调 / 非抢占不得夺回 / 失联租约迁移 / 三锁并存；
- 契约（`MmapRaftStoreTest` + `LockStateMachineCheckpointTest`）：帧保真 / "列表即全量"收口 / 检查点 epoch 跨重启保真；
- 真网（loopback 三 JVM + 174/176 实机）：非 Leader 转发持锁 `[PASS]`；三锁三持有者并存全网 apply 收敛；kill holder → lease 到期 → 移交 epoch=2 双副本一致；`--second-lock` 长驻副本申请他人名目锁取证；
- 全量回归 519 绿。

## 8. 演进边界（不在本期）

- **公平性**：当前为"非抢占公平竞争"，**不保证无饥饿**——理论上存在某竞争者连续多轮落败的可能（先到先得续期即长持）。业务侧若观察到长期拿不到锁，需在申请顺序/重试策略上自谋；公平队列列为演进项；
- **零改造成本项（已落地）**：申请失败重试已加入 ±50% 随机抖动（防多竞争者同拍重试惊群）；
- onAcquired/onLost 回调（当前以 `isLost()` 轮询替代）；批量锁命令日志压缩；"锁与 Leader 绑定/联动"由业务在申请顺序上自谋——框架不提供；
- 锁实例续期调度器当前"每锁一名单线程"，锁名量级很大时建议共享调度器（总设计 §5.1 已列）。
