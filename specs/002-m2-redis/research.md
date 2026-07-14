# Research: taskgate M2

## 1. 认领:单段 Lua 原子完成,不用 BLMOVE 两步(与方案 v5 §7.2 的有意偏差)

**问题**: 方案 v5 写的是"BLMOVE 进中转 list → Lua 挪 inflight zset"两步,两步之间崩溃靠 reaper 扫中转区兜底。要不要照抄?

**研究结果**: 两步方案的动机是"阻塞出队"(BLMOVE 挂在服务端)。但它带来三个代价:①崩溃窗口+中转区扫描;②BLMOVE 真阻塞挂在真时间上,fakeclock 推不动,brokertest 的时间语义破功;③中转区滞留判定还得再存一份时间戳。而 M1 的 sqlite 后端已经确立了"轮询式阻塞"的合法先例(Dequeue 合同:阻塞到有任务或 ctx 取消,实现允许轮询)。

**Decision**: 认领 = 一段 claim.lua 原子完成:搬运到期 delayed → LPOP pending → 校验 → 写 running/令牌/租约 → ZADD inflight → 计数。Dequeue 是 Go 侧循环:试认领 → 无果等 `min(100ms, 最近 run_at−now)`(走注入 clock,同进程 Enqueue 踢唤醒信号)→ 再试。

**Rationale**: 消灭崩溃窗口(spec FR-003 由构造满足);fakeclock 兼容;与 sqlite 的跨进程 ≤100ms 延迟同级,文档一致。

**Alternatives considered**:
- BLMOVE 两步:窗口+双份扫描+fakeclock 失效,排除;M3 可作为低延迟快路径叠加。
- 每队列一个 BLPOP 通知键(任务本体仍走 Lua):多一套键和信号语义,收益只有省轮询,M2 不值。

## 2. Lua 时间与随机:全部由 Go 注入

**Decision**: 所有 Lua 通过 ARGV 接收 `now`(clock.Now().UnixMilli())与预生成的 ulid;脚本内禁用 `TIME`/`math.random`。

**Rationale**: fakeclock 在 miniredis 上直接有效(与 sqlite 后端同一招);脚本纯确定性,真 Redis 复制/重放也安全。

## 3. 传播(依赖唤醒/连锁取消)的事务粒度

**Decision**: 与 M1 双后端一致——finish.lua 内用工作队列把整棵子树在**同一段脚本**里收敛(宪法 v1.1.0 认可的"单调用收敛"形态);reaper 防御修复照搬。

**Rationale**: brokertest 的 CascadeCancel 断言"触发调用返回前收敛",单段 Lua 是最直接的满足方式;逐层多脚本反而制造中间态。子树规模在 LLM 流水线场景是个位数,Lua 阻塞时长可忽略。

## 4. 键设计与 List/Counts 的实现取舍

**Decision**: 见 data-model.md。要点:任务本体 `tg:task:{id}` hash;每队列 `tg:pending:{q}` list + `tg:delayed:{q}` zset(score=run_at,延迟与重试退避共用);全局 `tg:inflight` zset(score=lease_until);计数 `tg:stats` hash 由每段 Lua 顺手 HINCRBY;List 走 `tg:idx:status:{status}` 索引 set。

**Rationale**: 与方案 §7.2 一致(仅去掉中转 list);Counts O(1)(spec FR-005);运维可 `LLEN tg:pending:{q}` 直查(FR-012)。

**Alternatives considered**: List 用 SCAN 全库匹配 `tg:task:*`:O(N) 且阻塞,排除。

## 5. 分布式限流的接入点:能力接口,不改 Broker

**问题**: scheduler 怎么知道"这个后端能提供分布式限流"?宪法 II.2 禁止上层对具体后端特判;Broker 接口又禁改(限流也不是所有后端都能做,进不了最小公倍数)。

**Decision**: 根包新增两个导出接口:

```go
// QueueLimiter 一个队列的限流器抽象(M1 的进程内实现改名 localLimiter 实现它)
type QueueLimiter interface {
    AcquireSlot(ctx context.Context) error // 占并发槽,阻塞
    ReleaseSlot()                          // 归还,与 Acquire 配对
    WaitToken(ctx context.Context) error   // 等 RPS 令牌
}
// LimiterProvider 后端的可选能力:能为队列提供跨进程共享的限流器
type LimiterProvider interface {
    QueueLimiter(queue string, qc QueueConfig) (QueueLimiter, error)
}
```

scheduler 装配:`if lp, ok := broker.(LimiterProvider); ok { 用它 } else { newLocalLimiter(...) }`。

**Rationale**: 断言的是**能力接口**而非具体后端类型——上层依然不 import 任何后端包,新后端想提供分布式限流实现该接口即可,不违反 II.2 的本意(防"签名一样、行为漂移"的后门特判)。memory/sqlite 不实现,自动走 local,零回归。

**Alternatives considered**:
- Config 注入 LimiterFactory:把装配负担丢给使用者,一行切换的承诺破功,排除。
- 塞进 Broker 接口:memory/sqlite 也被迫实现分布式语义,违反最小公倍数,排除。

## 6. Workers 分布式信号量的实现与崩溃回收

**Decision**: 每队列 `tg:sem:{q}` zset,member=槽 ID(ulid),score=过期时刻(now+LeaseTTL)。acquire.lua:先 ZREMRANGEBYSCORE 清过期,ZCARD < Workers 则 ZADD 占槽,否则失败(Go 侧带退避轮询,挂 clock);持有期间每 LeaseTTL/3 由限流器内部 goroutine 续期(ZADD XX 更新 score);Release = ZREM。进程崩溃 → 续期停 → 槽过期自动回收(SC-003 的 ≤2×LeaseTTL)。

**Rationale**: 复用 LeaseTTL 语义(任务租约和槽租约同生命周期,好理解);续期藏在限流器实现内,scheduler 零改动。

**Alternatives considered**: Redis 官方 Redlock/SETNX 单锁:是互斥锁不是计数信号量,排除;INCR/DECR 计数:崩溃不自愈,排除。

## 7. RPS 分布式令牌桶

**Decision**: `github.com/go-redis/redis_rate/v10`(GCRA)。`WaitToken` = AllowN(1) 循环:被拒时按返回的 RetryAfter 等待(挂注入 clock)再试。Burst 映射到 redis_rate 的 Burst 参数;RPS=0 仍是"不限速"直通。

**Rationale**: 方案与宪法技术栈钦点;GCRA 天然多进程共享。注意:redis_rate 的 Lua 用 Redis 服务器时间,属于"RPS 走真时钟"的既有豁免(spec FR-018 例外条款,M1 已注明),miniredis 支持其脚本。

**Alternatives considered**: 自写令牌桶 Lua:重复造轮子且要自己证明正确性,排除。

## 8. 双进程/kill -9 专项的测试形态

**Decision**: miniredis 起在父测试进程(它监听真实 TCP 端口,子进程连得上);"双进程"= exec 自身二进制的子进程模式(沿用 crash_test.go 的哨兵文件模式);断连恢复用 miniredis 的 Close/重启。全部 `-short` 跳过;另设 `TASKGATE_REDIS_ADDR` 档让同一批测试跑真 Redis。

**Rationale**: CI 离线可跑(测试纪律);真 Redis 档保 Lua 兼容性,两档同一套测试代码。

## 9. 断连错误与令牌错误的区分

**Decision**: redisbroker 对网络/IO 错误原样返回(包装上下文),**绝不**把它们折叠成 ErrLeaseLost/ErrTaskNotFound;哨兵错误只在 Lua 明确判定时返回(脚本返回错误码,Go 侧映射)。

**Rationale**: M1 调度器对心跳错误已区分"令牌拒绝(放弃结果)"与"其他错误(容忍重试)",redis 断连必须落进后者,否则一次网络抖动就丢结果(spec Edge Case 最后一条)。

## 10. 基准测试形态

**Decision**: 根包 `bench_test.go`:BenchmarkEnqueue(单/32 并发 × sqlite/redis)、BenchmarkDequeueAck(限流关掉,W=8)、BenchmarkPipeline(三级依赖链);redis 用 miniredis(基线含 miniredis 开销,注明);跑一轮把 ns/op 写回测试方案第 6 节。

**Rationale**: 测试方案第 6 节原文要求;目的是防退化基线,不是绝对性能标定,miniredis 足够。
