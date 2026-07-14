# Data Model: taskgate M2(redis 后端)

## 1. Go 类型变更

| 位置 | 变更 | 说明 |
|---|---|---|
| taskgate(根包) | 新增 `QueueLimiter` / `LimiterProvider` 接口 | 见 research 第 5 节;唯一的根包改动 |
| taskgate(根包) | `limiter` 改名 `localLimiter` 并实现 QueueLimiter | 行为零变化 |
| redisbroker | `New(opts Options) (*Broker, error)`;`Options{Addr, Password, DB, KeyPrefix}` | KeyPrefix 默认 `"tg:"`,多应用共用一个 Redis 时隔离用 |
| Task/Config/Status | 无变更 | |

状态机、计数语义、配置校验:**无变更**(全部沿用 001 的 data-model 与 broker-contract)。

## 2. Redis 键设计(前缀默认 tg:)

| 键 | 类型 | 用途 |
|---|---|---|
| `tg:task:{id}` | hash | 任务全字段(字段名与 Task 一一对应,时间存 unix 毫秒) |
| `tg:pending:{q}` | list | 就绪任务 ID 队列(RPUSH 入,LPOP 出;FIFO) |
| `tg:delayed:{q}` | zset | score=run_at:延迟任务与重试退避;到点由 claim.lua 顺手搬回 pending |
| `tg:inflight` | zset | score=lease_until,member=id;reaper 扫描源 |
| `tg:children:{id}` | set | 反向依赖索引:依赖 {id} 的子任务 ID |
| `tg:idx:status:{status}` | set | List/防御修复用的状态索引(七态各一) |
| `tg:idx:type:{type}` | set | List 按 Type 过滤 |
| `tg:stats` | hash | 字段 `{type}:{status}`,每段 Lua 流转时 HINCRBY 旧-1 新+1;Counts=HGETALL |
| `tg:types` | set | 出现过的 Type |
| `tg:sem:{q}` | zset | 分布式并发槽:member=槽 ulid,score=过期时刻(限流器私有,不属 Broker) |
| redis_rate 自管键 | - | GCRA 状态(限流器私有) |

任务的 `pending_parents`、`cancel_requested`、`lease_token` 等内部字段都在 `tg:task:{id}` hash 里,与 sqlite 列一一对应。

## 3. Lua 脚本清单(redisbroker/lua/,go:embed;时间/ID 全由 ARGV 注入)

| 脚本 | 原子边界内做的事 |
|---|---|
| `enqueue.lua` | 查重(EXISTS task)→ 校验父存在 → DecideOnSubmit 等价逻辑(读父状态算初始态/pending_parents)→ 写 hash+入 pending 或 delayed 或直接 canceled(含传播)→ 索引/计数/types |
| `claim.lua` | 搬到期 delayed→pending(限本次涉及的队列)→ 循环 LPOP pending 直到取到合法任务(校验 status∈{pending,retrying} 且 run_at≤now;不合法的丢弃出列,hash 为准)→ 写 running/lease_token/lease_until/started_at → ZADD inflight → 索引/计数 → 返回任务全字段 |
| `finish.lua` | op=ack/fail/finish_canceled/cancel/requeue 共用:令牌校验(op 需要时)→ canTransition 等价校验 → 写终态/retrying/pending → **工作队列收敛整棵子树传播**(唤醒/连锁取消)→ inflight/delayed/索引/计数维护 → 返回流转快照列表(Go 侧发 Notify) |
| `heartbeat.lua` | 令牌校验 → 续 lease_until(ZADD XX inflight + hash)→ 返回 cancel_requested 标记 |
| `reap.lua` | ZRANGEBYSCORE inflight 到期项:cancel_requested=1 → canceled(传播);LeaseLost+1 封顶 failed(传播)/否则回 pending;顺手防御修复 blocked(扫 `tg:idx:status:blocked`,父全终态的补唤醒/补取消) |
| `sem_acquire.lua` | 清过期槽 → ZCARD<limit 则 ZADD 占槽返回 1,否则返回 0(限流器私有) |

错误映射:脚本返回形如 `{err_code, ...}` 的表,Go 侧翻译为 ErrTaskExists/ErrTaskNotFound/ErrLeaseLost/ErrAlreadyFinal/ErrTaskCanceled 等哨兵;网络错误原样透传(research 第 9 节)。

## 4. Dequeue 阻塞语义的实现

Go 侧循环(与 sqlitebroker 同构):

1. 执行 claim.lua(非阻塞,原子)
2. 认领到 → 返回;没认领到 → 等 `min(100ms, 各队列 delayed 最近到期时刻−now)`,等待挂三个唤醒源:注入 clock、同进程 Enqueue 的换代式唤醒信号、ctx
3. ctx 取消 → 返回 ctx.Err()(所有 redis 错误若 `ctx.Err()!=nil` 统一翻译,复刻 sqlite 的 INTERRUPT 教训)

跨进程新任务的最坏发现延迟 100ms,与 sqlite 跨进程一致,写进 README。

## 5. 分布式限流器(redisbroker/limiter.go,实现 taskgate.QueueLimiter)

- `AcquireSlot`:sem_acquire.lua 轮询(失败等 50ms,挂 clock/ctx);成功后起续期 goroutine(每 LeaseTTL/3 ZADD XX 刷新 score),`ReleaseSlot` 停续期 + ZREM
- `WaitToken`:redis_rate.AllowN 循环,被拒按 RetryAfter 等待(挂 clock);RPS=0 直通
- 槽 TTL = 队列 LeaseTTL(与任务租约同生命周期);进程崩溃 → 续期停 → 过期自动回收
- Broker.Init 时接收 clock;`QueueLimiter(queue, qc)` 每队列构造一次,scheduler 缓存

## 6. sqlite / memory

无变更(仅 `limiter` 改名 `localLimiter`,行为不变)。
