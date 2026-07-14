# taskgate

一个 `go get` 即用的 Go 任务排队限流库:排队、限流、重试、依赖、取消、优雅停止。它是库不是服务——没有 Web UI、不读环境变量和配置文件,只吃你传进来的 `Config`。

典型场景:调用 LLM / OCR 这类有配额的外部网关时,把请求收进队列,按类型隔离限流,失败自动退避重试,进程崩了靠租约把任务捞回来。

## 特性

- **类型级限流**:每个队列独立的 `{Workers, RPS, Burst}`,慢队列绝不拖累快队列;`Routes` 支持多类型共享同一队列(共享同一个网关配额)
- **三种后端**:`memorybroker`(内存,零依赖)、`sqlitebroker`(单文件落盘,纯 Go 免 cgo)、`redisbroker`(多进程共享,Lua 原子流转);同一套行为契约(`brokertest` 17 条)三后端验收
- **分布式限流**(redis 后端):同一队列的 Workers/RPS 配额在所有接同一 Redis 的进程间共享,加机器不等于把网关打爆;进程崩溃占着的并发槽按租约自动回收
- **租约回收**:认领即加租约,心跳自动续租;worker 崩溃后 reaper 把任务捞回重跑,毒任务封顶进死信
- **重试三计数分工**:`Attempts` 管业务失败(指数退避 `min(2^n×1s,10min)±20%`,超 `MaxRetry` 进 failed)、`Throttled` 管网关限流(`ErrThrottled` 不占重试次数)、`LeaseLost` 管崩溃回收;`ErrSkipRetry` 直接死信
- **依赖流水线**:`DependsOn` 串联/扇入,父任务终态与子任务唤醒在同一事务内完成,不丢唤醒;父失败默认连锁取消(可 `IgnoreParentFailure`)
- **取消**:pending/blocked 直接取消并向下传播;running 任务的 handler ctx 被即时 cancel
- **优雅停止**:`Shutdown(ctx)` 等在跑任务善终;超时则打断并把任务原样归还(不占任何计数),部署重启不消耗任务配额
- **可观测**:`Get / List / Stats / Overview / Wait` 查询等待,`OnStateChange` 回调埋点

## 安装

```bash
go get github.com/ambrose/taskgate
```

运行期依赖:`modernc.org/sqlite`(纯 Go)、`golang.org/x/time/rate`、`github.com/oklog/ulid/v2`、`github.com/redis/go-redis/v9`、`github.com/go-redis/redis_rate/v10`;测试依赖 `github.com/alicebob/miniredis/v2`。

## 快速开始

三级流水线:检索 → 生成 → 打分(完整可跑版本见 [examples/llm](examples/llm/main.go),`go run ./examples/llm`)。

```go
g, err := taskgate.New(taskgate.Config{
    Broker: memorybroker.New(), // 或 sqlitebroker.Open("tasks.db") / redisbroker.New(...)
    Queues: map[string]taskgate.QueueConfig{
        "cpu": {Workers: 4},         // 本地轻活:4 并发不限速
        "llm": {Workers: 2, RPS: 3}, // 大模型网关:2 并发,每秒最多 3 个
    },
    Routes: map[string]string{ // 任务类型 → 队列
        "retrieve": "cpu", "generate": "llm", "score": "cpu",
    },
})

// 注册 handler:返回值写进 Result,返回错误决定重试路径。
g.Handle("generate", func(ctx context.Context, t *taskgate.Task) ([]byte, error) {
    parent, _ := g.Get(ctx, t.DependsOn[0]) // 读上游的 Result
    // ... 调大模型 ...
    return json.Marshal(map[string]string{"answer": "..."}), nil
})

// 起消费循环(阻塞,放 goroutine)。
go g.Run(context.Background())

// 提交流水线:DependsOn 串联,父任务完成才唤醒子任务。
rid, _ := g.Submit(ctx, "retrieve", payload)
gid, _ := g.Submit(ctx, "generate", nil, taskgate.DependsOn(rid))
sid, _ := g.Submit(ctx, "score", nil, taskgate.DependsOn(gid))

// 等最终结果;优雅停机。
result, _ := g.Wait(ctx, sid)
_ = g.Shutdown(ctx)
```

handler 里的错误语义:

```go
return nil, taskgate.ErrThrottled{RetryAfter: 30 * time.Second} // 网关限流:延后重排,不占重试次数
return nil, taskgate.ErrSkipRetry{Err: err}                     // 没救的错:直接进 failed 死信
return nil, err                                                 // 普通业务失败:指数退避重试
```

注意:ErrThrottled/ErrSkipRetry 必须按值返回(errors.As 按值匹配),不要返回其指针。

提交选项:`WithID`(幂等去重)、`Delay` / `RunAt`(延迟执行)、`MaxRetry`、`DependsOn`、`IgnoreParentFailure`。

## Redis 后端(多进程)

多个 worker 进程接同一个 Redis 抢同一批任务时用它:每个任务恰好被执行一次,进程 kill -9 后在跑任务由租约回收重跑。换后端只改构造这一行,其余代码零改动:

```go
b, err := redisbroker.New(redisbroker.Options{
    Addr:     "127.0.0.1:6379",
    Password: "",    // 空 = 不认证
    DB:       0,
    KeyPrefix: "tg:", // 默认 "tg:";多应用共用一个 Redis 时用它隔离
})
if err != nil { ... }
g, err := taskgate.New(taskgate.Config{Broker: b, Queues: ...})
```

所有"多步读写必须原子"的操作(认领、终态+依赖唤醒、连锁取消、计数维护)都在单段 Lua 脚本内完成,不存在"任务已离队却没有租约"的崩溃窗口;"父完成但子未唤醒"不可被观测到。

### 分布式限流:多进程共享配额

redis 后端额外实现了 `LimiterProvider` 能力接口,同一队列的 `{Workers, RPS}` 配额在**所有接同一 Redis 的进程间共享**——两个进程各配 `{Workers: 2}`,全局同时在跑的也是 2 个,不是 4 个;RPS 走 GCRA(redis_rate),同样是全局速率。memory/sqlite 后端不受影响,维持进程内限流。

并发槽的自愈:每占一个槽记一个过期时刻(= 队列的 `LeaseTTL`),持有进程每 `LeaseTTL/3` 自动续期;进程崩溃 → 续期停 → 槽到期自动回收,最坏 `2×LeaseTTL` 内配额可再用,不会永久泄漏。

### 跨进程延迟(心里有数)

- **新任务被别的进程发现**:同进程提交有内部唤醒信号即时响应;别的进程写入靠 Dequeue 的兜底轮询发现,最坏 **≤100ms**(与 sqlite 跨进程一致)。
- **跨进程 Cancel**:running 任务的取消标记由持有它的进程在下一次心跳发现,最坏 **≤ 一个心跳周期(LeaseTTL/3)** 后其 handler ctx 被 cancel。

### Redis 键名速查(运维直查)

默认前缀 `tg:`(`Options.KeyPrefix` 可改)。积压、在跑数不用走应用,`redis-cli` 直接看:

| 键 | 类型 | 用途 | 直查示例 |
|---|---|---|---|
| `tg:task:{id}` | hash | 任务全字段(时间存 unix 毫秒) | `HGETALL tg:task:01J...` |
| `tg:pending:{queue}` | list | 就绪任务 ID 队列(FIFO) | `LLEN tg:pending:scoring` |
| `tg:delayed:{queue}` | zset | 延迟/重试退避任务,score=run_at | `ZCARD tg:delayed:scoring` |
| `tg:inflight` | zset | 在跑任务,score=租约到期时刻 | `ZCARD tg:inflight` |
| `tg:children:{id}` | set | 反向依赖索引(依赖 {id} 的子任务) | `SMEMBERS tg:children:01J...` |
| `tg:idx:status:{status}` | set | 状态索引(七态各一) | `SCARD tg:idx:status:failed` |
| `tg:idx:type:{type}` | set | Type 索引(List 过滤用) | `SCARD tg:idx:type:ocr` |
| `tg:stats` | hash | Type×Status 计数,字段 `{type}:{status}` | `HGETALL tg:stats` |
| `tg:types` | set | 出现过的 Type | `SMEMBERS tg:types` |
| `tg:sem:{queue}` | zset | 分布式并发槽(限流器私有) | `ZCARD tg:sem:scoring` |
| `rate:tg:{queue}` | string | RPS 限速状态(redis_rate 的 GCRA 内部,限流器私有)。注意 `rate:` 前缀由 redis_rate 加在最外层,该键**不在** `KeyPrefix` 命名空间内(实际键名 = `rate:` + KeyPrefix + 队列名),按前缀批量清理时别漏 | `GET rate:tg:scoring` |

`Counts`/`Overview` 就是读 `tg:stats`(每次流转时 Lua 顺手 HINCRBY 维护),`QueueLen` 就是 `LLEN + ZCARD`,都是计数器/长度读取,不扫全库。

### 测试与限制

- 契约测试双档:miniredis 档离线进 CI;设 `TASKGATE_REDIS_ADDR=127.0.0.1:6379` 后同一套 17 条契约在真 Redis 上再跑一遍(随机 KeyPrefix 隔离、测后清理),验证 Lua 脚本兼容性。
- **不支持 Redis Cluster**:脚本内用前缀自行拼键、键不带 hash tag,面向单实例/主从/哨兵拓扑。
- 限流键与任务键在同一个 Redis 实例:flushdb 级故障两者同生共死(诚实的取舍)。

## Config 说明

库自己不读任何配置文件。`Config` 的字段带 `yaml`/`json` tag,应用自己 unmarshal 好再传进来:

```yaml
# 应用自己的配置文件(taskgate 不读它,由应用 unmarshal 后注入)
queues:
  llm:
    workers: 2       # 并发上限(必填,>=1)
    rps: 3           # 每秒放行数,0 = 不限速
    burst: 3         # 突发额度,0 时取 max(1, int(rps))
    lease_ttl: 60s   # 租约时长,0 补默认 60s
  cpu:
    workers: 4
routes:              # 任务类型 → 队列;没配的类型用类型名当队列名
  generate: llm
default_queue:       # 兜底队列,可整个不配
  workers: 2
lease_lost_max: 3    # 租约丢失封顶(默认 3),超过进 failed
throttled_max: 100   # 限流重排封顶(默认 100),超过进 failed
```

```go
var cfg taskgate.Config
_ = yaml.Unmarshal(raw, &cfg)      // 应用自己解;Duration 字段支持 "60s"、"10m" 写法
cfg.Broker = memorybroker.New()    // 运行期对象手动注入
cfg.OnStateChange = func(t taskgate.Task) { /* 埋点 */ }
g, err := taskgate.New(cfg)
```

## 设计文档

- [任务排队限流库 taskgate 方案](docs/plans/2026-07-14-任务排队限流库taskgate方案.md)(架构、Broker 合同、状态机)
- [测试方案](docs/plans/2026-07-14-测试方案.md)(分层测试、故障专项与性能基线)
- spec-kit 产物:[specs/001-m1-core-queue/](specs/001-m1-core-queue/)(M1)、[specs/002-m2-redis/](specs/002-m2-redis/)(M2)

## 里程碑

- **M1(已完成)**:核心排队、限流、重试、依赖、取消、Shutdown,memory / sqlite 双后端。
- **M2(已完成)**:redis 后端(Lua 原子流转、多进程恰好执行一次)、分布式限流(跨进程共享配额)、性能基线。
- **M3(待办)**:webhook 通知等重语义、List 分页、任务优先级、handler 手动续租等。明确不做:Web UI、cron 周期调度、DAG 工作流引擎、独立 server 模式。
