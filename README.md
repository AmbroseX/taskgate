# taskgate

一个 `go get` 即用的 Go 任务排队限流库:排队、限流、重试、依赖、取消、优雅停止。它是库不是服务——没有 Web UI、不读环境变量和配置文件,只吃你传进来的 `Config`。

典型场景:调用 LLM / OCR 这类有配额的外部网关时,把请求收进队列,按类型隔离限流,失败自动退避重试,进程崩了靠租约把任务捞回来。

## 特性

- **类型级限流**:每个队列独立的 `{Workers, RPS, Burst}`,慢队列绝不拖累快队列;`Routes` 支持多类型共享同一队列(共享同一个网关配额)
- **两种后端**:`memorybroker`(内存,零依赖)和 `sqlitebroker`(单文件落盘,纯 Go 免 cgo);同一套行为契约(`brokertest` 16 条)双后端验收
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

依赖只有三个:`modernc.org/sqlite`(纯 Go)、`golang.org/x/time/rate`、`github.com/oklog/ulid/v2`。

## 快速开始

三级流水线:检索 → 生成 → 打分(完整可跑版本见 [examples/llm](examples/llm/main.go),`go run ./examples/llm`)。

```go
g, err := taskgate.New(taskgate.Config{
    Broker: memorybroker.New(), // 或 sqlitebroker.Open("tasks.db")
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
- [测试方案](docs/plans/2026-07-14-测试方案.md)(分层测试与故障专项)
- spec-kit 产物:[specs/001-m1-core-queue/](specs/001-m1-core-queue/)

## M1 范围

当前是 M1:核心排队、限流、重试、依赖、取消、Shutdown,memory / sqlite 双后端。Redis 后端(跨进程分布式限流)留给 M2;webhook 通知等重语义留给 M3。明确不做:Web UI、cron 周期调度、DAG 工作流引擎、独立 server 模式。
