# Quickstart: taskgate M2 开发指南

## 前置条件

- M1 已全绿(brokertest 17 条、L3、专项);Broker 接口与契约**禁改**
- 新依赖只允许:`github.com/redis/go-redis/v9`、`github.com/go-redis/redis_rate/v10`、`github.com/alicebob/miniredis/v2`(仅测试)
- 真 Redis 档可选:`TASKGATE_REDIS_ADDR=127.0.0.1:6379`(没有就 skip)

## 开发顺序(每步 go build + 相关包测试绿再走下一步)

1. **能力接口**:根包 QueueLimiter/LimiterProvider;`limiter` 改名 `localLimiter` 实现 QueueLimiter;scheduler 装配点(断言 LimiterProvider,否则 local)——此步单机后端全量回归必须零失败
2. **redisbroker 骨架**:Options/New/Init/Close、键名工具、Lua 加载(go:embed + redis.NewScript)、hash↔Task 编解码
3. **enqueue.lua + claim.lua + Dequeue 循环**:先过 brokertest 前 5 条(RoundTrip/IdempotentID/ClaimMutex/BlockingDequeue/DelayedTask)
4. **finish.lua(五个 op)+ heartbeat.lua + reap.lua**:过完整 17 条契约(miniredis)
5. **真 Redis 档**:broker_test.go 加 env 门控第二档
6. **分布式限流器**:sem_acquire.lua + redis_rate 接入 + L1 测试(单进程内多 Gate 共享 miniredis 验证全局语义)
7. **L3 集成**:integration_test.go 后端参数化加 redis 档
8. **多进程专项**:multiproc_test.go(双进程恰好一次、kill -9、跨进程流水线/取消、断连恢复;-short 跳过)
9. **基准**:bench_test.go 三项,跑一轮把数字回写测试方案第 6 节

## 关键骨架

### brokertest 接入(两档)

```go
func TestBrokerContract(t *testing.T) {
    brokertest.Run(t, func(t *testing.T, opts taskgate.BrokerOptions) taskgate.Broker {
        mr := miniredis.RunT(t)
        b, err := redisbroker.New(redisbroker.Options{Addr: mr.Addr()})
        // Init(opts) 由套件/工厂约定完成,照 memorybroker/broker_test.go 的现有写法
        ...
    })
}
func TestBrokerContractRealRedis(t *testing.T) {
    addr := os.Getenv("TASKGATE_REDIS_ADDR")
    if addr == "" { t.Skip("TASKGATE_REDIS_ADDR 未设置") }
    // 同一 factory,换 addr;每个用例用随机 KeyPrefix 隔离并测后清理
}
```

### Lua 调用模式(时间注入)

```go
//go:embed lua/claim.lua
var claimSrc string
var claimScript = redis.NewScript(claimSrc)
// 调用:全部时间从 clock 来,脚本内禁 TIME
res, err := claimScript.Run(ctx, b.rdb, keys, b.clk.Now().UnixMilli(), newULID(), ...).Result()
```

### scheduler 装配点(唯一的上层改动)

```go
var lim QueueLimiter
if lp, ok := g.cfg.Broker.(LimiterProvider); ok {
    lim, err = lp.QueueLimiter(queue, qc)   // 分布式:多进程共享配额
} else {
    lim = newLocalLimiter(qc.Workers, qc.RPS, qc.Burst)
}
```

## 测试步骤

```bash
go test ./... -race -count=1                 # 全量(miniredis,离线)
go test ./... -race -short                   # 跳过多进程/崩溃专项
TASKGATE_REDIS_ADDR=127.0.0.1:6379 go test ./redisbroker/... -race   # 真 Redis 档
go test -bench . -benchmem -run '^$' .       # 基准,数字回写测试方案第 6 节
```

## 验收检查点(M2 完成定义,对齐测试方案第 9 节)

- [ ] brokertest 17 条在 miniredis 全绿;真 Redis 档(env 门控)全绿
- [ ] 单机后端(memory/sqlite)全部 M1 测试零回归
- [ ] L3 集成测试 redis 档全绿
- [ ] 双进程 1000 任务恰好各执行一次;kill -9 回收重跑;跨进程流水线唤醒;跨进程 Cancel 心跳周期内生效;断连恢复不丢任务
- [ ] 分布式限流:双 Gate {Workers:2} 全局并发 ≤2;{RPS:10} 1 秒全局 10±2;崩溃槽 ≤2×LeaseTTL 回收
- [ ] 覆盖率:redisbroker ≥80%,核心包维持 ≥85%
- [ ] 基准基线数字已回写 docs/plans/2026-07-14-测试方案.md 第 6 节
- [ ] `go test ./... -race -count=1` 连跑 3 遍无偶发;gofmt/vet 干净
