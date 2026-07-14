# Quickstart: taskgate M1 开发指南

## 前置条件

- Go ≥ 1.25;无需 Redis、无需 cgo
- 依赖只允许:`modernc.org/sqlite`、`golang.org/x/time/rate`、`github.com/oklog/ulid/v2`
- 初始化:`go mod init github.com/ambrose/taskgate`

## 开发顺序(严格按此推进,每步 go build ./... 通过再走下一步)

1. **类型定义**:taskgate.go(Task/Status/canTransition/Config/QueueConfig/Duration/选项)+ errors.go + clock.go + internal/fakeclock
2. **Broker 接口**:broker.go(接口+BrokerOptions+FailKind+Filter,照 contracts/broker-contract.md 抄)
3. **brokertest 契约**:brokertest/suite.go,16 条用例全部写完(此时没有实现,套件本身要能编译)
4. **memory 后端**:memorybroker/,跑 brokertest 到全绿(语义参考实现)
5. **sqlite 后端**:sqlitebroker/ + schema.sql,跑同一套 brokertest 到全绿
6. **limiter + backoff**:limiter.go(信号量+令牌桶)、backoff.go,配 L1 单元测试
7. **deps 纯逻辑**:deps.go(唤醒/传播决策纯函数)+ L1 测试(两后端事务内已用到的公共计算)
8. **scheduler**:认领循环、worker 池、自动续租(LeaseTTL/3)、reaper 循环、重试编排(FailKind 判定)、Cancel ctx 管理、Shutdown
9. **client**:New(校验)/Handle/Run/Submit(Routes 路由)/Get/Cancel/List/Stats/Overview/Wait
10. **L3 集成测试**:integration_test.go 全场景(用 memory+sqlite、fakeclock 或短租约)
11. **专项**:crash_test.go(kill -9 崩溃恢复、唤醒中途崩注入点)
12. **示例**:examples/llm/main.go 三级流水线

## 关键骨架

### handler 与注册

```go
type Handler func(ctx context.Context, t *Task) ([]byte, error)
func (g *Gate) Handle(taskType string, h Handler)
```

### 提交选项(函数式)

```go
func WithID(id string) SubmitOption
func Delay(d time.Duration) SubmitOption
func RunAt(t time.Time) SubmitOption
func MaxRetry(n int) SubmitOption
func DependsOn(ids ...string) SubmitOption
func IgnoreParentFailure() SubmitOption   // 默认 FailFast
```

### brokertest 接入(每个后端一行)

```go
func TestBroker(t *testing.T) {
    brokertest.Run(t, func(t *testing.T, clk taskgate.Clock) taskgate.Broker {
        b := memorybroker.New()
        // Init 由套件统一调,注入 clk 与默认上限
        return b
    })
}
```

### 调度器主循环要点

- 每队列独立 goroutine 组:`获取并发槽 → 限流器 Wait → Dequeue → 起 worker goroutine(执行 handler)+ 心跳 goroutine(LeaseTTL/3 Heartbeat,ErrTaskCanceled 则 cancel ctx)`
- handler 返回值 → errors.As 判定 ErrThrottled/ErrSkipRetry → 调 Ack/Fail(kind, retryAt)
- 全局 reaper goroutine:每 LeaseTTL/2(取最小队列 TTL)调一次 ReapExpired
- Shutdown:关认领 → 等 worker WaitGroup(ctx 超时则 cancel 各任务 ctx,等 handler 退出后 Requeue)→ 停 reaper → 返回

## 测试步骤

```bash
go build ./...
go vet ./...
go test ./... -race            # L1+L2+L3+专项全量
go test ./... -race -short     # 跳过慢用例(kill -9 子进程类)
go test ./... -cover           # 覆盖率:核心包 ≥85%,broker ≥80%
go run ./examples/llm          # 三级流水线示例
```

## 验收检查点(M1 完成定义)

- [X] L1 全绿:limiter 精度(RPS=10→1s 放行 10±1)、退避曲线±20%、配置校验、状态机表、依赖计数
- [X] brokertest 16 条契约在 memory 与 sqlite 双后端全绿
- [X] L3 全场景:提交→执行→结果、Wait 超时、重试链路、ErrThrottled、ErrSkipRetry、毒任务封顶、取消 running、三级流水线、fan-in、父已完成提交、限流隔离、Shutdown 正常/超时、统计一致
- [X] 专项:kill -9 崩溃恢复(LeaseLost=1 最终 completed)、唤醒中途崩不丢唤醒
- [X] `go test ./... -race` 零 data race;覆盖率达标(核心 91.0%、memorybroker 85.8%、sqlitebroker 85.8%)
- [X] examples/llm 跑通
- [X] 宪法取证:无 interface{} 进模型、无后端特判、无 env 读取(2026-07-14 grep 取证通过)
