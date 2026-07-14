# Quickstart: taskgate M3 开发指南

## 前置条件

- M1/M2 全绿;零新依赖(mockgw 用 net/http/httptest,随机用 math/rand 固定种子)
- 公开 API 只增不改;Broker 接口签名禁改

## 开发顺序

1. **List 分页**(Broker 行为变更,契约先行):
   a. broker.go Filter 加 Offset + 排序合同注释;修订 specs/001-m1-core-queue/contracts/broker-contract.md(Get/List 段 + 用例表加第 18 条)
   b. brokertest 加 ListPagination 契约(25 任务/3 页/并集无重无漏/顺序=(CreatedAt,ID) 升序/越界空/过滤+分页组合);此时三后端应全红
   c. memory → sqlite → redis 依次实现到全绿
2. **手动续租**:errors.go 加 ErrNoTask → client.go 加 RenewLease+ctx key → scheduler.go execute 注入闭包 + ManualHeartbeat 短路心跳 → taskgate.go QueueConfig 加字段 → integration_test.go 三场景(自动档互不干扰/手动档保活 3×TTL/不续被回收+过期令牌 ErrLeaseLost+非任务 ctx ErrNoTask)
3. **e2e/mockgw + L4 五用例**:mockgw.go(故障开关+原子观测)→ pipeline_test.go 五用例(memory 后端)
4. **realgw 冒烟档**:e2e/realgw_test.go(//go:build realgw,env 缺失 skip)
5. **收尾**:README(RenewLease/ManualHeartbeat/分页/L4 说明)、godoc、验收全表、完成记录

## 关键骨架

### RenewLease

```go
// client.go
type renewFunc func() error
type ctxKeyRenew struct{}
func RenewLease(ctx context.Context) error {
    fn, ok := ctx.Value(ctxKeyRenew{}).(renewFunc)
    if !ok { return ErrNoTask }
    return fn()
}
// scheduler.execute 里:taskCtx = context.WithValue(taskCtx, ctxKeyRenew{}, renewFunc(func() error {...}))
```

### ListPagination 契约要点

```go
// 25 个任务(掺不同 Type/Queue,CreatedAt 用 fakeclock 逐个 +1ms 保证全序)
// 三页 Limit=10 Offset=0/10/20 → 10/10/5;并集=全集;每页内部与跨页均升序
// Offset=100 → 空;Offset=5,Limit=0 → 剩余 20 条;Type 过滤+分页组合
```

### mockgw 用法(L4 用例 1)

```go
gw := mockgw.New(mockgw.Latency(50*time.Millisecond), mockgw.BusyAfterConcurrency(2))
defer gw.Close()
// handler:http.Post(gw.URL()) → 解析 body,busy 事件 → return taskgate.ErrThrottled{RetryAfter: 200*time.Millisecond}
// 断言:gw.MaxConcurrency() <= 2 && gw.BusyCount() == 0(Workers:2 档)
```

## 测试步骤

```bash
go test ./... -race -count=1          # 全量含 L4(e2e/)
go test ./e2e/... -race               # 只跑 L4
go vet ./...                          # realgw 档不编译(构建标签)
go test -tags realgw ./e2e/ -run RealGW -v   # 手动冒烟(需 LLM_GATEWAY_URL/KEY)
```

## 验收检查点(M3 完成定义)

- [x] brokertest 18 条契约三后端全绿(含新 ListPagination)
- [x] L4 五用例全绿且离线确定(固定种子;{W:2} 档 MaxConcurrency=2 且 BusyCount=0;{W:5} 档零 failed 且 Throttled≈2450 真实触发;流水线 30/30 逐份传递正确;中途取消连锁生效;SSE 藏错误 2 次重排后成功)
- [x] 手动续租三场景全绿(保活 3×TTL LeaseLost=0/不续被回收 LeaseLost=1/过期令牌 ErrLeaseLost;非任务 ctx ErrNoTask)
- [x] ManualHeartbeat=false 全量零回归
- [x] realgw 常规构建零引入(`go vet ./...` 与不带 tag 的 `go test` 不编译它,`-run RealGW` 报 no tests to run)
- [x] `go test ./... -race -count=1` 连跑 3 遍全绿;覆盖率核心 89.2%、memory 88.9%、sqlite 85.9%、redis 84.6%(核心 ≥85%、各后端 ≥80% 达标)
- [x] README/godoc 更新;完成记录写 docs/plans/2026-07-15-M3完成记录.md
