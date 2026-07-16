# Quickstart: 周期配额开发指南

## 前置条件

- Go 1.25;零新依赖;miniredis v2.38 已确认支持 `TIME` + `SetTime`(redis 介质时间缝)
- pg/mysql 走 DSN 门控;本地 MySQL(root@127.0.0.1)可用于真机验证

## 开发顺序

1. **根包类型**:QueueConfig 三字段 + validate;QuotaProvider/QuotaGate/QuotaReservation;QueueStats 两个位;newGate 能力断言
2. **RunQuota 合规套件**(brokertest/quota.go)——先写套件,后端全红属预期
3. **memorybroker**(语义参考:单锁 + 注入钟)→ RunQuota 绿
4. **sqlitebroker**(quota 表 + 单语句 Reserve + 测试时间钩子)→ 绿
5. **redisbroker**(quota_reserve/release.lua + miniredis SetTime)→ 绿
6. **internal/sqlbroker + 两方言**(PG 单语句 / MySQL 两步)→ 本地 MySQL 真机验
7. **scheduler 接线**:claimLoop 四环 + quotaState + Stats;L3 集成(stub 配额闸测 fail-closed;双 Gate 共享 sqlite)
8. **e2e + 基准 + README**

## 关键骨架

### RunQuota 用例风格

```go
// 硬上限:N 次成功后必须耗尽;释放一份后恰好能再预留一份。
func caseQuotaHardLimit(t *testing.T, h *quotaHarness) {
    for i := 0; i < h.limit; i++ {
        if r := h.reserve(t); r == nil { t.Fatalf("第 %d 份预留应成功", i+1) }
    }
    if r, err := h.gate.Reserve(ctx); err != nil || r != nil {
        t.Fatalf("超额预留应返回 (nil, nil) 耗尽态,得到 %v/%v", r, err)
    }
}
```

### scheduler claimLoop 四环(伪码)

```go
lim.AcquireSlot(ctx); lim.WaitToken(ctx)
if qg != nil {
    if n, _ := broker.QueueLen(ctx, queue); n == 0 { lim.ReleaseSlot(); sleep(退避); continue }
    res, err := qg.Reserve(ctx)
    if err != nil { st.stalled.Store(true); lim.ReleaseSlot(); sleep(退避); continue }
    if res == nil { st.exhausted.Store(true); lim.ReleaseSlot(); sleep(退避); continue }
    st.exhausted.Store(false); st.stalled.Store(false)
    dctx, cancel := context.WithTimeout(ctx, dequeueBound)      // 安全界
    t, err := broker.Dequeue(dctx, []string{queue}); cancel()
    if err != nil { _ = qg.Release(bg, res); lim.ReleaseSlot(); ...; continue }
}
```

### sqlite Reserve 三态判定

```go
row := db.QueryRowContext(ctx, reserveSQL, qkey, override, periodSec, periodSec, limit)
switch err := row.Scan(&win); {
case err == nil:                    return &QuotaReservation{Window: win}, nil
case errors.Is(err, sql.ErrNoRows): return nil, nil        // 耗尽,非错误
default:                            return nil, err        // fail-closed
}
```

## 测试步骤

```bash
go build ./... && go vet ./...
go test ./... -race                                             # 离线全绿(RunQuota: memory/sqlite/miniredis)
TASKGATE_MYSQL_DSN='root@tcp(127.0.0.1:3306)/taskgate_test' go test ./mysqlbroker/ . -race
go test -bench 'Quota' -benchtime 2s ./...                      # 基准:有/无配额吞吐对比
```

## 验收检查点(对齐 spec SC)

- [ ] RunQuota 五后端过(pg 走 CI);`QuotaLimit=0` 全量既有测试零回归(SC-004)
- [ ] SC-001 双 Gate 共享 sqlite 文件每窗恰好 N(时间钩子驱动,不真 sleep)
- [ ] SC-002 预留后不认领 → 只少不多;SC-003 stub 闸故障期零放行 + QuotaStalled 可见
- [ ] SC-005 e2e 组合 `{Workers:2,RPS:3,Quota:10/窗}` 三维度同时成立
- [ ] 基准数据写进完成记录;损耗 >30% 时附备选路线重评结论(SC-007)
- [ ] README:频率 ≠ 配额 + 能力声明收窄口径(FR-014)
