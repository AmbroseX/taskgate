# Quickstart: Identity 身份模型开发指南

## 前置条件

- Go 1.25;零新依赖
- 离线可跑:memory/sqlite/redis(miniredis)全绿即可提交
- 门控档:`TASKGATE_PG_DSN` / `TASKGATE_MYSQL_DSN` 有值时跑真库契约(CI service container 必跑)

## 开发顺序(严格按序,宪法 II.3)

1. **根包类型**:Task 加字段、WithBusinessKey/WithID 别名、ReplayRequest/ReplayOption、四个错误、Filter.BusinessKey、Broker 接口 +Replay
2. **brokertest 契约**:契约 2 重写(BusinessKeyIdempotent)+ 新增 cases_identity.go(见 contracts/)——此时全后端编译红,属预期
3. **memorybroker**(语义参考实现)→ 跑 brokertest 绿
4. **sqlitebroker**(schema + enqueue 键校验 + replay.go + query)→ 绿
5. **redisbroker**(common.lua 键工具 + enqueue.lua + replay.lua + query)→ miniredis 绿
6. **internal/sqlbroker + 两方言**(DDL 差异 + FOR UPDATE 串行化 + 约束名翻译)→ 本地有 DSN 则验,否则交 CI
7. **Gate 接线**:Submit 传 BusinessKey、Replay/ReplayByKey/History、集成测试
8. **e2e + README**:cron 配方场景重写

## 关键骨架

### brokertest 新契约用例(节选)

```go
// caseBusinessKeyIdempotent 契约 2(重写):同键二次入队一律拒,错误可解构出链尾。
func caseBusinessKeyIdempotent(t *testing.T, h *harness) {
    a := h.enqueue(t, taskBK("", "t", "", "key-1")) // ID 留空由 broker 生成
    tk := taskBK("", "t", "", "key-1")
    err := h.b.Enqueue(context.Background(), tk)
    if !errors.Is(err, taskgate.ErrTaskExists) { t.Fatalf("同键二次入队应拒: %v", err) }
    var te *taskgate.TaskExistsError
    if !errors.As(err, &te) || te.ExecutionID != a.ID { t.Fatalf("错误必须携带链尾信息: %+v", te) }
}
```

### sqlite Replay 事务要点

```go
// 链尾定位与校验、INSERT 全在 withTx 内;INSERT 撞 uq_replay_of → ErrAlreadyReplayed
row := tx.QueryRowContext(ctx, `SELECT `+taskCols+` FROM tasks
    WHERE business_key = ? ORDER BY created_at DESC, id DESC LIMIT 1`, key)
```

### redis replay.lua 返回约定

```text
成功: {newTaskStatus}            -- Go 侧回填新 Task
失败: TGERR:notfound / TGERR:notfinal:<status> / TGERR:replayed / TGERR:completed
```

## 测试步骤

```bash
go build ./... && go vet ./...
go test ./... -race                          # memory/sqlite/miniredis 全绿
TASKGATE_REDIS_ADDR=127.0.0.1:6379 go test ./redisbroker/ -race   # 可选真 redis
TASKGATE_PG_DSN=... TASKGATE_MYSQL_DSN=... go test ./... -race    # 门控档
go test ./... -cover                         # 核心 ≥85%,broker ≥80%
```

## 验收检查点

- [ ] brokertest 全契约五后端通过(pg/mysql 至少 CI 过)
- [ ] SC-001 原型五断言契约化且绿
- [ ] SC-002 并发竞态:同键 100 并发 Submit 恰 1 成功;同目标 100 并发 Replay 恰 1 成功(-race)
- [ ] SC-005 e2e cron 配方:被拒 → errors.As 拿链尾 → Replay → 跑完
- [ ] SC-006 旧库文件升级后可读可调度(sqlite 存量升级用例)
- [ ] `WithID` 编译兼容且 Deprecated 注释齐;README cron 配方重写
- [ ] `docs/plans/2026-07-15-MySQL-PG后端适配方案.md`"接口一行不改"结论补修订注记
