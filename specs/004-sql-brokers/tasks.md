# Tasks: SQL 后端适配(MySQL / PostgreSQL)

**Input**: Design documents from `/specs/004-sql-brokers/`
**Prerequisites**: plan.md、spec.md、research.md、data-model.md、quickstart.md(接口零改动,沿用现成 brokertest 18 条契约)
**前置已完成**: 宪法 v1.2.0 已批准 pgx/v5、go-sql-driver/mysql 依赖 + 测试门控例外(第 V.5 条)

**Tests**: L2(brokertest×PG / ×MySQL,env 门控)、L3(多进程加 SQL 档 + 并发 propagate 压测专项)、CI service container(postgres:16 / mysql:8)。

**Organization**: Phase 1 建共享核心 `internal/sqlbroker`(零驱动依赖,两故事共用地基);Phase 2 = PG(MVP);Phase 3 = MySQL;Phase 4 = 多进程/压测;Phase 5 = CI/文档。sqlbroker 内互不冲突的文件标 [P]。

## Format: `[ID] [P?] [Story] Description`

## Path Conventions

- 共享核心:`internal/sqlbroker/`(broker.go、dialect.go、enqueue.go、dequeue.go、lifecycle.go、propagate.go、query.go)
- PG 薄壳:`pgbroker/`(broker.go、dialect.go、driver.go、broker_test.go)
- MySQL 薄壳:`mysqlbroker/`(broker.go、dialect.go、driver.go、broker_test.go)
- 根包测试:`multiproc_test.go`;CI:`.github/workflows/ci.yml`

---

## Phase 1: Setup — 共享核心 internal/sqlbroker 骨架(两故事地基,零驱动依赖)

**Purpose**: 从 sqlitebroker 迁移出 database/sql 通用实现 + Dialect 接口。此阶段无真库可测,靠 Phase 2 的 PG 契约首次验证;先保证 `go build ./...` + `go vet` 通过。

- [x] T401 [P] `internal/sqlbroker/dialect.go`:定义 `Dialect` 接口(8 方法:Name/Rebind/SchemaSQL/Claim/IsDuplicateKey/Retryable/Lock/Unlock)、`RetryClass` 三态枚举(NotRetryable/RetryImmediate/RetryLimited)、`ClaimParams` 结构;每个方法写 godoc 注明"errors.As 禁字符串匹配""1205≠1213 分档"等纪律(research 第 3 节)
- [x] T402 `internal/sqlbroker/broker.go`:`Broker` 结构(db/dialect/mu/wakeCh/clk/cfg/inited/closed)、`Config`(TablePrefix/MaxOpenConns=10/PollInterval=200ms/MaxTxRetry=5)、`New(dialect,db,cfg)`、`Init`(补默认 + 建表走 Lock/Unlock 独占连接,research 第 5 节)/`Close`、导出 `Rec`/`RowScanner`/`ScanRec`(照 sqlitebroker broker.go:199-246 迁移)/`TaskCols()`/`placeholders`/`ms/fromMS`;换代式 `wakeAll/wakeChan` 原样搬(broker.go:135-144)
- [x] T403 `internal/sqlbroker/broker.go` withTx 重试环:实现 research 第 1 节的重试环(BeginTx→fn→按 `dialect.Retryable` 三态决定回滚重跑/原样返回,退避走注入 clk);**硬纪律注释**:fn 无副作用可重跑、Notify/wake 禁入环(fn 只收集通知快照切片,withTx 成功后调用方才发)
- [x] T404 [P] `internal/sqlbroker/enqueue.go`:Enqueue(父存在校验 + 落 tasks/task_deps + 初始状态/计数 + 撞键经 `dialect.IsDuplicateKey` 翻译 `taskgate.ErrTaskExists`);SQL 只用不复用位置 `?`,过 `dialect.Rebind`
- [x] T405 [P] `internal/sqlbroker/lifecycle.go`:Ack/Fail/Cancel/FinishCanceled/Requeue/Heartbeat(照 sqlitebroker 迁移,令牌校验→getRecForUpdate→判状态→改状态→收集通知快照;终态调 propagateTx 同事务)
- [x] T406 [P] `internal/sqlbroker/propagate.go`:单事务 BFS 工作队列连锁传播(逐字照抄 sqlitebroker/propagate.go:16,childrenOf 先读全子 ID 再逐个 DecideOnParentFinal;标准 SQL 过 Rebind)
- [x] T407 [P] `internal/sqlbroker/query.go`:Get/List/QueueLen/Counts(只读走连接池无锁,FR-012)+ ReapExpired;**展开 `?N` 复用**——sqlitebroker/query.go:129-133(nowMS×2)、160-168(LeaseLostMax×4+nowMS×2)改成位置 `?` 重复传值(research 第 2 节);ReapExpired 扫描加 `FOR UPDATE SKIP LOCKED`,认领/回收的 RETURNING 依赖抽成 `dialect.Claim` 之外的可两版实现点(PG RETURNING / MySQL SELECT+UPDATE)
- [x] T408 `internal/sqlbroker/dequeue.go`:Dequeue 三唤醒源循环(wakeChan/注入 clk 到点等待/ctx)+ 认领委托 `dialect.Claim(ctx,tx,ClaimParams)` + 认领后第二条补 lease_until(照 dequeue.go:116-117)+ 无就绪任务 `SELECT MIN(run_at)` 算下次到点;出错先查 `ctx.Err()` 统一返回(FR-021)

**Checkpoint**: `go build ./... && go vet ./...` 通过;核心逻辑就绪待 PG 方言点亮。

---

## Phase 2: User Story 1 - PostgreSQL 后端过全部契约 (Priority: P1) 🎯 MVP

**Goal**: pgbroker 通过 brokertest 18 条(TASKGATE_PG_DSN 门控)。

**Independent Test**: 无 DSN 时 `go test ./pgbroker/...` skip 全绿;设 TASKGATE_PG_DSN 指向 PG 9.5+ 后 18 条全绿。

- [x] T409 [US1] `pgbroker/driver.go` blank import `_ "github.com/jackc/pgx/v5/stdlib"` + `pgbroker/dialect.go`:`pgDialect` 实现 sqlbroker.Dialect——Rebind(`?`→`$n`)、SchemaSQL(BYTEA/BIGINT/TEXT + 带前缀表名索引名,data-model.md)、Claim(UPDATE 子查询 FOR UPDATE SKIP LOCKED RETURNING,一条 + lease_until 第二条,research 第 4 节)、IsDuplicateKey(errors.As `*pgconn.PgError` SQLSTATE 23505)、Retryable(40P01/40001→RetryImmediate)、Lock/Unlock(pg_advisory_lock,key 字符串 hash 成 bigint);新依赖 pgx/v5
- [x] T410 [US1] `pgbroker/broker.go`:`Options{TablePrefix,MaxOpenConns,PollInterval}`(带 yaml/json tag)+ 函数式 `WithTablePrefix/WithMaxOpenConns/WithPollInterval` + `Open(dsn, opts...)`(sql.Open("pgx",dsn)→SetMaxOpenConns→New(pgDialect,db,cfg));返回 *sqlbroker.Broker
- [x] T411 [US1] `pgbroker/broker_test.go`:`brokertest.Run(t, factory)`,TASKGATE_PG_DSN 门控(空则 t.Skip,照 redisbroker/broker_test.go:37-54)+ 随机表前缀隔离 + t.Cleanup DROP 清理;**18 条契约全绿**;补 TestUseBeforeInit 对齐

**Checkpoint**: MVP——第 4 个后端(PG)契约全绿(设 DSN 时);`go test ./... -race` 本地无 DSN 全绿(skip)。**STOP and VALIDATE**。

---

## Phase 3: User Story 2 - MySQL 后端过全部契约 (Priority: P2)

**Goal**: mysqlbroker 通过 brokertest 18 条(TASKGATE_MYSQL_DSN 门控)+ 长度校验;复用 PG 已验证的核心,只换方言。

**Independent Test**: 无 DSN skip;设 TASKGATE_MYSQL_DSN 指向 MySQL 8.0+ 后 18 条全绿 + 256 字符 ID 报错用例通过。

- [x] T412 [US2] `mysqlbroker/driver.go` blank import `_ "github.com/go-sql-driver/mysql"` + `mysqlbroker/dialect.go`:`mysqlDialect` 实现 Dialect——Rebind(原样返回)、SchemaSQL(VARCHAR(255)/LONGBLOB/BIGINT + **COLLATE utf8mb4_bin** + 带前缀名,data-model.md)、Claim(**两步式** SELECT ... FOR UPDATE SKIP LOCKED + UPDATE 一步写全 lease_until,research 第 4 节)、IsDuplicateKey(errors.As `*mysql.MySQLError` errno 1062)、Retryable(1213→RetryImmediate;1205→RetryLimited)、Lock/Unlock(GET_LOCK/RELEASE_LOCK 钉独占连接);新依赖 go-sql-driver/mysql
- [x] T413 [US2] `mysqlbroker/broker.go`:Options + `Open(dsn, opts...)`;连接建立后 `SET SESSION innodb_lock_wait_timeout=5`(DSN 参数或连接钩子,配合 1205 分档);**Enqueue 入口长度校验**——ID/type/queue 超 255 字符返回清晰错误不落库(FR-015,参照 redisbroker validateID 但补长度);校验放薄壳 Open 返回的包装或核心留 hook 由 dialect 提供
- [x] T414 [US2] `mysqlbroker/broker_test.go`:brokertest.Run,TASKGATE_MYSQL_DSN 门控 + 随机前缀 + 清理;**18 条全绿**;新增 `TestEnqueueRejectsOversizeID`(256 字符 ID/type/queue 各报错、255 字符放行、验证未落库,SC-007);验证 utf8mb4_bin 下 "abc"/"ABC" 视为不同任务(SC 用例)

**Checkpoint**: 两后端契约全绿(各自 DSN);`go test ./... -race` 本地 skip 全绿。

---

## Phase 4: User Story 3 - 高并发依赖传播不丢错(死锁重试环验证) (Priority: P3)

**Goal**: 多进程/多 goroutine 压测证明死锁被重试环吸收;复用现有专项覆盖 kill -9/恰好一次/断连。

- [x] T415 [US3] `multiproc_test.go`:DSN 存在时把 PG/MySQL 追加进 `forEachBackend` 系列(kill -9 回收计 LeaseLost、双进程恰好一次、断连恢复复用 tcpProxy);无 DSN 该档 skip
- [x] T416 [US3] `multiproc_test.go` 并发 propagate 压测专项 `TestConcurrentPropagate`(env 门控):构造共享子树,N≥8 goroutine 同时打 Cancel/Fail/ReapExpired,断言**零错误抛给调用方 + 全部任务终态一致**(SC-005);重复跑验证死锁被重试环吃掉而非漏出;验证幻通知不发生(通知计数 = 实际终态数)

**Checkpoint**: SC-005/SC-006 成立;重试环有自动化证明。

---

## Phase 5: Polish — CI、文档、验收

- [x] T417 [P] `.github/workflows/ci.yml`:加 `test-pg`(service postgres:16,注入 TASKGATE_PG_DSN,`go test -race ./pgbroker/... ./...`)、`test-mysql`(service mysql:8 带 utf8mb4,注入 TASKGATE_MYSQL_DSN)两 job,照现有 `test-redis` 模板
- [x] T418 [P] README(中英)加 PG/MySQL 后端快速开始 + 已知限制(quickstart.md:跨进程 200ms 延迟、不做分布式限流、MySQL 255 长度上限/max_allowed_packet、本地零覆盖回归靠 CI);导出符号 godoc 补全(sqlbroker/pgbroker/mysqlbroker)
- [x] T419 验收(设 DSN 环境):brokertest 18 条 PG/MySQL 全绿(SC-001/002)、`go test ./... -race` 无 DSN skip 全绿(SC-003)、覆盖率 sqlbroker/pgbroker/mysqlbroker 各 ≥80%(SC-004)、gofmt/vet 干净;宪法取证 grep(SC-008/009:broker.go/scheduler.go/client.go 零改动、上层无具体后端断言)
- [x] T420 完成记录写 `docs/plans/2026-07-16-spec004-SQL后端完成记录.md`(交付/验收/裁决/已知限制);更新 memory taskgate 状态——git 提交由主控做

---

## Dependencies & Execution Order

- Phase 1 → 2 → 3 → 4 → 5 大体串行:Phase 2(PG)先点亮核心,Phase 3(MySQL)复用已验证核心只换方言,Phase 4 依赖两后端存在。
- Phase 1 内 T401→T402→T403 串行(结构+withTx 是地基),T404~T408 依赖 T402/T403 但彼此不同文件可 [P] 并行编写(dequeue T408 依赖 dialect.Claim 签名 T401)。
- Phase 5 T417/T418 不同文件可并行;T419/T420 收尾串行。

### Parallel Opportunities

```
Phase 1: T401 先行;T404/T405/T406/T407 [P](enqueue/lifecycle/propagate/query 各自文件)
Phase 5: T417(CI yml)/T418(README+godoc)[P]
```

---

## Implementation Strategy

1. Phase 1 建核心,`go build`/`go vet` 绿即推进(真库验证留给 Phase 2)。
2. Phase 2 完成即 MVP(PG 契约全绿),STOP and VALIDATE。
3. 之后每 Phase 结束跑 `go test ./... -race`(本地 skip 档 + 有 DSN 时全跑)+ git 提交。
4. 本地无真库时靠 CI 兜底——牢记 V.5 门控代价:本地绿不代表跑过 PG/MySQL。

## Task Summary

| 阶段 | 任务数 | 可并行 |
|------|--------|--------|
| P1 Setup(共享核心) | 8 | 4 |
| P2 US1 PG(MVP) | 3 | 0 |
| P3 US2 MySQL | 3 | 0 |
| P4 US3 多进程/压测 | 2 | 0 |
| P5 Polish | 4 | 2 |
| **Total** | **20** | **6** |

**MVP Scope**: Phase 1 + Phase 2 = 11 tasks(共享核心 + PG 契约全绿)
