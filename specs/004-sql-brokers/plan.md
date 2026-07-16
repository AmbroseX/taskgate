# Implementation Plan: SQL 后端适配(MySQL / PostgreSQL)

**Branch**: `004-sql-brokers` | **Date**: 2026-07-16 | **Spec**: [spec.md](./spec.md)

## Summary

从 sqlitebroker 迁移出一份共享 SQL 核心 `internal/sqlbroker`(database/sql 通用实现,注入 `Dialect`),再挂两个薄壳公开包 `pgbroker`/`mysqlbroker`,各自 blank import 自己的驱动。相对 sqlite 蓝本新写两条机制:**withTx 内置死锁/序列化失败重试环**(sqlite 单连接不死锁,是照抄最大陷阱)与**占位符只用不复用**(sqlite 的 `?N` 复用写法只在 ReapExpired 两处,展开成位置参数)。Broker 接口一行不改,过现成 brokertest 18 条契约,env 门控 + CI service container 验收。

## Technical Context

**Language/Version**: Go 1.25
**Primary Dependencies**: 新增 `github.com/jackc/pgx/v5`(stdlib 模式,pgbroker 用)、`github.com/go-sql-driver/mysql`(mysqlbroker 用);均纯 Go 零 cgo,已随宪法 v1.2.0 批准。internal/sqlbroker 只依赖标准库 `database/sql`,不 import 任何驱动。
**Affected Backends**: 新增 pgbroker/mysqlbroker + internal/sqlbroker;Broker 接口零改动;memory/sqlite/redis/scheduler/client 零改动(只加测试接线)。
**Testing**: L2(brokertest×PG / ×MySQL,env 门控)+ L3(集成/多进程加 SQL 档)+ 新增并发 propagate 压测专项(死锁重试环唯一自动化验证)+ CI service container(postgres:16 / mysql:8)。
**Concurrency Semantics**: 认领互斥靠 `FOR UPDATE SKIP LOCKED`(MySQL 8.0+ / PG 9.5+);终态+唤醒原子性靠单事务;死锁窗口靠 withTx 重试环吸收;通知只在事务成功后发(防幻通知)。
**Performance Goals**: 无硬指标;跨进程新任务感知延迟 = 轮询间隔(默认 200ms 可调)。
**Constraints**: Broker 签名禁改;brokertest 契约语义禁改;上层禁对具体后端特判;共享 SQL 只用不复用位置 `?`;错误判定用 `errors.As` 禁字符串匹配;SQL 内不用 NOW()(时间全由 Go 层注入,保 fakeclock)。

## Constitution Check

| 原则 | 本计划遵循方式 | 结果 |
|------|---------------|------|
| I 库的边界 | 只加两 SQL 后端,不碰第 10 节禁区;新依赖 2 个纯 Go 零 cgo,已随 v1.2.0 批准登记 | ✅ |
| I 不做分布式限流/迁移框架 | LimiterProvider/LISTEN-NOTIFY/迁移框架均不做(YAGNI 边界,FR-022/023) | ✅ |
| II.1 接口最小公倍数 | Broker 接口零改动 | ✅ |
| II.2 上层不特判后端 | SQL 后端不实现 LimiterProvider,scheduler.go:151 探测不到自动退回 localLimiter,零改动零断言 | ✅ |
| II.3 brokertest 合规 | pgbroker/mysqlbroker 作为第 4、5 个 factory 接入,18 条契约一字不改全过 | ✅ |
| II.4 租约令牌 | 认领生成新令牌,Ack/Fail/… 校验令牌不符返回 ErrLeaseLost,语义照 sqlite | ✅ |
| III.1 终态+唤醒原子性 | 同一事务收敛(宪法 v1.1.0 认可的单事务整树形态,照抄 sqlite propagateTx) | ✅ |
| III.2 认领互斥 | FOR UPDATE SKIP LOCKED,同一行不被两 worker 认领 | ✅ |
| III.3 三计数分工 | 照抄 sqlite query.go 的 Attempts/LeaseLost/Throttled 语义,不改 | ✅ |
| III.4 连锁取消收敛 | 单事务 BFS 工作队列(照抄 propagate.go),死锁被重试环吃掉不漏错 | ✅ |
| IV 数据模型 | Payload/Result 仍 json.RawMessage;BIGINT 存毫秒;错误翻译成既有哨兵(ErrTaskExists 等) | ✅ |
| V 测试纪律 | env 门控 + CI service container(v1.2.0 第 V.5 门控例外);时间注入 clock 保 fakeclock;覆盖率新后端 ≥80% | ✅ |
| V.5 门控代价 | 明写:PG/MySQL 本地缺 DSN skip,回归唯一防线是 CI(已入宪法与已知限制) | ✅ |
| VI 文档纪律 | spec-kit 产物在 specs/004-sql-brokers/,完成后归档 docs/plans/ | ✅ |

**Constitution Check Result**: ✅ 通过(依赖与门控例外已在 v1.2.0 前置修宪批准)

## Project Structure

### Documentation (this feature)

```text
specs/004-sql-brokers/
├── spec.md
├── plan.md          # 本文件
├── research.md      # 6 个头号问题决策:重试环/占位符/Dialect 边界/认领 SQL/建表互斥/测试门控
├── data-model.md    # 表结构三库对照 + Dialect 接口 + Rec/Options 类型
└── quickstart.md    # pgbroker/mysqlbroker 起步用法 + 本地起库跑契约
```
contracts/ 不需要:接口零改动,沿用 001 的 broker 契约与 brokertest 18 条。

### Source Code (repository root)

```text
taskgate/
├── internal/sqlbroker/          # 共享核心,只依赖 database/sql,不 import 驱动
│   ├── broker.go                # Broker 结构、New(dialect,db,opts)、Init/Close、withTx(重试环)、wake、Rec、ScanRec、helpers
│   ├── dialect.go               # Dialect 接口定义(见 data-model.md)
│   ├── enqueue.go               # Enqueue(撞键→ErrTaskExists 走 dialect.IsDuplicateKey)
│   ├── dequeue.go               # Dequeue + 阻塞轮询(三唤醒源);认领委托 dialect.Claim
│   ├── lifecycle.go             # Ack/Fail/Cancel/FinishCanceled/Requeue/Heartbeat
│   ├── propagate.go             # 单事务 BFS 连锁传播(照抄 sqlite propagateTx)
│   └── query.go                 # Get/List/QueueLen/Counts/ReapExpired(展开 ?N 复用)
├── pgbroker/
│   ├── broker.go                # Open(dsn, opts...) → *sqlbroker.Broker;PG 方言注入
│   ├── dialect.go               # pgDialect 实现 sqlbroker.Dialect;import pgconn 判 23505/40P01/40001
│   ├── driver.go                # blank import _ "github.com/jackc/pgx/v5/stdlib"
│   └── broker_test.go           # brokertest.Run,TASKGATE_PG_DSN 门控 + 随机表前缀隔离
├── mysqlbroker/
│   ├── broker.go                # Open(dsn, opts...);Enqueue 入口长度校验(≤255)
│   ├── dialect.go               # mysqlDialect;import mysql 判 1062/1213/1205;认领两步式;utf8mb4_bin DDL
│   ├── driver.go                # blank import _ "github.com/go-sql-driver/mysql"
│   └── broker_test.go           # TASKGATE_MYSQL_DSN 门控 + 长度校验用例
├── multiproc_test.go            # 改:DSN 存在时追加 PG/MySQL 后端(含并发 propagate 压测)
└── .github/workflows/ci.yml     # 改:加 test-pg(postgres:16)/test-mysql(mysql:8)两 job
```

**Structure Decision**:
- 具体 Dialect(含驱动私有错误类型断言 `*pgconn.PgError`/`*mysql.MySQLError`)放**薄壳包**,不放 sqlbroker——这样 sqlbroker 零驱动依赖,且用户只 import pgbroker 就不会把 mysql 驱动链进二进制(FR-001)。
- `internal/sqlbroker` 是 internal 包,但同模块的 pgbroker/mysqlbroker 可正常 import 它;因此 sqlbroker 导出 `Broker`/`Dialect`/`Rec`/`ScanRec`/`Options` 等给薄壳使用。
- 薄壳 `Open` 构造具体 dialect + `sql.Open` 打开 `*sql.DB` + `sqlbroker.New(dialect, db, opts)`,返回核心 Broker。

## Complexity Tracking

| 超标项 | 原因 | 补救措施 |
|--------|------|----------|
| 相对 sqlite 蓝本新增 withTx 重试环(蓝本零重试) | PG/MySQL 真并发行锁,多行事务必然死锁窗口,不处理就脏 flaky | research 第 1 节定形态;fn 无副作用可重跑 + Notify 禁入环 + 1205/1213 分档;L3 并发压测专项作唯一自动化证明(SC-005) |
| 引入 Dialect 抽象(方案 B 是两包各抄一份) | 两库标准 SQL 逐字相同,复制两份每次契约变更三处同步、漂移风险(违 DRY/宪法 II) | Dialect 只收 8 个"真正不同"的点(枚举可数),标准 SQL 全在核心一份;认领 SQL 因 RETURNING 差异由 dialect.Claim 各写一版 |
| 首个"本地零覆盖"后端(env 门控) | MySQL/PG 无纯 Go 内存替身(go-mysql-server 不支持 SKIP LOCKED,embedded-postgres 要联网) | 已修宪 v1.2.0 第 V.5 明批;CI service container 兜底必跑;代价明写进已知限制 |
