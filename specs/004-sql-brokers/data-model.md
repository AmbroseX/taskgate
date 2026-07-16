# Data Model: SQL 后端

**Spec**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md)

表结构与 sqlitebroker/schema.sql 一一对应,只做类型映射;逻辑列语义、索引、时间存毫秒(Go 层注入)一概不变。

## 表 `<prefix>tasks`(21 列,与 sqlite 对齐)

`<prefix>` 默认 `taskgate_`(Options.TablePrefix 可配)。

| 列 | 语义 | sqlite | PostgreSQL | MySQL(utf8mb4_bin) |
|---|---|---|---|---|
| id | 主键,ulid 或自定义 | TEXT PK | TEXT PK | VARCHAR(255) PK |
| type | 任务类型 | TEXT NOT NULL | TEXT NOT NULL | VARCHAR(255) NOT NULL |
| queue | 队列名 | TEXT NOT NULL | TEXT NOT NULL | VARCHAR(255) NOT NULL |
| payload | 载荷(json.RawMessage) | BLOB | BYTEA | LONGBLOB |
| status | 状态机 | TEXT NOT NULL | TEXT NOT NULL | VARCHAR(32) NOT NULL |
| result | 结果 | BLOB | BYTEA | LONGBLOB |
| last_error | 最近错误 | TEXT NOT NULL '' | TEXT NOT NULL '' | TEXT NOT NULL |
| attempts | 业务失败计数 | INTEGER 0 | BIGINT 0 | BIGINT 0 |
| max_retry | 最大重试 | INTEGER 0 | BIGINT 0 | BIGINT 0 |
| lease_lost | worker 崩溃计数 | INTEGER 0 | BIGINT 0 | BIGINT 0 |
| throttled | 网关限流计数 | INTEGER 0 | BIGINT 0 | BIGINT 0 |
| run_at | 到点时刻(unix ms) | INTEGER | BIGINT | BIGINT |
| depends_on | 依赖父 ID JSON 数组 | TEXT '[]' | TEXT '[]' | TEXT |
| on_parent_fail | 父失败策略 | TEXT 'fail_fast' | TEXT 'fail_fast' | VARCHAR(32) |
| pending_parents | 未完成父数 | INTEGER 0 | BIGINT 0 | BIGINT 0 |
| lease_token | 租约令牌 | TEXT '' | TEXT '' | VARCHAR(64) |
| lease_until | 租约到期(unix ms) | INTEGER 0 | BIGINT 0 | BIGINT 0 |
| cancel_requested | 取消标记 | INTEGER 0 | BIGINT 0 | BIGINT 0 / TINYINT |
| created_at | 创建(unix ms) | INTEGER | BIGINT | BIGINT |
| started_at | 开始(unix ms) | INTEGER 0 | BIGINT 0 | BIGINT 0 |
| finished_at | 结束(unix ms) | INTEGER 0 | BIGINT 0 | BIGINT 0 |

`taskCols` 21 列固定顺序沿用 sqlitebroker/broker.go:38-41,ScanRec 按此顺序扫;MySQL 的 `on_parent_fail`/`status` 用 VARCHAR 便于建索引,`depends_on`/`last_error` 用 TEXT。

**MySQL 三个硬约束**:
1. 表与所有 VARCHAR 列排序规则 `utf8mb4_bin`(FR-016):默认 `_ci` 会把自定义 ID "abc"/"ABC" 判成重复、排序契约漂移。DDL 显式 `COLLATE utf8mb4_bin`。
2. id/type/queue 是 VARCHAR(255):Enqueue 入口校验长度 ≤255(FR-015),超限清晰报错,不发驱动。
3. payload/result LONGBLOB 受 `max_allowed_packet`(默认 64M)限制:不做入口校验,写进已知限制。

## 表 `<prefix>task_deps`(依赖边)

| 列 | 语义 | sqlite | PG | MySQL |
|---|---|---|---|---|
| child_id | 子任务 | TEXT | TEXT | VARCHAR(255) |
| parent_id | 父任务 | TEXT | TEXT | VARCHAR(255) |
| done | 父是否已完成 | INTEGER 0 | BIGINT 0 | BIGINT 0 / TINYINT |

主键 `(child_id, parent_id)`。

## 索引(名字带前缀,PG 索引名 schema 内全局唯一必须带前缀)

| 索引 | 列 | 用途 |
|---|---|---|
| `<prefix>idx_claim` | tasks(queue, status, run_at) | 认领扫描 |
| `<prefix>idx_status` | tasks(status, lease_until) | ReapExpired 收割 |
| `<prefix>idx_deps_parent` | task_deps(parent_id, done) | 连锁传播 |

## Dialect 接口(internal/sqlbroker/dialect.go)

见 research.md 第 3 节。8 个方法:`Name / Rebind / SchemaSQL / Claim / IsDuplicateKey / Retryable / Lock / Unlock`。`Retryable` 返回三态 `RetryClass`(NotRetryable / RetryImmediate / RetryLimited)。

## 导出给薄壳包的类型(internal/sqlbroker)

- `Broker`:核心实现,`New(dialect Dialect, db *sql.DB, opts Config) *Broker` 构造;实现 taskgate.Broker 15 方法。
- `Rec`:内部记录(task + pendingParents + leaseUntilMS + cancelRequested),对应 sqlitebroker rec 结构。
- `ScanRec(s RowScanner) (*Rec, error)`、`RowScanner` 接口(`Scan(...any) error`)。
- `ClaimParams`:`{Queues []string, NowMS int64, LeaseToken string}`,Claim 用。
- `TaskCols() string`:21 列拼好的 SELECT 列清单(带表前缀)。
- `Config`:核心配置——`TablePrefix`、`MaxOpenConns`(默认 10)、`PollInterval`(默认 200ms)、`MaxTxRetry`(默认 5);从薄壳 Options 映射而来。

## 薄壳包 Options(每后端各一个 struct,字段带 tag)

```go
// pgbroker
type Options struct {
    TablePrefix  string        `yaml:"table_prefix" json:"table_prefix"`   // 默认 "taskgate_"
    MaxOpenConns int           `yaml:"max_open_conns" json:"max_open_conns"` // 默认 10
    PollInterval time.Duration `yaml:"poll_interval" json:"poll_interval"`   // 默认 200ms
}
func Open(dsn string, opts ...Option) (*sqlbroker.Broker, error)   // 函数式 Option 或直接收 Options struct
```

mysqlbroker.Options 同构。构造流程:`sql.Open(driverName, dsn)` → `SetMaxOpenConns` → 建具体 dialect → `sqlbroker.New(dialect, db, cfg)` → `Init` 时建表(第 5 节互斥)。mysqlbroker 额外在连接建立后 `SET SESSION innodb_lock_wait_timeout=5`(通过 DSN 参数或连接钩子)。

## 状态机与计数(不变,照 sqlite/契约)

status:`pending / retrying / running / completed / failed / canceled`。三计数各管一段(Attempts 业务失败、LeaseLost 崩溃默认封顶 3、Throttled 限流封顶 100),Shutdown 的 Requeue 不占任何计数——全部照现有 brokertest 契约,SQL 后端逐字复用 sqlite 的 lifecycle/query 逻辑。
