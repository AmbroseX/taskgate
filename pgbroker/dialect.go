package pgbroker

import (
	"context"
	"database/sql"
	"errors"
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/AmbroseX/taskgate/internal/sqlbroker"
	"github.com/jackc/pgx/v5/pgconn"
)

// pgDialect PostgreSQL 方言。实现 sqlbroker.Dialect。
type pgDialect struct{}

var _ sqlbroker.Dialect = (*pgDialect)(nil)

func (pgDialect) Name() string { return "postgres" }

// Rebind 把核心 SQL 里的位置 ? 依次换成 PG 的 $1..$n。核心 SQL 的字符串字面量里没有 ?,
// 但仍逐字符扫描、跳过单引号内区域,稳妥起见不误伤。
func (pgDialect) Rebind(query string) string {
	var b strings.Builder
	b.Grow(len(query) + 8)
	n := 0
	inQuote := false
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case c == '\'':
			inQuote = !inQuote
			b.WriteByte(c)
		case c == '?' && !inQuote:
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// SchemaSQL PG 建表 + 索引:BYTEA 存二进制、BIGINT 存 unix 毫秒、TEXT 存文本;索引名带前缀
// (PG 索引名在 schema 内全局唯一,裸名会撞车)。时间列由 Go 层传入,不用 NOW()。
func (pgDialect) SchemaSQL(prefix string) []string {
	tasks := prefix + "tasks"
	deps := prefix + "task_deps"
	return []string{
		`CREATE TABLE IF NOT EXISTS ` + tasks + ` (
			id               TEXT PRIMARY KEY,
			type             TEXT NOT NULL,
			queue            TEXT NOT NULL,
			payload          BYTEA,
			status           TEXT NOT NULL,
			result           BYTEA,
			last_error       TEXT NOT NULL DEFAULT '',
			attempts         BIGINT NOT NULL DEFAULT 0,
			max_retry        BIGINT NOT NULL DEFAULT 0,
			lease_lost       BIGINT NOT NULL DEFAULT 0,
			throttled        BIGINT NOT NULL DEFAULT 0,
			run_at           BIGINT NOT NULL,
			depends_on       TEXT NOT NULL DEFAULT '[]',
			on_parent_fail   TEXT NOT NULL DEFAULT 'fail_fast',
			pending_parents  BIGINT NOT NULL DEFAULT 0,
			lease_token      TEXT NOT NULL DEFAULT '',
			lease_until      BIGINT NOT NULL DEFAULT 0,
			cancel_requested BIGINT NOT NULL DEFAULT 0,
			created_at       BIGINT NOT NULL,
			started_at       BIGINT NOT NULL DEFAULT 0,
			finished_at      BIGINT NOT NULL DEFAULT 0,
			business_key     TEXT NOT NULL DEFAULT '',
			replay_of        TEXT NOT NULL DEFAULT ''
		)`,
		// 存量表升级(spec 005,幂等):spec 004 建的表没有这两列,补上;新表是空操作。
		`ALTER TABLE ` + tasks + ` ADD COLUMN IF NOT EXISTS business_key TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE ` + tasks + ` ADD COLUMN IF NOT EXISTS replay_of TEXT NOT NULL DEFAULT ''`,
		`CREATE INDEX IF NOT EXISTS ` + prefix + `idx_claim ON ` + tasks + ` (queue, status, run_at)`,
		`CREATE INDEX IF NOT EXISTS ` + prefix + `idx_status ON ` + tasks + ` (status, lease_until)`,
		// 不变式 1(链头唯一):同键下 replay_of 为空的行至多一条 → 并发同键入队兜底(spec 005)。
		`CREATE UNIQUE INDEX IF NOT EXISTS ` + prefix + `uq_chain_head ON ` + tasks +
			` (business_key) WHERE business_key <> '' AND replay_of = ''`,
		// 不变式 2(重放来源唯一):链不分叉 → 并发同目标 Replay 兜底(spec 005)。
		`CREATE UNIQUE INDEX IF NOT EXISTS ` + prefix + `uq_replay_of ON ` + tasks +
			` (replay_of) WHERE replay_of <> ''`,
		`CREATE INDEX IF NOT EXISTS ` + prefix + `idx_business_key ON ` + tasks +
			` (business_key) WHERE business_key <> ''`,
		// 周期配额(spec 006):qkey + 窗口起点 → 已用次数,"检查+扣减"单语句原子完成。
		`CREATE TABLE IF NOT EXISTS ` + prefix + `quota (
			qkey TEXT   NOT NULL,
			win  BIGINT NOT NULL,
			used BIGINT NOT NULL,
			PRIMARY KEY (qkey, win)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + deps + ` (
			child_id  TEXT NOT NULL,
			parent_id TEXT NOT NULL,
			done      BIGINT NOT NULL DEFAULT 0,
			PRIMARY KEY (child_id, parent_id)
		)`,
		`CREATE INDEX IF NOT EXISTS ` + prefix + `idx_deps_parent ON ` + deps + ` (parent_id, done)`,
	}
}

// IsDuplicateKey PG 唯一约束冲突 SQLSTATE 23505。errors.As 到驱动错误类型再看码,禁字符串匹配。
func (pgDialect) IsDuplicateKey(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// DuplicateKeyConstraint 撞的约束/索引名:PG 有结构化字段,直接取。
func (pgDialect) DuplicateKeyConstraint(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return pgErr.ConstraintName
	}
	return ""
}

// IsIdempotentDDLErr PG 的 DDL 全部用 IF NOT EXISTS 表达,不存在良性重复错误。
func (pgDialect) IsIdempotentDDLErr(error) bool { return false }

// QuotaSQL PG 走单语句路径:ON CONFLICT..DO UPDATE..WHERE + RETURNING 一条原子完成
// "取服务端时间 → 算窗口 → 检查余额 → 扣减";时间覆盖参数为 NULL 时用 now()(服务端钟)。
func (pgDialect) QuotaSQL(prefix string) sqlbroker.QuotaSQL {
	q := prefix + "quota"
	return sqlbroker.QuotaSQL{
		Reserve: `INSERT INTO ` + q + ` (qkey, win, used)
			VALUES (?, COALESCE(CAST(? AS BIGINT), CAST(EXTRACT(EPOCH FROM now()) AS BIGINT)) / ? * ?, 1)
			ON CONFLICT (qkey, win) DO UPDATE SET used = ` + q + `.used + 1 WHERE ` + q + `.used < ?
			RETURNING win`,
		Now: `SELECT COALESCE(CAST(? AS BIGINT), CAST(EXTRACT(EPOCH FROM now()) AS BIGINT))`,
	}
}

// Retryable PG 死锁 40P01 / 序列化失败 40001 都是数据库立即返回,可立即重试。
func (pgDialect) Retryable(err error) sqlbroker.RetryClass {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40P01", "40001": // deadlock_detected / serialization_failure
			return sqlbroker.RetryImmediate
		}
	}
	return sqlbroker.NotRetryable
}

// Lock/Unlock 建表期库级互斥:pg_advisory_lock 是会话级,钉在传入的独占连接上;
// key 字符串 hash 成 bigint 传给 advisory 锁。
func (pgDialect) Lock(ctx context.Context, conn *sql.Conn, key string) error {
	_, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, advisoryKey(key))
	return err
}

func (pgDialect) Unlock(ctx context.Context, conn *sql.Conn, key string) error {
	_, err := conn.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey(key))
	return err
}

// advisoryKey 把前缀字符串 hash 成一个稳定的 int64,给 pg_advisory_lock 当锁号。
func advisoryKey(key string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("taskgate:" + key))
	return int64(h.Sum64())
}
