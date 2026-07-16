// Package pgbroker 是 taskgate 的 PostgreSQL 后端:database/sql + pgx(stdlib 模式,纯 Go 免 cgo)。
// 只是薄壳——把 PG 方言注入共享核心 internal/sqlbroker,认领互斥靠 FOR UPDATE SKIP LOCKED
// (要求 PostgreSQL 9.5+),语义与 sqlite 后端完全对齐,过 brokertest 18 条契约(env 门控)。
//
// 已知限制见 docs:跨进程新任务感知延迟 = 轮询间隔(默认 200ms,可调);不提供分布式限流
// (LimiterProvider 不实现,scheduler 自动退回进程内限流);未实现 LISTEN/NOTIFY 即时唤醒。
package pgbroker

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/AmbroseX/taskgate/internal/sqlbroker"
)

// Options pgbroker 的可选配置,字段带 tag 供应用自己 unmarshal(库内不读环境变量,宪法 I)。
type Options struct {
	TablePrefix  string        `yaml:"table_prefix" json:"table_prefix"`     // 默认 "taskgate_"
	MaxOpenConns int           `yaml:"max_open_conns" json:"max_open_conns"` // 默认 10
	PollInterval time.Duration `yaml:"poll_interval" json:"poll_interval"`   // 默认 200ms
}

// Option 函数式选项。
type Option func(*Options)

// WithTablePrefix 设置表名/索引名前缀(多应用共享同一 PG 库时隔离)。
func WithTablePrefix(prefix string) Option {
	return func(o *Options) { o.TablePrefix = prefix }
}

// WithMaxOpenConns 设置连接池上限(防阻塞轮询打爆 max_connections)。
func WithMaxOpenConns(n int) Option {
	return func(o *Options) { o.MaxOpenConns = n }
}

// WithPollInterval 设置 Dequeue 兜底轮询间隔(跨进程新任务感知延迟)。
func WithPollInterval(d time.Duration) Option {
	return func(o *Options) { o.PollInterval = d }
}

// Open 打开一个连到 dsn 的 PG 后端(如 "postgres://user:pass@host:5432/db?sslmode=disable")。
// 返回的 Broker 用之前必须先 Init(由 taskgate.New(cfg) 统一调用),Init 时自动建表。
func Open(dsn string, opts ...Option) (*sqlbroker.Broker, error) {
	var o Options
	for _, fn := range opts {
		fn(&o)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("pgbroker: open: %w", err)
	}
	cfg := sqlbroker.Config{
		TablePrefix:  o.TablePrefix,
		MaxOpenConns: o.MaxOpenConns,
		PollInterval: o.PollInterval,
	}
	return sqlbroker.New(pgDialect{}, db, cfg), nil
}
