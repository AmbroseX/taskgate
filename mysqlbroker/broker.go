// Package mysqlbroker 是 taskgate 的 MySQL 后端:database/sql + go-sql-driver/mysql(纯 Go 免 cgo)。
// 薄壳——把 MySQL 方言注入共享核心 internal/sqlbroker,认领互斥靠 FOR UPDATE SKIP LOCKED
// (要求 MySQL 8.0+),语义与 sqlite 后端完全对齐,过 brokertest 18 条契约(env 门控)。
//
// MySQL 独有限制(见已知限制):ID/type/queue 最长 255 字符(Enqueue 入口校验,超限清晰报错);
// payload/result 受服务器 max_allowed_packet 限制(默认 64M);表用 utf8mb4_bin 排序规则(DDL 内置)。
// 同 pgbroker:跨进程感知延迟 = 轮询间隔(默认 200ms);不提供分布式限流;无 LISTEN/NOTIFY。
package mysqlbroker

import (
	"context"
	"fmt"
	"time"
	"unicode/utf8"

	"database/sql"

	"github.com/AmbroseX/taskgate"
	"github.com/AmbroseX/taskgate/internal/sqlbroker"
	"github.com/go-sql-driver/mysql"
)

// maxIDLen ID/type/queue 的字符上限,对应 DDL 里的 VARCHAR(255)。
const maxIDLen = 255

// Options mysqlbroker 的可选配置,字段带 tag 供应用自己 unmarshal(库内不读环境变量,宪法 I)。
type Options struct {
	TablePrefix  string        `yaml:"table_prefix" json:"table_prefix"`     // 默认 "taskgate_"
	MaxOpenConns int           `yaml:"max_open_conns" json:"max_open_conns"` // 默认 10
	PollInterval time.Duration `yaml:"poll_interval" json:"poll_interval"`   // 默认 200ms
}

// Option 函数式选项。
type Option func(*Options)

// WithTablePrefix 设置表名/索引名前缀(多应用共享同一 MySQL 库时隔离)。
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

// Broker 包一层共享核心,只为在 Enqueue 入口加 MySQL 独有的长度校验;其余方法全部透传。
type Broker struct {
	*sqlbroker.Broker
}

// 编译期断言:*Broker 必须实现完整的 Broker 接口。
var _ taskgate.Broker = (*Broker)(nil)

// Open 打开一个连到 dsn 的 MySQL 后端(如 "user:pass@tcp(localhost:3306)/db")。
// 会话级 innodb_lock_wait_timeout 调低到 5s(配合核心对 1205 的有限重试),连接排序规则设 utf8mb4_bin。
// 返回的 Broker 用之前必须先 Init(由 taskgate.New(cfg) 统一调用),Init 时自动建表。
func Open(dsn string, opts ...Option) (*Broker, error) {
	var o Options
	for _, fn := range opts {
		fn(&o)
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("mysqlbroker: parse dsn: %w", err)
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	// go-sql-driver 把未知 DSN 参数当作系统变量,在每个新连接上 SET;把锁等待超时调低,
	// 让 1205(lock wait timeout)几秒就返回,配合 Dialect.Retryable 的有限重试,不至于挂死几百秒。
	if _, ok := cfg.Params["innodb_lock_wait_timeout"]; !ok {
		cfg.Params["innodb_lock_wait_timeout"] = "5"
	}
	cfg.Collation = "utf8mb4_bin" // 连接侧也用 bin,自定义 ID 大小写敏感,和表排序规则一致

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("mysqlbroker: open: %w", err)
	}
	core := sqlbroker.New(mysqlDialect{}, db, sqlbroker.Config{
		TablePrefix:  o.TablePrefix,
		MaxOpenConns: o.MaxOpenConns,
		PollInterval: o.PollInterval,
	})
	return &Broker{Broker: core}, nil
}

// Enqueue 在透传给核心前先做 MySQL 独有的长度校验:id/type/queue 超过 255 字符(VARCHAR(255))
// 直接返回清晰错误,不落库——否则驱动会在协议层报脏错(参照 redisbroker validateID 的先例)。
func (b *Broker) Enqueue(ctx context.Context, t *taskgate.Task) error {
	if err := validateLen("id", t.ID); err != nil {
		return err
	}
	if err := validateLen("type", t.Type); err != nil {
		return err
	}
	if err := validateLen("queue", t.Queue); err != nil {
		return err
	}
	if err := validateLen("business_key", t.BusinessKey); err != nil {
		return err
	}
	return b.Broker.Enqueue(ctx, t)
}

// validateLen 校验单个字段的字符数(utf8mb4 VARCHAR(255) 是 255 个字符,不是字节)。
func validateLen(field, val string) error {
	if utf8.RuneCountInString(val) > maxIDLen {
		return fmt.Errorf("mysqlbroker: %s too long: %d characters exceeds max %d",
			field, utf8.RuneCountInString(val), maxIDLen)
	}
	return nil
}
