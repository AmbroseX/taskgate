package mysqlbroker

import (
	"context"
	"database/sql"
	"errors"

	"github.com/AmbroseX/taskgate/internal/sqlbroker"
	"github.com/go-sql-driver/mysql"
)

// mysqlDialect MySQL 方言。实现 sqlbroker.Dialect。要求 MySQL 8.0+(FOR UPDATE SKIP LOCKED)。
type mysqlDialect struct{}

var _ sqlbroker.Dialect = (*mysqlDialect)(nil)

func (mysqlDialect) Name() string { return "mysql" }

// Rebind MySQL 原生就用位置 ?,原样返回(核心里本来就只用不复用的 ?)。
func (mysqlDialect) Rebind(query string) string { return query }

// SchemaSQL MySQL 建表 + 索引。三个 MySQL 独有约束:
//   - id/type/queue 用 VARCHAR(255)(TEXT 不能直接做主键/索引),超长由 Enqueue 入口挡(见 broker.go);
//   - payload/result 用 LONGBLOB(受服务器 max_allowed_packet 限制,写进已知限制);
//   - 表与字符列排序规则必须 utf8mb4_bin:默认 _ci 会把自定义 ID "abc"/"ABC" 判成重复、排序契约漂移。
//
// 时间列 BIGINT 存 unix 毫秒,由 Go 层传入,不用 NOW()。索引名带前缀。
func (mysqlDialect) SchemaSQL(prefix string) []string {
	tasks := prefix + "tasks"
	deps := prefix + "task_deps"
	return []string{
		"CREATE TABLE IF NOT EXISTS " + tasks + " (" +
			"id               VARCHAR(255) NOT NULL," +
			"type             VARCHAR(255) NOT NULL," +
			"queue            VARCHAR(255) NOT NULL," +
			"payload          LONGBLOB," +
			"status           VARCHAR(32) NOT NULL," +
			"result           LONGBLOB," +
			"last_error       TEXT," +
			"attempts         BIGINT NOT NULL DEFAULT 0," +
			"max_retry        BIGINT NOT NULL DEFAULT 0," +
			"lease_lost       BIGINT NOT NULL DEFAULT 0," +
			"throttled        BIGINT NOT NULL DEFAULT 0," +
			"run_at           BIGINT NOT NULL," +
			"depends_on       TEXT," +
			"on_parent_fail   VARCHAR(32) NOT NULL DEFAULT 'fail_fast'," +
			"pending_parents  BIGINT NOT NULL DEFAULT 0," +
			"lease_token      VARCHAR(64) NOT NULL DEFAULT ''," +
			"lease_until      BIGINT NOT NULL DEFAULT 0," +
			"cancel_requested BIGINT NOT NULL DEFAULT 0," +
			"created_at       BIGINT NOT NULL," +
			"started_at       BIGINT NOT NULL DEFAULT 0," +
			"finished_at      BIGINT NOT NULL DEFAULT 0," +
			// spec 005:业务幂等键 + 重放来源。MySQL 没有部分唯一索引,
			// 用生成列(非链头/空值时为 NULL,唯一索引对 NULL 不去重)表达两条不变式。
			"business_key     VARCHAR(255) NOT NULL DEFAULT ''," +
			"replay_of        VARCHAR(255) NOT NULL DEFAULT ''," +
			"chain_head_key   VARCHAR(255) GENERATED ALWAYS AS " +
			"(IF(business_key <> '' AND replay_of = '', business_key, NULL)) STORED," +
			"replay_of_uq     VARCHAR(255) GENERATED ALWAYS AS " +
			"(IF(replay_of <> '', replay_of, NULL)) STORED," +
			"PRIMARY KEY (id)," +
			"KEY " + prefix + "idx_claim (queue, status, run_at)," +
			"KEY " + prefix + "idx_status (status, lease_until)," +
			// 不变式 1(链头唯一)与不变式 2(链不分叉)的并发兜底(spec 005)。
			"UNIQUE KEY " + prefix + "uq_chain_head (chain_head_key)," +
			"UNIQUE KEY " + prefix + "uq_replay_of (replay_of_uq)," +
			"KEY " + prefix + "idx_business_key (business_key)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		// 存量表升级(spec 005):spec 004 建的表没有下列列/索引。MySQL 的 ALTER 没有
		// IF NOT EXISTS,重复执行报 1060(列已存在)/1061(索引名已存在),
		// 由 IsIdempotentDDLErr 判定后幂等跳过;新表上这些语句全部报 1060/1061,同样跳过。
		"ALTER TABLE " + tasks + " ADD COLUMN business_key VARCHAR(255) NOT NULL DEFAULT ''",
		"ALTER TABLE " + tasks + " ADD COLUMN replay_of VARCHAR(255) NOT NULL DEFAULT ''",
		"ALTER TABLE " + tasks + " ADD COLUMN chain_head_key VARCHAR(255) GENERATED ALWAYS AS " +
			"(IF(business_key <> '' AND replay_of = '', business_key, NULL)) STORED",
		"ALTER TABLE " + tasks + " ADD COLUMN replay_of_uq VARCHAR(255) GENERATED ALWAYS AS " +
			"(IF(replay_of <> '', replay_of, NULL)) STORED",
		"ALTER TABLE " + tasks + " ADD UNIQUE KEY " + prefix + "uq_chain_head (chain_head_key)",
		"ALTER TABLE " + tasks + " ADD UNIQUE KEY " + prefix + "uq_replay_of (replay_of_uq)",
		"ALTER TABLE " + tasks + " ADD KEY " + prefix + "idx_business_key (business_key)",
		"CREATE TABLE IF NOT EXISTS " + deps + " (" +
			"child_id  VARCHAR(255) NOT NULL," +
			"parent_id VARCHAR(255) NOT NULL," +
			"done      BIGINT NOT NULL DEFAULT 0," +
			"PRIMARY KEY (child_id, parent_id)," +
			"KEY " + prefix + "idx_deps_parent (parent_id, done)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	}
}

// IsDuplicateKey MySQL 唯一约束冲突 errno 1062。errors.As 到驱动错误类型再看码,禁字符串匹配。
func (mysqlDialect) IsDuplicateKey(err error) bool {
	var myErr *mysql.MySQLError
	return errors.As(err, &myErr) && myErr.Number == 1062
}

// DuplicateKeyConstraint 撞的约束名。go-sql-driver 没有结构化的约束名字段,
// 只能返回 1062 的原始 message(形如 Duplicate entry 'x' for key 't.taskgate_uq_replay_of'),
// 调用方按子串匹配——这是 Dialect 合同里明写的唯一字符串匹配豁免。
func (mysqlDialect) DuplicateKeyConstraint(err error) string {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) && myErr.Number == 1062 {
		return myErr.Message
	}
	return ""
}

// IsIdempotentDDLErr 存量表升级的 ALTER 撞"已存在":1060 列已存在 / 1061 索引名已存在。
func (mysqlDialect) IsIdempotentDDLErr(err error) bool {
	var myErr *mysql.MySQLError
	return errors.As(err, &myErr) && (myErr.Number == 1060 || myErr.Number == 1061)
}

// Retryable MySQL 死锁 1213 立即返回、可立即重试;锁等待超时 1205 默认要等
// innodb_lock_wait_timeout 才报(broker.go 已把会话级超时调低),只有限重试一次、不长退避。
func (mysqlDialect) Retryable(err error) sqlbroker.RetryClass {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		switch myErr.Number {
		case 1213: // ER_LOCK_DEADLOCK
			return sqlbroker.RetryImmediate
		case 1205: // ER_LOCK_WAIT_TIMEOUT
			return sqlbroker.RetryLimited
		}
	}
	return sqlbroker.NotRetryable
}

// Lock/Unlock 建表期库级互斥:GET_LOCK 是会话(连接)级,必须钉在传入的独占连接上,
// 否则会出现"A 连接拿锁、B 连接放锁"的错位。等锁上限 10s。
func (mysqlDialect) Lock(ctx context.Context, conn *sql.Conn, key string) error {
	var ok sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, 10)`, lockName(key)).Scan(&ok); err != nil {
		return err
	}
	if !ok.Valid || ok.Int64 != 1 {
		return errors.New("mysql: GET_LOCK for schema did not return 1")
	}
	return nil
}

func (mysqlDialect) Unlock(ctx context.Context, conn *sql.Conn, key string) error {
	_, err := conn.ExecContext(ctx, `SELECT RELEASE_LOCK(?)`, lockName(key))
	return err
}

// lockName GET_LOCK 名字上限 64 字符,前缀可能较长,统一截断到安全长度。
func lockName(key string) string {
	name := "taskgate:" + key
	if len(name) > 64 {
		name = name[:64]
	}
	return name
}
