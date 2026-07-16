package sqlbroker

import (
	"context"
	"database/sql"
)

// RetryClass 事务错误的重试判定,三态。sqlite 单连接串行不会死锁,所以蓝本里没有重试;
// PG/MySQL 是真并发行锁,多行事务(尤其连锁传播按树形状加锁)必然有死锁窗口,
// withTx 靠 Dialect.Retryable 判定后自动重跑整个事务(见 broker.go 的重试环)。
type RetryClass int

const (
	// NotRetryable 不可重试:业务错误、约束冲突等,原样返回给调用方。
	NotRetryable RetryClass = iota
	// RetryImmediate 立即可重试:数据库单方面 kill 的死锁/序列化失败(PG 40P01/40001、MySQL 1213),
	// 事务几乎瞬间返回,重试便宜,按指数退避重试到上限。
	RetryImmediate
	// RetryLimited 有限重试:MySQL 1205 锁等待超时——默认要等 innodb_lock_wait_timeout 才报,
	// 不能和死锁同等指数重试(单次调用最坏挂死几百秒),只重试一次、短退避。
	// mysqlbroker 同时会把会话级 innodb_lock_wait_timeout 调低配合。
	RetryLimited
)

// QuotaSQL 周期配额的方言语句包(spec 006):
//   - Reserve 非空 = 单语句路径(PG):一条原子语句完成"取服务端时间 → 算窗口 →
//     检查余额 → 扣减 → RETURNING win";参数 (qkey, 时间覆盖, period, period, limit),
//     零行 = 耗尽;
//   - Reserve 为空 = 两步路径(MySQL 无 RETURNING):先 Now 取服务端时间(参数:时间覆盖,
//     NULL = 服务端钟),Go 侧算窗口,再 Upsert 原子扣减(参数 qkey, win, limit),
//     按 affected rows 判定(1=插入 2=真更新 → 成功;0=没变 → 耗尽)。
//     取时与扣减分两步不破坏硬配额:原子性在 Upsert 上,时间只决定窗口归属,
//     边界竞态最多把预留记进上一窗(仍是合法窗口,不会多放)。
type QuotaSQL struct {
	Reserve string
	Now     string
	Upsert  string
}

// Dialect 收口 PostgreSQL 与 MySQL 两库"真正不同"的点,其余标准 SQL 全在共享核心一份。
// 实现放在各自薄壳包(pgbroker/mysqlbroker),因为要断言驱动私有错误类型
// (*pgconn.PgError / *mysql.MySQLError),核心包 internal/sqlbroker 因此零驱动依赖。
//
// 认领(claim)与收割(reap)不在本接口:它们统一用
// "SELECT ... FOR UPDATE SKIP LOCKED 拿行 → 逐行 UPDATE" 完成,这个语法两库完全一致,
// 不需要 UPDATE...RETURNING(PG 有 MySQL 无),核心一份代码即可。
type Dialect interface {
	// Name 方言名 "postgres" / "mysql",日志与错误前缀用。
	Name() string

	// Rebind 只改 SQL 文本、不碰 args 切片:PG 把第 n 个 ? 换成 $n;MySQL 原样返回。
	// 核心里所有 SQL 一律只用不复用的位置 ?(同值在 args 里重复传),Rebind 才能保持最简形态。
	Rebind(query string) string

	// SchemaSQL 返回建表 + 建索引的 DDL(按 prefix 拼表名/索引名),按顺序执行。
	// 各库类型映射不同(TEXT/VARCHAR、BLOB/BYTEA/LONGBLOB、INTEGER/BIGINT);
	// MySQL 必须内置 utf8mb4_bin 排序规则(否则自定义 ID "abc"/"ABC" 被判重复、排序契约漂移)。
	SchemaSQL(prefix string) []string

	// IsDuplicateKey 判定错误是否为唯一约束冲突(主键或唯一索引)。
	// PG SQLSTATE 23505 / MySQL errno 1062。实现必须 errors.As 到驱动错误类型再看码,禁止字符串匹配。
	IsDuplicateKey(err error) bool

	// DuplicateKeyConstraint 唯一冲突撞的是哪个约束/索引(spec 005:据此区分主键、
	// uq_chain_head、uq_replay_of 三种冲突并翻译成不同的合同错误);不是唯一冲突或拿不到名字
	// 返回 ""。PG 从 pgconn.PgError.ConstraintName 取(结构化字段);MySQL 驱动没有结构化字段,
	// 只能从 1062 的 message 提取索引名——这是本项目唯一允许的错误字符串匹配,原因如上。
	DuplicateKeyConstraint(err error) string

	// IsIdempotentDDLErr 判定建表期 DDL 错误是否为"重复执行导致的良性错误"(列已存在/索引已存在)。
	// PG 的 DDL 全部用 IF NOT EXISTS 表达,恒返回 false;MySQL 的 ALTER 没有 IF NOT EXISTS,
	// 存量表升级(spec 005)靠 errno 1060(列已存在)/1061(索引名已存在)幂等跳过。
	IsIdempotentDDLErr(err error) bool

	// Retryable 判定事务错误的重试档位(见 RetryClass)。同样 errors.As 禁字符串匹配。
	Retryable(err error) RetryClass

	// QuotaSQL 周期配额(spec 006)两库"真正不同"的三条语句;表名按 prefix 拼好、
	// 占位符用 ?(核心过 Rebind)。Release/清理语句两库同形,不在本方法(见 quota.go)。
	QuotaSQL(prefix string) QuotaSQL

	// Lock 建表期库级互斥:多进程首启并发跑 DDL,PG 会报 tuple concurrently updated 等脏错。
	// 锁必须钉在传入的独占连接 conn 上(MySQL GET_LOCK 是会话级,连接池会错位)。
	Lock(ctx context.Context, conn *sql.Conn, key string) error
	// Unlock 释放建表锁(同一 conn)。PG advisory 会话结束也会自动释放,MySQL 必须显式 RELEASE_LOCK。
	Unlock(ctx context.Context, conn *sql.Conn, key string) error
}
