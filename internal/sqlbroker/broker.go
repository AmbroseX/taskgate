// Package sqlbroker 是 PostgreSQL / MySQL 两个服务器型后端的共享核心:基于标准库
// database/sql,把两库"真正不同"的点收进 Dialect(见 dialect.go),其余标准 SQL 一份。
// 实现蓝本是 sqlitebroker,但有两处 sqlite 没有、SQL 后端必须新写的机制:
//
//  1. withTx 死锁重试环:sqlite 单连接串行天生不死锁,PG/MySQL 是真并发行锁,
//     多行事务(尤其连锁传播按树形状加锁)必然有死锁窗口,靠 Dialect.Retryable 判定后自动重跑。
//  2. 占位符只用不复用:一律位置 ?,同值在 args 里重复传(MySQL 不支持 ?N 复用)。
//
// 认领互斥与 ReapExpired 靠 FOR UPDATE SKIP LOCKED(要求 MySQL 8.0+ / PG 9.5+),
// 终态更新 + 子任务唤醒同事务(宪法 III),语义由 brokertest 18 条契约统一验收(env 门控)。
//
// 本包是 internal:只给同模块的 pgbroker/mysqlbroker 薄壳 import,不对外公开。
package sqlbroker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AmbroseX/taskgate"
)

// totalRetries 统计 withTx 重试环被触发的总次数(死锁/序列化失败被吃掉、事务重跑的次数)。
// 只给测试观测重试环是否真的生效用(SC-005),生产不读。跨 broker 全局累加,够测试用。
var totalRetries atomic.Int64

// TotalRetries 返回 withTx 重试环累计触发次数,供压测断言死锁重试真的发生过。
func TotalRetries() int64 { return totalRetries.Load() }

// taskCols 全列清单,SELECT 与 scanRec 的扫描顺序必须严格一致(与 sqlitebroker 对齐)。
const taskCols = `id, type, queue, payload, status, result, last_error,
	attempts, max_retry, lease_lost, throttled, run_at, depends_on, on_parent_fail,
	pending_parents, lease_token, lease_until, cancel_requested,
	created_at, started_at, finished_at`

// Config 共享核心的运行参数,由薄壳包从各自 Options 映射而来(零值补默认)。
type Config struct {
	TablePrefix  string        // 表名/索引名前缀,默认 "taskgate_";多应用共享服务器库时隔离
	MaxOpenConns int           // 连接池上限,默认 10;防一堆 worker 的阻塞轮询打爆 max_connections
	PollInterval time.Duration // Dequeue 无果时的兜底轮询间隔,默认 200ms;跨进程写入靠它发现
	MaxTxRetry   int           // withTx 死锁重试上限,默认 5
}

func (c *Config) withDefaults() {
	if c.TablePrefix == "" {
		c.TablePrefix = "taskgate_"
	}
	if c.MaxOpenConns <= 0 {
		c.MaxOpenConns = 10
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 200 * time.Millisecond
	}
	if c.MaxTxRetry <= 0 {
		c.MaxTxRetry = 5
	}
}

// Broker PG/MySQL 共享后端。实现 taskgate.Broker。
type Broker struct {
	db      *sql.DB
	dialect Dialect
	cfg     Config

	tasksTable string
	depsTable  string
	tableRepl  *strings.Replacer // {{t}}→tasksTable、{{d}}→depsTable

	mu     sync.Mutex    // 保护 wakeCh 换代与 inited/closed 标记
	wakeCh chan struct{} // 换代式广播:每次唤醒 close 掉再换一个新的
	opts   taskgate.BrokerOptions
	clk    taskgate.Clock
	inited bool
	closed bool
}

// 编译期断言:*Broker 必须实现完整的 Broker 接口。
var _ taskgate.Broker = (*Broker)(nil)

// New 用打开好的 *sql.DB 和一个方言装配核心。db 由薄壳包 sql.Open 得到;
// 返回的 Broker 用之前必须先 Init(建表也在 Init 里,那时才有独占连接跑 DDL)。
func New(dialect Dialect, db *sql.DB, cfg Config) *Broker {
	cfg.withDefaults()
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	tasks := cfg.TablePrefix + "tasks"
	deps := cfg.TablePrefix + "task_deps"
	return &Broker{
		db:         db,
		dialect:    dialect,
		cfg:        cfg,
		tasksTable: tasks,
		depsTable:  deps,
		tableRepl:  strings.NewReplacer("{{t}}", tasks, "{{d}}", deps),
		wakeCh:     make(chan struct{}),
	}
}

// Init 装配运行参数(零值补默认),并建表。首次调用时在独占连接上加库级互斥锁跑 DDL,
// 兼容多进程冷启动并发建表(避免 PG 的 tuple concurrently updated 等脏错)。
func (b *Broker) Init(opts taskgate.BrokerOptions) error {
	b.mu.Lock()
	if opts.DefaultLeaseTTL <= 0 {
		opts.DefaultLeaseTTL = 60 * time.Second
	}
	if opts.LeaseLostMax <= 0 {
		opts.LeaseLostMax = 3
	}
	if opts.ThrottledMax <= 0 {
		opts.ThrottledMax = 100
	}
	if opts.Clock == nil {
		opts.Clock = taskgate.RealClock()
	}
	b.opts = opts
	b.clk = opts.Clock
	alreadyInited := b.inited
	b.inited = true
	b.mu.Unlock()

	if alreadyInited {
		return nil
	}
	if err := b.ensureSchema(context.Background()); err != nil {
		b.mu.Lock()
		b.inited = false
		b.mu.Unlock()
		return err
	}
	return nil
}

// ensureSchema 在独占连接上"加锁 → 跑 DDL → 解锁",全程钉同一连接(见 Dialect.Lock 注释)。
func (b *Broker) ensureSchema(ctx context.Context) error {
	conn, err := b.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("%s: acquire conn for schema: %w", b.dialect.Name(), err)
	}
	defer conn.Close()

	key := b.cfg.TablePrefix + "schema"
	if err := b.dialect.Lock(ctx, conn, key); err != nil {
		return fmt.Errorf("%s: lock for schema: %w", b.dialect.Name(), err)
	}
	defer func() { _ = b.dialect.Unlock(ctx, conn, key) }()

	for _, ddl := range b.dialect.SchemaSQL(b.cfg.TablePrefix) {
		if _, err := conn.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("%s: apply schema: %w", b.dialect.Name(), err)
		}
	}
	return nil
}

// Close 关库:标记 closed 并广播,让阻塞中的 Dequeue 尽快退出,再关连接池。
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.wakeCh)
	b.wakeCh = make(chan struct{})
	b.mu.Unlock()
	return b.db.Close()
}

// requireInit 没 Init 就用是接线错误,直接报错而不是悄悄用坏参数跑。
func (b *Broker) requireInit() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.inited {
		return fmt.Errorf("%s: Init must be called before use", b.dialect.Name())
	}
	if b.closed {
		return fmt.Errorf("%s: broker is closed", b.dialect.Name())
	}
	return nil
}

// ttlFor 队列的租约时长:按队列配置,没配走缺省。
func (b *Broker) ttlFor(queue string) time.Duration {
	if d, ok := b.opts.LeaseTTL[queue]; ok && d > 0 {
		return d
	}
	return b.opts.DefaultLeaseTTL
}

// prep 把核心 SQL 里的表名占位({{t}}/{{d}})替换成带前缀的真名,再过方言的 Rebind。
func (b *Broker) prep(query string) string {
	return b.dialect.Rebind(b.tableRepl.Replace(query))
}

// wakeAll 换代式广播:close 旧 channel 唤醒所有等待者,再换一个新的。
func (b *Broker) wakeAll() {
	b.mu.Lock()
	close(b.wakeCh)
	b.wakeCh = make(chan struct{})
	b.mu.Unlock()
}

// wakeChan 取当前代的唤醒信号。Dequeue 必须先取信号再试认领,防丢信号。
func (b *Broker) wakeChan() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.wakeCh
}

// fireNotify 在事务提交之后异步触发状态流转回调,recover 包住:
// 回调 panic/阻塞不能砸主流程,也不能拖长事务(合同要求)。
// 严禁在 withTx 的重试环内调用——回滚掉的事务已发出的通知就是"幻通知"。
func (b *Broker) fireNotify(snaps []taskgate.Task) {
	fn := b.opts.Notify
	if fn == nil || len(snaps) == 0 {
		return
	}
	go func() {
		for _, s := range snaps {
			func() {
				defer func() { _ = recover() }()
				fn(s)
			}()
		}
	}()
}

// withTx 开事务跑 fn,内置死锁/序列化失败重试环(sqlite 蓝本没有的新机制):
//   - fn 报错 → 回滚,按 Dialect.Retryable 判定;可重试且没到上限就退避后重跑整个 fn,否则原样返回。
//   - fn 成功 → 提交;提交也可能撞死锁,同样判定重试。
//   - 返回 fn 最后一次**成功**收集的通知快照;调用方在 withTx 返回后才 wakeAll/fireNotify。
//
// 硬纪律(防幻通知):fn 必须无副作用可重跑——每轮自建本地 notifs 切片、重新 getRecForUpdate;
// 绝不能在 fn 内 wakeAll/fireNotify,否则前几轮 rollback 掉的事务会广播从未发生的状态流转。
func (b *Broker) withTx(ctx context.Context, fn func(tx *sql.Tx) ([]taskgate.Task, error)) ([]taskgate.Task, error) {
	var lastErr error
	for attempt := 0; attempt < b.cfg.MaxTxRetry; attempt++ {
		tx, err := b.db.BeginTx(ctx, nil) // 隔离级别用两库默认:PG RC / MySQL RR,写路径都先锁行
		if err != nil {
			return nil, err
		}
		notifs, ferr := fn(tx)
		if ferr != nil {
			_ = tx.Rollback()
			if b.retryTx(ctx, ferr, attempt) {
				lastErr = ferr
				continue
			}
			return nil, ferr
		}
		if cerr := tx.Commit(); cerr != nil {
			if b.retryTx(ctx, cerr, attempt) {
				lastErr = cerr
				continue
			}
			return nil, cerr
		}
		return notifs, nil
	}
	return nil, lastErr
}

// retryTx 判定这次事务错误要不要退避后重跑:结合方言的重试档位与剩余次数,
// 退避走注入 clock(测试确定,不真 sleep);ctx 取消则不再重试。
func (b *Broker) retryTx(ctx context.Context, err error, attempt int) bool {
	class := b.dialect.Retryable(err)
	if class == NotRetryable {
		return false
	}
	limit := b.cfg.MaxTxRetry
	if class == RetryLimited { // MySQL 1205:只重试一次(共两次尝试)
		limit = 2
	}
	if attempt+1 >= limit {
		return false
	}
	// 指数退避:5ms、10ms、20ms…(RetryLimited 只退一次,短)。
	backoff := (5 * time.Millisecond) << attempt
	select {
	case <-ctx.Done():
		return false
	case <-b.clk.After(backoff):
		totalRetries.Add(1)
		return true
	}
}

// ---- 时间与行扫描(与 sqlitebroker 对齐)----

// ms 时间转 unix 毫秒落库;零值时间存 0。
func ms(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// fromMS 毫秒转回 time.Time;0 还原成零值时间。
func fromMS(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.UnixMilli(v).UTC()
}

// rec 一行任务:公开的 Task 快照 + 只在后端内部用的三个列。
type rec struct {
	task            taskgate.Task
	pendingParents  int
	leaseUntilMS    int64
	cancelRequested bool
}

// rowScanner *sql.Row 和 *sql.Rows 的公共扫描面。
type rowScanner interface {
	Scan(dest ...any) error
}

// scanRec 按 taskCols 的顺序扫一行。sql.ErrNoRows 原样透传,调用方自行翻译。
func scanRec(s rowScanner) (*rec, error) {
	var (
		r               rec
		payload, result []byte
		status, deps    string
		policy          string
		runAt, until    int64
		created, start  int64
		finished        int64
		cancelReq       int64
	)
	err := s.Scan(&r.task.ID, &r.task.Type, &r.task.Queue, &payload, &status, &result,
		&r.task.LastError, &r.task.Attempts, &r.task.MaxRetry, &r.task.LeaseLost, &r.task.Throttled,
		&runAt, &deps, &policy, &r.pendingParents, &r.task.LeaseToken, &until, &cancelReq,
		&created, &start, &finished)
	if err != nil {
		return nil, err
	}
	r.task.Payload = payload
	r.task.Result = result
	r.task.Status = taskgate.Status(status)
	r.task.OnParentFailure = taskgate.ParentFailurePolicy(policy)
	if deps != "" && deps != "[]" && deps != "null" {
		if err := json.Unmarshal([]byte(deps), &r.task.DependsOn); err != nil {
			return nil, fmt.Errorf("sqlbroker: bad depends_on of task %s: %w", r.task.ID, err)
		}
	}
	r.task.RunAt = fromMS(runAt)
	r.task.CreatedAt = fromMS(created)
	r.task.StartedAt = fromMS(start)
	r.task.FinishedAt = fromMS(finished)
	r.leaseUntilMS = until
	r.cancelRequested = cancelReq != 0
	return &r, nil
}

// querier *sql.DB / *sql.Tx / *sql.Conn 的公共查询面,让取行在事务内外都能用。
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// getRec 按 ID 取一行(不加锁,只读路径用);不存在 → ErrTaskNotFound。
func (b *Broker) getRec(ctx context.Context, q querier, id string) (*rec, error) {
	r, err := scanRec(q.QueryRowContext(ctx, b.prep(`SELECT `+taskCols+` FROM {{t}} WHERE id = ?`), id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", taskgate.ErrTaskNotFound, id)
	}
	return r, err
}

// getRecForUpdate 事务内取行并加行锁(SELECT ... FOR UPDATE):写路径先锁行再判状态改状态。
// 只读路径(Get/List)绝不走这里,免得在只读查询上白加行锁(FR-012)。
func (b *Broker) getRecForUpdate(ctx context.Context, tx *sql.Tx, id string) (*rec, error) {
	r, err := scanRec(tx.QueryRowContext(ctx, b.prep(`SELECT `+taskCols+` FROM {{t}} WHERE id = ? FOR UPDATE`), id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", taskgate.ErrTaskNotFound, id)
	}
	return r, err
}

// checkLeaseRec 令牌校验三连:在 running?令牌非空?令牌一致?不满足 → ErrLeaseLost。
func checkLeaseRec(r *rec, token string) error {
	if r.task.Status != taskgate.StatusRunning || token == "" || r.task.LeaseToken != token {
		return fmt.Errorf("%w: task %s (status=%s)", taskgate.ErrLeaseLost, r.task.ID, r.task.Status)
	}
	return nil
}

// placeholders n 个 "?" 逗号相连,拼 IN (...) 用。
func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// illegalTransition 非法流转的统一报错文案(带 from→to,合同要求)。
func (b *Broker) illegalTransition(from, to taskgate.Status, id string) error {
	return fmt.Errorf("%s: illegal transition %s -> %s (task %s)", b.dialect.Name(), from, to, id)
}
