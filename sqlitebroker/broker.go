// Package sqlitebroker 是 Broker 的 sqlite 文件后端:纯 Go 驱动(modernc.org/sqlite,免 cgo),
// WAL 模式单文件落盘。所有"终态更新 + 子任务唤醒/连锁取消"都在同一个事务里完成(宪法 III),
// 语义以 memorybroker 为基准,由 brokertest 的 16 条契约统一验收。
//
// 并发模型:连接池收紧到 1 个连接(单进程内所有读写串行),配合 WAL + busy_timeout,
// 避免 SQLITE_BUSY;跨进程的写入靠 Dequeue 的 100ms 兜底轮询发现。
package sqlitebroker

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ambrose/taskgate"
	_ "modernc.org/sqlite" // database/sql 驱动,驱动名 "sqlite"
)

//go:embed schema.sql
var schemaSQL string

// pollInterval Dequeue 无果时的兜底轮询间隔:同进程写入靠内部唤醒信号即时响应,
// 别的进程写同一个库文件时靠这个间隔兜底发现(等待走注入 clock,测试不真 sleep)。
const pollInterval = 100 * time.Millisecond

// testHookBeforeAckCommit 崩溃专项测试的注入点:Ack 事务里所有写入(终态+子唤醒)
// 完成之后、提交之前被调用。用它模拟"唤醒中途进程崩了"——因为同一个事务提交前崩掉
// 等于什么都没写,重启后由 ReapExpired 的租约回收 + 防御修复兜底,不丢唤醒。
// 默认 nil 零侵入;测试通过 export_test.go 的 SetTestHookBeforeAckCommit 设置。
var testHookBeforeAckCommit func()

// taskCols 全列清单,SELECT/RETURNING 与 scanRec 的扫描顺序必须严格一致。
const taskCols = `id, type, queue, payload, status, result, last_error,
	attempts, max_retry, lease_lost, throttled, run_at, depends_on, on_parent_fail,
	pending_parents, lease_token, lease_until, cancel_requested,
	created_at, started_at, finished_at`

// Broker sqlite 后端。实现 taskgate.Broker。
type Broker struct {
	db     *sql.DB
	mu     sync.Mutex    // 保护 wakeCh 换代与 inited/closed 标记
	wakeCh chan struct{} // 换代式广播:每次唤醒 close 掉再换一个新的
	opts   taskgate.BrokerOptions
	clk    taskgate.Clock
	inited bool
	closed bool
}

// 编译期断言:*Broker 必须实现完整的 Broker 接口。
var _ taskgate.Broker = (*Broker)(nil)

// Open 打开(不存在则创建)path 指向的 sqlite 库文件并建表。
// 连接参数:WAL 日志、busy_timeout 5s、synchronous NORMAL、事务一律 BEGIN IMMEDIATE。
// 返回的 Broker 用之前必须先 Init(由 taskgate.New(cfg) 统一调用)。
func Open(path string) (*Broker, error) {
	dsn := "file:" + path +
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlitebroker: open %s: %w", path, err)
	}
	// 单文件库收紧到 1 个连接:单进程内读写全串行,从根上避开 SQLITE_BUSY。
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlitebroker: apply schema: %w", err)
	}
	return &Broker{db: db, wakeCh: make(chan struct{})}, nil
}

// Init 装配运行参数,零值补默认(TTL 60s / LeaseLostMax 3 / ThrottledMax 100 / 真时钟)。
func (b *Broker) Init(opts taskgate.BrokerOptions) error {
	b.mu.Lock()
	defer b.mu.Unlock()
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
	b.inited = true
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
		return errors.New("sqlitebroker: Init must be called before use")
	}
	if b.closed {
		return errors.New("sqlitebroker: broker is closed")
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

// wakeAll 换代式广播:close 旧 channel 唤醒所有等待者,再换一个新的。
// 等价于 memorybroker 的 cond.Broadcast(),同进程的 Dequeue 靠它即时醒来。
func (b *Broker) wakeAll() {
	b.mu.Lock()
	close(b.wakeCh)
	b.wakeCh = make(chan struct{})
	b.mu.Unlock()
}

// wakeChan 取当前代的唤醒信号。Dequeue 必须先取信号再试认领,
// 这样"试完没抢到 → 挂起等待"之间发生的写入不会丢信号。
func (b *Broker) wakeChan() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.wakeCh
}

// fireNotify 在事务提交之后异步触发状态流转回调,recover 包住:
// 回调 panic/阻塞不能砸主流程,也不能拖长事务(合同要求)。
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

// withTx 开一个事务跑 fn,fn 报错就回滚,否则提交(连接参数已定死 BEGIN IMMEDIATE)。
func (b *Broker) withTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() // 提交成功后 Rollback 是无害的空操作
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// ---- 时间与行扫描 ----

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
		cancelReq       int
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
			return nil, fmt.Errorf("sqlitebroker: bad depends_on of task %s: %w", r.task.ID, err)
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

// querier *sql.DB 和 *sql.Tx 的公共查询面,让 getRec 在事务内外都能用。
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// getRec 按 ID 取一行;不存在 → ErrTaskNotFound。
func getRec(ctx context.Context, q querier, id string) (*rec, error) {
	r, err := scanRec(q.QueryRowContext(ctx, `SELECT `+taskCols+` FROM tasks WHERE id = ?`, id))
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
func illegalTransition(from, to taskgate.Status, id string) error {
	return fmt.Errorf("sqlitebroker: illegal transition %s -> %s (task %s)", from, to, id)
}
