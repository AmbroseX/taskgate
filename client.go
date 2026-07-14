package taskgate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// waitPollInterval Wait 的轮询间隔,走注入的 clock(research.md 第 7 节:M1 用轮询,不做订阅)。
const waitPollInterval = 50 * time.Millisecond

// Gate 是 taskgate 的统一门面:提交、查询、等待、消费全从这里走。
// 一个 Gate 既可以只当生产者(New 后直接 Submit,不 Handle 不 Run),
// 也可以注册 handler 后 Run 起来当消费者,两者共用同一个 Broker。
type Gate struct {
	cfg    Config
	broker Broker
	clock  Clock

	mu       sync.RWMutex
	handlers map[string]Handler

	sched *scheduler

	// backoff 业务失败的退避函数(入参是当前 Attempts)。
	// 默认是指数退避,留成字段是为了测试能换成毫秒级的短退避。
	backoff func(n int) time.Duration
}

// New 校验配置、补默认值、装配 BrokerOptions 并 Init 后端,返回可用的 Gate。
// 配置有问题直接返回 error,fail fast,绝不 panic。
func New(cfg Config) (*Gate, error) {
	return newGate(cfg, nil)
}

// newGate New 的实现本体;clk 传 nil 用真时钟,测试通过内部入口注入 fakeclock。
func newGate(cfg Config, clk Clock) (*Gate, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if clk == nil {
		clk = RealClock()
	}

	// 组装 BrokerOptions:每个队列的租约 TTL + 全局缺省 + 两个封顶计数 + 回调 + 时钟。
	ttls := make(map[string]time.Duration, len(cfg.Queues))
	for name, q := range cfg.Queues {
		ttls[name] = time.Duration(q.LeaseTTL)
	}
	defTTL := time.Duration(defaultLeaseTTL)
	if cfg.DefaultQueue.LeaseTTL > 0 {
		defTTL = time.Duration(cfg.DefaultQueue.LeaseTTL)
	}
	opts := BrokerOptions{
		LeaseTTL:        ttls,
		DefaultLeaseTTL: defTTL,
		LeaseLostMax:    cfg.LeaseLostMax,
		ThrottledMax:    cfg.ThrottledMax,
		Notify:          cfg.OnStateChange,
		Clock:           clk,
	}
	if err := cfg.Broker.Init(opts); err != nil {
		return nil, fmt.Errorf("taskgate: broker init: %w", err)
	}

	g := &Gate{
		cfg:      cfg,
		broker:   cfg.Broker,
		clock:    clk,
		handlers: make(map[string]Handler),
	}
	g.backoff = newBackoffFunc(nil) // 指数退避 min(2^n×1s,10min)±20%,生产用真随机种子
	g.sched = newScheduler(g)
	return g, nil
}

// Handle 注册任务类型对应的处理函数。必须在 Run 之前注册完。
func (g *Gate) Handle(taskType string, h Handler) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.handlers[taskType] = h
}

// handlerFor 查 handler,没注册返回 nil。
func (g *Gate) handlerFor(taskType string) Handler {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.handlers[taskType]
}

// queueFor 按 Routes 定队列:Routes 里有映射用映射,没有则队列名 = Type;
// 目标队列必须在 Queues 里配过,或者有可用的 DefaultQueue 兜底,否则报错。
// 返回队列名和它生效的限流配置。
func (g *Gate) queueFor(taskType string) (string, QueueConfig, error) {
	name := taskType
	if target, ok := g.cfg.Routes[taskType]; ok {
		name = target
	}
	if qc, ok := g.cfg.Queues[name]; ok {
		return name, qc, nil
	}
	if g.cfg.DefaultQueue.Workers >= 1 {
		return name, g.cfg.DefaultQueue, nil
	}
	return "", QueueConfig{}, fmt.Errorf(
		"taskgate: type %q: queue %q not configured and no usable default_queue", taskType, name)
}

// Submit 提交任务:按 Routes 定队列、应用提交选项、入队,返回任务 ID。
// Delay 和 RunAt 都设置时 RunAt 生效(RunAt 是绝对时刻,语义更强)。
func (g *Gate) Submit(ctx context.Context, taskType string, payload json.RawMessage, opts ...SubmitOption) (string, error) {
	if taskType == "" {
		return "", errors.New("taskgate: submit: task type is required")
	}
	queue, _, err := g.queueFor(taskType)
	if err != nil {
		return "", err
	}
	o := applySubmitOptions(opts...)

	t := &Task{
		ID:        o.id,
		Type:      taskType,
		Queue:     queue,
		Payload:   payload,
		MaxRetry:  o.maxRetry,
		DependsOn: o.dependsOn,
	}
	if o.ignoreParentFailure {
		t.OnParentFailure = IgnoreParentFail
	} else {
		t.OnParentFailure = FailFast
	}
	switch {
	case !o.runAt.IsZero():
		t.RunAt = o.runAt
	case o.delay > 0:
		t.RunAt = g.clock.Now().Add(o.delay)
	}
	if err := g.broker.Enqueue(ctx, t); err != nil {
		return "", err
	}
	return t.ID, nil
}

// Get 查单个任务的当前快照。
func (g *Gate) Get(ctx context.Context, id string) (*Task, error) {
	return g.broker.Get(ctx, id)
}

// List 按过滤条件列任务。
func (g *Gate) List(ctx context.Context, f Filter) ([]*Task, error) {
	return g.broker.List(ctx, f)
}

// QueueStats 单个队列的水位:配置的并发/限速 + 当前在跑数 + 积压长度。
type QueueStats struct {
	Workers  int     `json:"workers"`   // 配置的并发上限
	Running  int     `json:"running"`   // 本进程正在执行的任务数(纯生产者恒为 0)
	QueueLen int     `json:"queue_len"` // 积压:pending + retrying
	RPS      float64 `json:"rps"`       // 配置的限速,0 = 不限
}

// Stats 查单个队列的水位。队列没配置且没有 DefaultQueue 兜底时报错。
func (g *Gate) Stats(ctx context.Context, queue string) (QueueStats, error) {
	qc, ok := g.cfg.Queues[queue]
	if !ok {
		if g.cfg.DefaultQueue.Workers < 1 {
			return QueueStats{}, fmt.Errorf("taskgate: stats: queue %q not configured", queue)
		}
		qc = g.cfg.DefaultQueue
	}
	n, err := g.broker.QueueLen(ctx, queue)
	if err != nil {
		return QueueStats{}, err
	}
	return QueueStats{
		Workers:  qc.Workers,
		Running:  g.sched.runningCount(queue),
		QueueLen: n,
		RPS:      qc.RPS,
	}, nil
}

// Overview 全局概览:Type × Status 的数量矩阵(就是 Broker.Counts)。
func (g *Gate) Overview(ctx context.Context) (map[string]map[Status]int64, error) {
	return g.broker.Counts(ctx)
}

// TaskFailedError Wait 等到 failed/canceled 终态时返回的错误,带上任务现场方便定位。
type TaskFailedError struct {
	ID        string // 任务 ID
	Status    Status // failed 或 canceled
	LastError string // 最后一次失败/取消原因
}

func (e *TaskFailedError) Error() string {
	return fmt.Sprintf("taskgate: task %s %s: %s", e.ID, e.Status, e.LastError)
}

// Wait 阻塞等任务到终态:completed 返回 Result;failed/canceled 返回 *TaskFailedError;
// ctx 先取消返回 ctx.Err()(任务本身照常跑,Wait 只是不等了)。
// 实现是 50ms 轮询 Get,间隔走注入的 clock。
func (g *Gate) Wait(ctx context.Context, id string) (json.RawMessage, error) {
	for {
		t, err := g.broker.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		switch t.Status {
		case StatusCompleted:
			return t.Result, nil
		case StatusFailed, StatusCanceled:
			return nil, &TaskFailedError{ID: t.ID, Status: t.Status, LastError: t.LastError}
		}
		if err := g.clock.Sleep(ctx, waitPollInterval); err != nil {
			return nil, err
		}
	}
}

// Run 启动消费:按注册过 handler 的类型对应的队列起认领循环,阻塞到 ctx 取消,
// 然后停止认领、等在跑任务全部收尾后返回。生命周期细节在 scheduler.go。
func (g *Gate) Run(ctx context.Context) error {
	return g.sched.run(ctx)
}
