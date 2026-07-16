// Package brokertest 是 Broker 行为契约的统一验收套件。
// memory/sqlite/redis 三个后端跑同一套 18 条契约用例(见 contracts/broker-contract.md),
// 用例只断言语义、不断言实现手段:后端用轮询还是 Cond 唤醒都算合法,只要行为对。
// 时间一律用 fakeclock 手动推进,禁止真 sleep;唯一的例外是阻塞语义用例里
// 等 goroutine 结果的短观察窗,并且全部带超时保护。
package brokertest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/AmbroseX/taskgate/internal/fakeclock"
)

// Factory 由各后端的测试文件提供:用给定选项构造一个**已 Init**的空 broker。
// 套件会为每条用例注入独立的 fakeclock 与默认参数(LeaseTTL 60s、LeaseLostMax 3、
// ThrottledMax 100),个别用例会按需调小上限(比如 ThrottledMax)。
type Factory func(t *testing.T, opts taskgate.BrokerOptions) taskgate.Broker

// baseTime 所有用例的时间起点。取整毫秒对齐的固定时刻,
// 这样即使后端按 unix 毫秒落库(sqlite),时间戳也能精确比对。
var baseTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

const (
	// waitTimeout 等 goroutine 结果的保护超时(真时间),防止实现有 bug 时测试挂死。
	waitTimeout = 5 * time.Second
	// blockProbe 判定"仍在阻塞"的观察窗口(真时间)。窗口只影响误报为通过的等待时长,
	// 不影响正确性:实现若错误地返回了任务,会立刻被抓到。
	blockProbe = 100 * time.Millisecond
)

// contractCase 一条契约用例:名字 + 可选的选项微调 + 用例体。
// notify=true 的用例会在构造 broker 前给选项装上 Notify 收集器(挂到 harness.notify)。
type contractCase struct {
	name   string
	tune   func(*taskgate.BrokerOptions) // nil = 用默认选项
	run    func(t *testing.T, h *harness)
	notify bool
}

// allCases 18 条契约,顺序与 contracts/broker-contract.md 的清单一致。
var allCases = []contractCase{
	{name: "RoundTrip", run: caseRoundTrip},
	{name: "IdempotentID", run: caseIdempotentID},
	{name: "ClaimMutex", run: caseClaimMutex},
	{name: "BlockingDequeue", run: caseBlockingDequeue},
	{name: "DelayedTask", run: caseDelayedTask},
	// Throttled 封顶要真实触发,默认 100 次太啰嗦,调小到 3。
	{name: "AckFail", tune: func(o *taskgate.BrokerOptions) { o.ThrottledMax = 3 }, run: caseAckFail},
	{name: "LeaseReap", run: caseLeaseReap},
	{name: "StaleToken", run: caseStaleToken},
	{name: "RetryingReclaim", run: caseRetryingReclaim},
	{name: "DepWake", run: caseDepWake},
	{name: "CascadeCancel", run: caseCascadeCancel},
	{name: "CancelStates", run: caseCancelStates},
	{name: "CountsConsistency", run: caseCountsConsistency},
	{name: "ListFilter", run: caseListFilter},
	{name: "RequeueNoCount", run: caseRequeueNoCount},
	{name: "IllegalTransition", run: caseIllegalTransition},
	{name: "Notify", run: caseNotify, notify: true},
	{name: "ListPagination", run: caseListPagination},
}

// Run 对 factory 构造的后端跑全部 18 条契约。这是所有后端的统一验收入口:
// 后端测试文件里一行 brokertest.Run(t, factory) 即接入。
func Run(t *testing.T, factory Factory) {
	for _, c := range allCases {
		t.Run(c.name, func(t *testing.T) {
			clk := fakeclock.New(baseTime)
			opts := taskgate.BrokerOptions{
				DefaultLeaseTTL: 60 * time.Second,
				LeaseLostMax:    3,
				ThrottledMax:    100,
				Clock:           clk,
			}
			if c.tune != nil {
				c.tune(&opts)
			}
			h := &harness{clk: clk}
			if c.notify {
				h.notify = &notifyCollector{}
				opts.Notify = h.notify.record
			}
			b := factory(t, opts)
			if b == nil {
				t.Fatal("factory 返回了 nil broker:必须返回一个已 Init 的空 broker")
			}
			t.Cleanup(func() { _ = b.Close() })
			h.b = b
			c.run(t, h)
		})
	}
}

// harness 一条用例的运行环境:broker + 它专属的假时钟(+ 可选的 Notify 收集器)。
type harness struct {
	b      taskgate.Broker
	clk    *fakeclock.Clock
	notify *notifyCollector // 仅 notify=true 的用例非 nil
}

// advance 推进假时间。所有"过了多久"一律走这里,不真 sleep。
func (h *harness) advance(d time.Duration) { h.clk.Advance(d) }

// now 当前假时刻。
func (h *harness) now() time.Time { return h.clk.Now() }

// task 造一个最小任务。queue 留空时直接用 type 当队列名(套件内约定,broker 不做路由)。
func task(id, typ, queue string) *taskgate.Task {
	if queue == "" {
		queue = typ
	}
	return &taskgate.Task{ID: id, Type: typ, Queue: queue}
}

// enqueue 入队并断言成功;顺带断言 broker 回填了生成的 ID(否则调用方拿不到任务号)。
func (h *harness) enqueue(t *testing.T, tk *taskgate.Task) *taskgate.Task {
	t.Helper()
	if err := h.b.Enqueue(context.Background(), tk); err != nil {
		t.Fatalf("Enqueue(type=%s id=%q) 意外失败: %v", tk.Type, tk.ID, err)
	}
	if tk.ID == "" {
		t.Fatal("Enqueue 后必须把生成的 ulid 回填到 t.ID,否则调用方无法追踪任务")
	}
	return tk
}

// get 取任务并断言存在。
func (h *harness) get(t *testing.T, id string) *taskgate.Task {
	t.Helper()
	tk, err := h.b.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%s) 意外失败: %v", id, err)
	}
	return tk
}

// mustStatus 断言任务处于指定状态,失败时带上 LastError 方便排查。
func (h *harness) mustStatus(t *testing.T, id string, want taskgate.Status) *taskgate.Task {
	t.Helper()
	tk := h.get(t, id)
	if tk.Status != want {
		t.Fatalf("任务 %s 状态应为 %s,实际 %s(Attempts=%d LeaseLost=%d Throttled=%d LastError=%q)",
			id, want, tk.Status, tk.Attempts, tk.LeaseLost, tk.Throttled, tk.LastError)
	}
	return tk
}

// dequeue 断言"队列里现在就有就绪任务",立即认领并返回;拿不到算失败(带保护超时)。
func (h *harness) dequeue(t *testing.T, queues ...string) *taskgate.Task {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	tk, err := h.b.Dequeue(ctx, queues)
	if err != nil {
		t.Fatalf("Dequeue(%v) 应立即认领到就绪任务,却失败: %v", queues, err)
	}
	if tk.Status != taskgate.StatusRunning {
		t.Fatalf("Dequeue(%v) 返回的任务 %s 状态应为 running,实际 %s", queues, tk.ID, tk.Status)
	}
	if tk.LeaseToken == "" {
		t.Fatalf("Dequeue(%v) 返回的任务 %s 必须携带新生成的租约令牌", queues, tk.ID)
	}
	return tk
}

// expectBlocked 断言"现在没有就绪任务可认领":在观察窗内 Dequeue 必须一直阻塞。
func (h *harness) expectBlocked(t *testing.T, queues ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), blockProbe)
	defer cancel()
	tk, err := h.b.Dequeue(ctx, queues)
	if err == nil {
		t.Fatalf("Dequeue(%v) 此刻不应有就绪任务,却认领到了 %s(status=%s run_at=%v now=%v)",
			queues, tk.ID, tk.Status, tk.RunAt, h.now())
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dequeue(%v) 阻塞到 ctx 超时应返回 context.DeadlineExceeded,实际 %v", queues, err)
	}
}

// dequeueResult 异步认领的结果。
type dequeueResult struct {
	task *taskgate.Task
	err  error
}

// asyncDequeue 起 goroutine 做阻塞认领,结果从 channel 取(配 waitResult/stillBlocked 用)。
func (h *harness) asyncDequeue(ctx context.Context, queues ...string) <-chan dequeueResult {
	ch := make(chan dequeueResult, 1)
	go func() {
		tk, err := h.b.Dequeue(ctx, queues)
		ch <- dequeueResult{task: tk, err: err}
	}()
	return ch
}

// waitResult 等异步认领出结果,超过保护超时判失败。what 描述在等什么,用于失败信息。
func waitResult(t *testing.T, ch <-chan dequeueResult, what string) dequeueResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(waitTimeout):
		t.Fatalf("等待 %s 超过 %v 仍无结果:阻塞的 Dequeue 没有被唤醒(检查唤醒是否挂在 clock/入队事件上)", what, waitTimeout)
		return dequeueResult{}
	}
}

// stillBlocked 断言异步认领在观察窗内没有返回(仍在阻塞)。
func stillBlocked(t *testing.T, ch <-chan dequeueResult, what string) {
	t.Helper()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("%s:Dequeue 应保持阻塞,却提前出错返回: %v", what, r.err)
		}
		t.Fatalf("%s:Dequeue 应保持阻塞,却认领到了任务 %s(status=%s)", what, r.task.ID, r.task.Status)
	case <-time.After(blockProbe):
		// 观察窗内没动静,符合预期
	}
}
