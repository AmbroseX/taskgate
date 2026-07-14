// integration_test.go L3 集成测试:走 Gate 公开 API 全链路,memory/sqlite 双后端参数化。
// 这里用真时钟 + 短时长(毫秒级),有竞态风险的断言一律走 waitFor 轮询,不靠碰运气。
package taskgate_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ambrose/taskgate"
	"github.com/ambrose/taskgate/internal/fakeclock"
	"github.com/ambrose/taskgate/memorybroker"
	"github.com/ambrose/taskgate/sqlitebroker"
)

// backends 双后端参数化:同一个场景在 memory 和 sqlite 上都得绿。
var backends = []struct {
	name string
	make func(t *testing.T) taskgate.Broker
}{
	{"memory", func(t *testing.T) taskgate.Broker {
		return memorybroker.New()
	}},
	{"sqlite", func(t *testing.T) taskgate.Broker {
		b, err := sqlitebroker.Open(filepath.Join(t.TempDir(), "gate.db"))
		if err != nil {
			t.Fatalf("打开 sqlite 后端失败: %v", err)
		}
		return b
	}},
}

// forEachBackend 把同一个场景跑遍所有后端。
func forEachBackend(t *testing.T, run func(t *testing.T, b taskgate.Broker)) {
	for _, be := range backends {
		t.Run(be.name, func(t *testing.T) {
			b := be.make(t)
			t.Cleanup(func() { _ = b.Close() })
			run(t, b)
		})
	}
}

// newGateOn 在指定后端上建 Gate,配置校验失败直接挂测试。
func newGateOn(t *testing.T, b taskgate.Broker, cfg taskgate.Config) *taskgate.Gate {
	t.Helper()
	cfg.Broker = b
	g, err := taskgate.New(cfg)
	if err != nil {
		t.Fatalf("New(cfg) 失败: %v", err)
	}
	return g
}

// startGate 起消费循环,返回幂等的 stop(测试结束自动调,手动调也行)。
func startGate(t *testing.T, g *taskgate.Gate) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := g.Run(ctx); err != nil {
			t.Errorf("Run 返回错误: %v", err)
		}
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(stop)
	return stop
}

// waitFor 带超时的轮询断言:竞态类场景不许一把梭直接断言,必须等到条件成立或超时。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待超时(%v): %s", timeout, msg)
}

// mustGet Get 一把,失败挂测试。
func mustGet(t *testing.T, g *taskgate.Gate, id string) *taskgate.Task {
	t.Helper()
	task, err := g.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%s) 失败: %v", id, err)
	}
	return task
}

// ---------- Phase 3(US1):提交 → 执行 → 取结果 ----------

// TestSubmitExecuteResult 最基本闭环:Submit → handler 执行 → Wait 拿 Result,
// 时间戳链 CreatedAt ≤ StartedAt ≤ FinishedAt 完整。
func TestSubmitExecuteResult(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"scoring": {Workers: 2}},
		})
		g.Handle("scoring", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			var in map[string]int
			if err := json.Unmarshal(task.Payload, &in); err != nil {
				return nil, err
			}
			return json.Marshal(map[string]int{"score": in["a"] + in["b"]})
		})
		startGate(t, g)

		id, err := g.Submit(context.Background(), "scoring", json.RawMessage(`{"a":1,"b":2}`))
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
		if id == "" {
			t.Fatal("Submit 应返回非空任务 ID")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := g.Wait(ctx, id)
		if err != nil {
			t.Fatalf("Wait 失败: %v", err)
		}
		if string(result) != `{"score":3}` {
			t.Fatalf("Result 不对: %s", result)
		}

		task := mustGet(t, g, id)
		if task.Status != taskgate.StatusCompleted {
			t.Fatalf("状态应为 completed,得到 %s", task.Status)
		}
		if task.CreatedAt.IsZero() || task.StartedAt.IsZero() || task.FinishedAt.IsZero() {
			t.Fatalf("时间戳不能有零值: %+v", task)
		}
		if task.StartedAt.Before(task.CreatedAt) || task.FinishedAt.Before(task.StartedAt) {
			t.Fatalf("时间戳链断了: created=%v started=%v finished=%v",
				task.CreatedAt, task.StartedAt, task.FinishedAt)
		}
	})
}

// TestWaitTimeout Wait 的 ctx 先到期:Wait 返回 ctx 错误,任务照常跑完不受影响。
func TestWaitTimeout(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"slow": {Workers: 1}},
		})
		g.Handle("slow", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			time.Sleep(1 * time.Second) // handler 睡 1s
			return []byte(`"done"`), nil
		})
		startGate(t, g)

		id, err := g.Submit(context.Background(), "slow", nil)
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		_, err = g.Wait(ctx, id)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Wait 应返回 ctx 超时错误,得到: %v", err)
		}

		// 任务本身不受 Wait 放弃的影响,照常跑完。
		waitFor(t, 5*time.Second, func() bool {
			return mustGet(t, g, id).Status == taskgate.StatusCompleted
		}, "任务应照常跑完")
	})
}

// TestStatsConsistency 造一批已知分布的任务,Overview / Stats / List 三个口径必须对得上。
func TestStatsConsistency(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{
				"alpha": {Workers: 1},
				"beta":  {Workers: 2},
			},
		})
		// 只注册 beta 的 handler:alpha 的 3 个任务保持 pending,beta 的 2 个跑成 completed。
		g.Handle("beta", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return []byte(`true`), nil
		})

		ctx := context.Background()
		for i := 0; i < 3; i++ {
			if _, err := g.Submit(ctx, "alpha", nil); err != nil {
				t.Fatalf("Submit alpha 失败: %v", err)
			}
		}
		var betaIDs []string
		for i := 0; i < 2; i++ {
			id, err := g.Submit(ctx, "beta", nil)
			if err != nil {
				t.Fatalf("Submit beta 失败: %v", err)
			}
			betaIDs = append(betaIDs, id)
		}

		startGate(t, g)
		for _, id := range betaIDs {
			if _, err := g.Wait(ctx, id); err != nil {
				t.Fatalf("Wait beta 失败: %v", err)
			}
		}

		// Overview:Type × Status 矩阵。
		ov, err := g.Overview(ctx)
		if err != nil {
			t.Fatalf("Overview 失败: %v", err)
		}
		if got := ov["alpha"][taskgate.StatusPending]; got != 3 {
			t.Fatalf("Overview alpha/pending 应为 3,得到 %d", got)
		}
		if got := ov["beta"][taskgate.StatusCompleted]; got != 2 {
			t.Fatalf("Overview beta/completed 应为 2,得到 %d", got)
		}

		// Stats:单队列水位。
		sa, err := g.Stats(ctx, "alpha")
		if err != nil {
			t.Fatalf("Stats alpha 失败: %v", err)
		}
		if sa.QueueLen != 3 || sa.Workers != 1 {
			t.Fatalf("Stats alpha 不对: %+v", sa)
		}
		sb, err := g.Stats(ctx, "beta")
		if err != nil {
			t.Fatalf("Stats beta 失败: %v", err)
		}
		if sb.QueueLen != 0 || sb.Workers != 2 || sb.Running != 0 {
			t.Fatalf("Stats beta 不对: %+v", sb)
		}

		// List:与上面两个口径交叉验证。
		pend, err := g.List(ctx, taskgate.Filter{Status: taskgate.StatusPending})
		if err != nil {
			t.Fatalf("List pending 失败: %v", err)
		}
		if len(pend) != 3 {
			t.Fatalf("List pending 应为 3 条,得到 %d", len(pend))
		}
		done, err := g.List(ctx, taskgate.Filter{Type: "beta", Status: taskgate.StatusCompleted})
		if err != nil {
			t.Fatalf("List beta/completed 失败: %v", err)
		}
		if len(done) != 2 {
			t.Fatalf("List beta/completed 应为 2 条,得到 %d", len(done))
		}
	})
}

// TestIdempotentID 同 ID 二次 Submit → ErrTaskExists,原任务不被覆盖。
func TestIdempotentID(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"idem": {Workers: 1}},
		})
		ctx := context.Background()

		id, err := g.Submit(ctx, "idem", json.RawMessage(`{"v":1}`), taskgate.WithID("job-1"))
		if err != nil {
			t.Fatalf("首次 Submit 失败: %v", err)
		}
		if id != "job-1" {
			t.Fatalf("自定义 ID 应原样返回,得到 %q", id)
		}

		_, err = g.Submit(ctx, "idem", json.RawMessage(`{"v":2}`), taskgate.WithID("job-1"))
		if !errors.Is(err, taskgate.ErrTaskExists) {
			t.Fatalf("二次 Submit 应返回 ErrTaskExists,得到: %v", err)
		}

		// 原任务的 Payload 没被第二次提交覆盖。
		if got := mustGet(t, g, "job-1"); string(got.Payload) != `{"v":1}` {
			t.Fatalf("原任务被覆盖了: %s", got.Payload)
		}
	})
}

// ---------- Phase 4(US2):限流隔离与队列路由 ----------

// TestThrottleIsolation 慢队列 {Workers:1,RPS:1} 被灌满,快队列 {Workers:8} 吞吐不受影响:
// 20 个快任务必须在几秒内全部完成(如果被慢队列拖着,1 RPS 跑 20 个要 20 秒)。
func TestThrottleIsolation(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{
				"slow": {Workers: 1, RPS: 1},
				"fast": {Workers: 8},
			},
		})
		g.Handle("slow", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return []byte(`"s"`), nil
		})
		g.Handle("fast", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return []byte(`"f"`), nil
		})

		ctx := context.Background()
		const nSlow, nFast = 5, 20
		for i := 0; i < nSlow; i++ {
			if _, err := g.Submit(ctx, "slow", nil); err != nil {
				t.Fatalf("Submit slow 失败: %v", err)
			}
		}
		for i := 0; i < nFast; i++ {
			if _, err := g.Submit(ctx, "fast", nil); err != nil {
				t.Fatalf("Submit fast 失败: %v", err)
			}
		}

		completed := func(typ string) int64 {
			ov, err := g.Overview(ctx)
			if err != nil {
				t.Fatalf("Overview 失败: %v", err)
			}
			return ov[typ][taskgate.StatusCompleted]
		}

		start := time.Now()
		startGate(t, g)

		// 快队列 3 秒内必须全部干完(不被慢队列的 1 RPS 拖累)。
		waitFor(t, 3*time.Second, func() bool {
			return completed("fast") == nFast
		}, "快队列 20 个任务应在 3 秒内全部完成")
		fastDone := time.Since(start)

		// 此刻慢队列受 RPS=1 限制,5 个不可能全部完成(需要 4 秒以上)。
		if got := completed("slow"); got >= nSlow {
			t.Fatalf("快队列完成时(%v)慢队列不该也全完成(完成 %d/%d),限流隔离失效",
				fastDone, got, nSlow)
		}

		// 收尾:慢队列最终也要全部完成,证明只是被限速而不是被卡死。
		waitFor(t, 10*time.Second, func() bool {
			return completed("slow") == nSlow
		}, "慢队列最终应全部完成")
	})
}

// TestRoutesRouting Routes{"review":"xunfei"}:纯生产者 Gate(只 New 不 Run)提交的任务
// 落进 xunfei 队列;共用同一后端的消费者 Gate 能认领并执行成功。
func TestRoutesRouting(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		cfg := taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"xunfei": {Workers: 2}},
			Routes: map[string]string{"review": "xunfei"},
		}

		// 纯生产者:不注册 handler、不 Run,只负责提交。
		producer := newGateOn(t, b, cfg)
		id, err := producer.Submit(context.Background(), "review", json.RawMessage(`{"doc":"x"}`))
		if err != nil {
			t.Fatalf("生产者 Submit 失败: %v", err)
		}
		if task := mustGet(t, producer, id); task.Queue != "xunfei" {
			t.Fatalf("review 类型应路由进 xunfei 队列,实际 %q", task.Queue)
		}

		// 消费者:同一个后端,注册 handler 并 Run,认领执行。
		consumer := newGateOn(t, b, cfg)
		consumer.Handle("review", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return []byte(`"reviewed"`), nil
		})
		startGate(t, consumer)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := producer.Wait(ctx, id)
		if err != nil {
			t.Fatalf("Wait 失败: %v", err)
		}
		if string(result) != `"reviewed"` {
			t.Fatalf("Result 不对: %s", result)
		}
	})
}

// ---------- Phase 5(US3):失败重试、死信与限流特化 ----------

// TestRetryChain 前 2 次业务失败、第 3 次成功:任务经历 2 次 retrying 后 completed,
// Attempts 记 2(Attempts 是"业务失败次数",成功那次不计,见 data-model 计数语义)。
// 退避换成 20ms 短退避,测试不用真等指数曲线。
func TestRetryChain(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"flaky": {Workers: 1}},
		})
		taskgate.SetBackoff(g, func(int) time.Duration { return 20 * time.Millisecond })

		var calls atomic.Int32
		g.Handle("flaky", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			if calls.Add(1) <= 2 {
				return nil, fmt.Errorf("第 %d 次故意失败", calls.Load())
			}
			return []byte(`"ok"`), nil
		})
		startGate(t, g)

		id, err := g.Submit(context.Background(), "flaky", nil, taskgate.MaxRetry(3))
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := g.Wait(ctx, id); err != nil {
			t.Fatalf("Wait 失败: %v", err)
		}

		task := mustGet(t, g, id)
		if task.Status != taskgate.StatusCompleted {
			t.Fatalf("最终状态应为 completed,得到 %s", task.Status)
		}
		if task.Attempts != 2 {
			t.Fatalf("业务失败 2 次,Attempts 应为 2,得到 %d", task.Attempts)
		}
		if got := calls.Load(); got != 3 {
			t.Fatalf("handler 应被执行 3 次,实际 %d", got)
		}
	})
}

// TestThrottledFlow handler 连续 3 次返回 ErrThrottled 后成功:
// Attempts 一点不涨、Throttled=3,任务按 RetryAfter 延后重排,最终 completed。
func TestThrottledFlow(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"gw": {Workers: 1}},
		})
		var calls atomic.Int32
		g.Handle("gw", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			if calls.Add(1) <= 3 {
				return nil, taskgate.ErrThrottled{RetryAfter: 30 * time.Millisecond}
			}
			return []byte(`"through"`), nil
		})
		startGate(t, g)

		id, err := g.Submit(context.Background(), "gw", nil)
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := g.Wait(ctx, id); err != nil {
			t.Fatalf("Wait 失败: %v", err)
		}

		task := mustGet(t, g, id)
		if task.Attempts != 0 {
			t.Fatalf("限流重排不占 Attempts,应为 0,得到 %d", task.Attempts)
		}
		if task.Throttled != 3 {
			t.Fatalf("Throttled 应为 3,得到 %d", task.Throttled)
		}
	})
}

// TestSkipRetry handler 返回 ErrSkipRetry:哪怕 MaxRetry 还有余量也直接 failed,不再重试。
func TestSkipRetry(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"strict": {Workers: 1}},
		})
		var calls atomic.Int32
		g.Handle("strict", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			calls.Add(1)
			return nil, taskgate.ErrSkipRetry{Err: errors.New("参数错误,重试也没救")}
		})
		startGate(t, g)

		id, err := g.Submit(context.Background(), "strict", nil, taskgate.MaxRetry(5))
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = g.Wait(ctx, id)
		var tfe *taskgate.TaskFailedError
		if !errors.As(err, &tfe) {
			t.Fatalf("Wait 应返回 *TaskFailedError,得到: %v", err)
		}
		if tfe.Status != taskgate.StatusFailed {
			t.Fatalf("终态应为 failed,得到 %s", tfe.Status)
		}

		task := mustGet(t, g, id)
		if task.Status != taskgate.StatusFailed {
			t.Fatalf("状态应为 failed,得到 %s", task.Status)
		}
		if task.Attempts != 0 {
			t.Fatalf("FailSkip 不占 Attempts,应为 0,得到 %d", task.Attempts)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("handler 只该执行 1 次,实际 %d", got)
		}
	})
}

// TestSkipRetryWrapsThrottled ErrSkipRetry 里包着 ErrThrottled 时,
// "明确不重试"必须赢:任务直接 failed,不许穿透匹配到内层限流走延后重排。
func TestSkipRetryWrapsThrottled(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"strict": {Workers: 1}},
		})
		var calls atomic.Int32
		g.Handle("strict", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			calls.Add(1)
			return nil, taskgate.ErrSkipRetry{Err: taskgate.ErrThrottled{RetryAfter: time.Second}}
		})
		startGate(t, g)

		id, err := g.Submit(context.Background(), "strict", nil, taskgate.MaxRetry(5))
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = g.Wait(ctx, id)
		var tfe *taskgate.TaskFailedError
		if !errors.As(err, &tfe) {
			t.Fatalf("Wait 应返回 *TaskFailedError,得到: %v", err)
		}
		if tfe.Status != taskgate.StatusFailed {
			t.Fatalf("终态应为 failed,得到 %s", tfe.Status)
		}

		task := mustGet(t, g, id)
		if task.Status != taskgate.StatusFailed {
			t.Fatalf("状态应为 failed,得到 %s", task.Status)
		}
		if task.Throttled != 0 {
			t.Fatalf("不该走限流重排,Throttled 应为 0,得到 %d", task.Throttled)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("handler 只该执行 1 次,实际 %d", got)
		}
	})
}

// TestThrottledCap ThrottledMax=2 封顶:第 2 次 ErrThrottled 就进 failed,
// LastError 用固定文案 "throttled 2 times"。
func TestThrottledCap(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues:       map[string]taskgate.QueueConfig{"gw": {Workers: 1}},
			ThrottledMax: 2,
		})
		g.Handle("gw", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return nil, taskgate.ErrThrottled{RetryAfter: 20 * time.Millisecond}
		})
		startGate(t, g)

		id, err := g.Submit(context.Background(), "gw", nil)
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = g.Wait(ctx, id)
		var tfe *taskgate.TaskFailedError
		if !errors.As(err, &tfe) {
			t.Fatalf("Wait 应返回 *TaskFailedError,得到: %v", err)
		}

		task := mustGet(t, g, id)
		if task.Status != taskgate.StatusFailed {
			t.Fatalf("封顶后状态应为 failed,得到 %s", task.Status)
		}
		if task.Throttled != 2 {
			t.Fatalf("Throttled 应停在封顶值 2,得到 %d", task.Throttled)
		}
		if task.LastError != "throttled 2 times" {
			t.Fatalf("LastError 应为固定文案 %q,得到 %q", "throttled 2 times", task.LastError)
		}
		if task.Attempts != 0 {
			t.Fatalf("限流封顶不占 Attempts,应为 0,得到 %d", task.Attempts)
		}
	})
}

// ---------- Phase 6(US4):租约、reaper 与自动续租 ----------

// TestPoisonTaskLeaseCap 毒任务:worker 认领后"进程假死"(不心跳、不回执)。
// 绕过 scheduler 直接操作 Broker + fakeclock,循环 3 次"认领→租约过期→ReapExpired 回收",
// LeaseLost 封顶(默认 3)后任务进 failed,LastError 用固定文案 "lease expired 3 times"。
func TestPoisonTaskLeaseCap(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		clk := fakeclock.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		if err := b.Init(taskgate.BrokerOptions{
			DefaultLeaseTTL: 200 * time.Millisecond,
			LeaseLostMax:    3,
			ThrottledMax:    100,
			Clock:           clk,
		}); err != nil {
			t.Fatalf("Init 失败: %v", err)
		}
		ctx := context.Background()

		poison := &taskgate.Task{ID: "poison-1", Type: "poison", Queue: "poison"}
		if err := b.Enqueue(ctx, poison); err != nil {
			t.Fatalf("Enqueue 失败: %v", err)
		}

		for i := 1; i <= 3; i++ {
			claimed, err := b.Dequeue(ctx, []string{"poison"})
			if err != nil {
				t.Fatalf("第 %d 轮 Dequeue 失败: %v", i, err)
			}
			if claimed.ID != "poison-1" {
				t.Fatalf("第 %d 轮认领到了别的任务: %s", i, claimed.ID)
			}
			// worker 假死:不心跳不回执,直接把时间推过租约期,让 reaper 回收。
			clk.Advance(201 * time.Millisecond)
			n, err := b.ReapExpired(ctx)
			if err != nil || n != 1 {
				t.Fatalf("第 %d 轮 ReapExpired 应回收 1 条,实际 n=%d err=%v", i, n, err)
			}

			got, err := b.Get(ctx, "poison-1")
			if err != nil {
				t.Fatalf("Get 失败: %v", err)
			}
			if got.LeaseLost != i {
				t.Fatalf("第 %d 轮后 LeaseLost 应为 %d,实际 %d", i, i, got.LeaseLost)
			}
			if i < 3 {
				if got.Status != taskgate.StatusPending {
					t.Fatalf("第 %d 轮后应回 pending 等重跑,实际 %s", i, got.Status)
				}
				continue
			}
			// 第 3 次封顶:进 failed 死信,固定文案。
			if got.Status != taskgate.StatusFailed {
				t.Fatalf("封顶后应为 failed,实际 %s", got.Status)
			}
			if got.LastError != "lease expired 3 times" {
				t.Fatalf("LastError 应为 %q,实际 %q", "lease expired 3 times", got.LastError)
			}
		}
	})
}

// TestSlowTaskAutoRenew 慢任务自动续租:LeaseTTL=300ms,handler 真跑 3×TTL(900ms),
// 期间心跳每 TTL/3 续租、reaper 每 TTL/2 在扫,任务全程不被误回收:
// 最终 completed 且 LeaseLost=0(SC-004 的"零误回收")。
func TestSlowTaskAutoRenew(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{
				"slow": {Workers: 1, LeaseTTL: taskgate.Duration(300 * time.Millisecond)},
			},
		})
		g.Handle("slow", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			time.Sleep(900 * time.Millisecond) // 3 倍租约时长,不续租必被回收
			return []byte(`"survived"`), nil
		})
		startGate(t, g)

		id, err := g.Submit(context.Background(), "slow", nil)
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result, err := g.Wait(ctx, id)
		if err != nil {
			t.Fatalf("Wait 失败: %v", err)
		}
		if string(result) != `"survived"` {
			t.Fatalf("Result 不对: %s", result)
		}

		task := mustGet(t, g, id)
		if task.Status != taskgate.StatusCompleted {
			t.Fatalf("状态应为 completed,实际 %s", task.Status)
		}
		if task.LeaseLost != 0 {
			t.Fatalf("自动续租下不该被回收,LeaseLost 应为 0,实际 %d", task.LeaseLost)
		}
	})
}

// ---------- Phase 7(US5):任务依赖与流水线 ----------

// TestPipelineThreeStages 三级流水线 A(summarize)→B(extract)→C(score):
// 每级 handler 用 Get(task.DependsOn[0]) 读父任务的 Result,把字段拼进自己的结果,
// 最终断言三级字段全部传递正确(SC-005)。
func TestPipelineThreeStages(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{
				"summarize": {Workers: 1},
				"extract":   {Workers: 1},
				"score":     {Workers: 1},
			},
		})
		// mergeParent 读父结果、附加一个自己的字段再返回,模拟真实流水线的结果传递。
		mergeParent := func(key, val string) taskgate.Handler {
			return func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
				parent, err := g.Get(ctx, task.DependsOn[0])
				if err != nil {
					return nil, err
				}
				out := map[string]string{}
				if err := json.Unmarshal(parent.Result, &out); err != nil {
					return nil, err
				}
				out[key] = val
				return json.Marshal(out)
			}
		}
		g.Handle("summarize", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return json.Marshal(map[string]string{"summary": "S"})
		})
		g.Handle("extract", mergeParent("keywords", "K"))
		g.Handle("score", mergeParent("score", "100"))
		startGate(t, g)

		ctx := context.Background()
		aid, err := g.Submit(ctx, "summarize", nil)
		if err != nil {
			t.Fatalf("Submit A 失败: %v", err)
		}
		bid, err := g.Submit(ctx, "extract", nil, taskgate.DependsOn(aid))
		if err != nil {
			t.Fatalf("Submit B 失败: %v", err)
		}
		cid, err := g.Submit(ctx, "score", nil, taskgate.DependsOn(bid))
		if err != nil {
			t.Fatalf("Submit C 失败: %v", err)
		}

		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		result, err := g.Wait(wctx, cid)
		if err != nil {
			t.Fatalf("Wait C 失败: %v", err)
		}
		var out map[string]string
		if err := json.Unmarshal(result, &out); err != nil {
			t.Fatalf("C 的结果不是合法 JSON: %s", result)
		}
		if out["summary"] != "S" || out["keywords"] != "K" || out["score"] != "100" {
			t.Fatalf("三级字段传递不对: %s", result)
		}
	})
}

// TestFanInDependency fan-in:C 依赖 A+B 两个父,必须两个都完成才被唤醒。
// 用一个放行 channel 卡住 B:A 先完成时断言 C 仍 blocked,放行 B 后 C 才跑。
func TestFanInDependency(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{
				"fast": {Workers: 1}, "gated": {Workers: 1}, "join": {Workers: 1},
			},
		})
		releaseB := make(chan struct{})
		g.Handle("fast", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return []byte(`"a"`), nil
		})
		g.Handle("gated", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			<-releaseB // 等测试放行
			return []byte(`"b"`), nil
		})
		g.Handle("join", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return []byte(`"c"`), nil
		})
		startGate(t, g)

		ctx := context.Background()
		aid, err := g.Submit(ctx, "fast", nil)
		if err != nil {
			t.Fatalf("Submit A 失败: %v", err)
		}
		bid, err := g.Submit(ctx, "gated", nil)
		if err != nil {
			t.Fatalf("Submit B 失败: %v", err)
		}
		cid, err := g.Submit(ctx, "join", nil, taskgate.DependsOn(aid, bid))
		if err != nil {
			t.Fatalf("Submit C 失败: %v", err)
		}

		// A 完成后 C 必须还卡在 blocked(B 没放行,fan-in 不能提前唤醒)。
		waitFor(t, 5*time.Second, func() bool {
			return mustGet(t, g, aid).Status == taskgate.StatusCompleted
		}, "A 应先完成")
		if got := mustGet(t, g, cid).Status; got != taskgate.StatusBlocked {
			t.Fatalf("只完成一个父时 C 应仍 blocked,实际 %s", got)
		}

		close(releaseB)
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if _, err := g.Wait(wctx, cid); err != nil {
			t.Fatalf("两个父都完成后 C 应跑完,Wait 失败: %v", err)
		}
	})
}

// TestIgnoreParentFailure 父失败照跑:A 直接 failed,C 带 IgnoreParentFailure 依赖 A,
// C 仍被唤醒并正常完成。
func TestIgnoreParentFailure(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"boom": {Workers: 1}, "tolerant": {Workers: 1}},
		})
		g.Handle("boom", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return nil, taskgate.ErrSkipRetry{Err: errors.New("父任务注定失败")}
		})
		g.Handle("tolerant", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return []byte(`"still ran"`), nil
		})
		startGate(t, g)

		ctx := context.Background()
		aid, err := g.Submit(ctx, "boom", nil)
		if err != nil {
			t.Fatalf("Submit A 失败: %v", err)
		}
		cid, err := g.Submit(ctx, "tolerant", nil,
			taskgate.DependsOn(aid), taskgate.IgnoreParentFailure())
		if err != nil {
			t.Fatalf("Submit C 失败: %v", err)
		}

		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		result, err := g.Wait(wctx, cid)
		if err != nil {
			t.Fatalf("IgnoreParentFailure 下 C 应照常跑完,Wait 失败: %v", err)
		}
		if string(result) != `"still ran"` {
			t.Fatalf("C 的结果不对: %s", result)
		}
		if got := mustGet(t, g, aid).Status; got != taskgate.StatusFailed {
			t.Fatalf("A 应为 failed,实际 %s", got)
		}
	})
}

// TestChildNotBlockedWhenParentDone 提交那一刻父已经 completed → 子直接 pending 入队,
// 不卡 blocked(可能已被立刻认领跑完,所以只断言"非 blocked")。
func TestChildNotBlockedWhenParentDone(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"step": {Workers: 1}},
		})
		g.Handle("step", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return []byte(`"ok"`), nil
		})
		startGate(t, g)

		ctx := context.Background()
		aid, err := g.Submit(ctx, "step", nil)
		if err != nil {
			t.Fatalf("Submit A 失败: %v", err)
		}
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := g.Wait(wctx, aid); err != nil {
			t.Fatalf("Wait A 失败: %v", err)
		}

		// 父已终态,子必须直接可跑(pending/running/completed 都行,唯独不能 blocked)。
		cid, err := g.Submit(ctx, "step", nil, taskgate.DependsOn(aid))
		if err != nil {
			t.Fatalf("Submit 子任务失败: %v", err)
		}
		if got := mustGet(t, g, cid).Status; got == taskgate.StatusBlocked {
			t.Fatal("父已完成,子不应卡在 blocked")
		}
	})
}

// TestFailFastCascade A 失败 → B(默认 FailFast)连锁 canceled 且 LastError 记明原因,
// B 的子任务 C 也逐层连锁 canceled(链式最终一致)。
func TestFailFastCascade(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{
				"src": {Workers: 1}, "mid": {Workers: 1}, "leaf": {Workers: 1},
			},
		})
		g.Handle("src", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return nil, taskgate.ErrSkipRetry{Err: errors.New("boom")}
		})

		// 先把三级链全部提交好(B/C 都在 blocked),再开消费,保证走"运行期连锁取消"路径,
		// 而不是"提交时父已失败"的直接判定路径。
		ctx := context.Background()
		aid, err := g.Submit(ctx, "src", nil)
		if err != nil {
			t.Fatalf("Submit A 失败: %v", err)
		}
		bid, err := g.Submit(ctx, "mid", nil, taskgate.DependsOn(aid))
		if err != nil {
			t.Fatalf("Submit B 失败: %v", err)
		}
		cid, err := g.Submit(ctx, "leaf", nil, taskgate.DependsOn(bid))
		if err != nil {
			t.Fatalf("Submit C 失败: %v", err)
		}
		startGate(t, g)

		waitFor(t, 5*time.Second, func() bool {
			return mustGet(t, g, cid).Status == taskgate.StatusCanceled
		}, "C 应被连锁取消")

		bt := mustGet(t, g, bid)
		if bt.Status != taskgate.StatusCanceled {
			t.Fatalf("B 应为 canceled,实际 %s", bt.Status)
		}
		if want := "parent " + aid + " failed"; bt.LastError != want {
			t.Fatalf("B 的 LastError 应为 %q,实际 %q", want, bt.LastError)
		}
		if got := mustGet(t, g, aid).Status; got != taskgate.StatusFailed {
			t.Fatalf("A 应为 failed,实际 %s", got)
		}
	})
}

// TestDependsOnMissingParent DependsOn 指向不存在的任务 → Submit 拒收,
// 错误可用 errors.Is(ErrTaskNotFound) 判断(依赖无环靠这条提交校验)。
func TestDependsOnMissingParent(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"step": {Workers: 1}},
		})
		_, err := g.Submit(context.Background(), "step", nil, taskgate.DependsOn("no-such-parent"))
		if !errors.Is(err, taskgate.ErrTaskNotFound) {
			t.Fatalf("父不存在应报 ErrTaskNotFound,实际: %v", err)
		}
	})
}

// TestCancelMidPipeline 流水线中途取消:A 已 completed、B 还在 pending 时 Cancel B →
// B canceled,孙 C 连锁 canceled,A 保持 completed 不受影响。
func TestCancelMidPipeline(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{
				"head": {Workers: 1}, "mid": {Workers: 1}, "tail": {Workers: 1},
			},
		})
		// 只消费 head:B 被父完成唤醒成 pending 后没人认领,停在可取消的位置。
		g.Handle("head", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return []byte(`"a"`), nil
		})
		startGate(t, g)

		ctx := context.Background()
		aid, err := g.Submit(ctx, "head", nil)
		if err != nil {
			t.Fatalf("Submit A 失败: %v", err)
		}
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := g.Wait(wctx, aid); err != nil {
			t.Fatalf("Wait A 失败: %v", err)
		}
		bid, err := g.Submit(ctx, "mid", nil, taskgate.DependsOn(aid))
		if err != nil {
			t.Fatalf("Submit B 失败: %v", err)
		}
		cid, err := g.Submit(ctx, "tail", nil, taskgate.DependsOn(bid))
		if err != nil {
			t.Fatalf("Submit C 失败: %v", err)
		}
		if got := mustGet(t, g, bid).Status; got != taskgate.StatusPending {
			t.Fatalf("前置条件不成立:B 应为 pending,实际 %s", got)
		}

		if err := g.Cancel(ctx, bid); err != nil {
			t.Fatalf("Cancel B 失败: %v", err)
		}
		if got := mustGet(t, g, bid).Status; got != taskgate.StatusCanceled {
			t.Fatalf("B 应为 canceled,实际 %s", got)
		}
		waitFor(t, 5*time.Second, func() bool {
			return mustGet(t, g, cid).Status == taskgate.StatusCanceled
		}, "孙任务 C 应被连锁取消")
		if got := mustGet(t, g, aid).Status; got != taskgate.StatusCompleted {
			t.Fatalf("A 应保持 completed,实际 %s", got)
		}
	})
}

// ---------- Phase 8(US6):取消 ----------

// TestCancelRunning 取消 running 任务:Cancel 后本进程 handler 的 ctx 立即被 cancel,
// handler 退出后任务以 canceled 落库,Wait 返回 *TaskFailedError(Status=canceled)。
func TestCancelRunning(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"long": {Workers: 1}},
		})
		started := make(chan struct{})
		ctxCanceled := make(chan struct{})
		g.Handle("long", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			close(started)
			<-ctx.Done() // 老老实实听取消信号
			close(ctxCanceled)
			return nil, ctx.Err()
		})
		startGate(t, g)

		ctx := context.Background()
		id, err := g.Submit(ctx, "long", nil)
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("等待 handler 开跑超时")
		}

		if err := g.Cancel(ctx, id); err != nil {
			t.Fatalf("Cancel 失败: %v", err)
		}
		// handler 的 ctx 必须真的被 cancel(本进程即时路径,不用等 Heartbeat)。
		select {
		case <-ctxCanceled:
		case <-time.After(5 * time.Second):
			t.Fatal("Cancel 后 handler 的 ctx 应立即被取消")
		}

		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err = g.Wait(wctx, id)
		var tfe *taskgate.TaskFailedError
		if !errors.As(err, &tfe) {
			t.Fatalf("Wait 应返回 *TaskFailedError,实际: %v", err)
		}
		if tfe.Status != taskgate.StatusCanceled {
			t.Fatalf("终态应为 canceled,实际 %s", tfe.Status)
		}
		if got := mustGet(t, g, id); got.Status != taskgate.StatusCanceled {
			t.Fatalf("落库状态应为 canceled,实际 %s", got.Status)
		}
	})
}

// TestCancelBlockedPropagation 取消 blocked 任务向下传播:
// 父还 pending 没人跑,子 blocked、孙 blocked;Cancel 子 → 子和孙都 canceled,父不动。
func TestCancelBlockedPropagation(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		// 不起消费循环:父保持 pending,子孙保持 blocked,纯验证取消传播。
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"chain": {Workers: 1}},
		})
		ctx := context.Background()
		pid, err := g.Submit(ctx, "chain", nil)
		if err != nil {
			t.Fatalf("Submit 父失败: %v", err)
		}
		cid, err := g.Submit(ctx, "chain", nil, taskgate.DependsOn(pid))
		if err != nil {
			t.Fatalf("Submit 子失败: %v", err)
		}
		gid, err := g.Submit(ctx, "chain", nil, taskgate.DependsOn(cid))
		if err != nil {
			t.Fatalf("Submit 孙失败: %v", err)
		}
		if got := mustGet(t, g, cid).Status; got != taskgate.StatusBlocked {
			t.Fatalf("前置条件不成立:子应为 blocked,实际 %s", got)
		}

		if err := g.Cancel(ctx, cid); err != nil {
			t.Fatalf("Cancel 子失败: %v", err)
		}
		if got := mustGet(t, g, cid).Status; got != taskgate.StatusCanceled {
			t.Fatalf("子应为 canceled,实际 %s", got)
		}
		gt := mustGet(t, g, gid)
		if gt.Status != taskgate.StatusCanceled {
			t.Fatalf("孙应被连锁取消,实际 %s", gt.Status)
		}
		if want := "parent " + cid + " canceled"; gt.LastError != want {
			t.Fatalf("孙的 LastError 应为 %q,实际 %q", want, gt.LastError)
		}
		if got := mustGet(t, g, pid).Status; got != taskgate.StatusPending {
			t.Fatalf("父不受影响,应保持 pending,实际 %s", got)
		}
	})
}

// TestCancelFinalTask 终态任务再 Cancel → ErrAlreadyFinal。
func TestCancelFinalTask(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"quick": {Workers: 1}},
		})
		g.Handle("quick", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return []byte(`"done"`), nil
		})
		startGate(t, g)

		ctx := context.Background()
		id, err := g.Submit(ctx, "quick", nil)
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := g.Wait(wctx, id); err != nil {
			t.Fatalf("Wait 失败: %v", err)
		}

		if err := g.Cancel(ctx, id); !errors.Is(err, taskgate.ErrAlreadyFinal) {
			t.Fatalf("终态 Cancel 应报 ErrAlreadyFinal,实际: %v", err)
		}
	})
}

// ---------- Phase 9(US7):Shutdown 优雅停止 ----------

// TestShutdownGraceful Shutdown 正常路径:3 个任务在跑,Shutdown(5s) 等它们全部干完
// 才返回 nil;停机期间新 Submit 一律 ErrShutdown;重复 Shutdown 幂等。
func TestShutdownGraceful(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"work": {Workers: 3}},
		})
		started := make(chan struct{}, 3)
		release := make(chan struct{})
		g.Handle("work", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			started <- struct{}{}
			<-release // 卡住,等测试放行
			return []byte(`"done"`), nil
		})
		startGate(t, g)

		ctx := context.Background()
		ids := make([]string, 3)
		for i := range ids {
			id, err := g.Submit(ctx, "work", nil)
			if err != nil {
				t.Fatalf("Submit 失败: %v", err)
			}
			ids[i] = id
		}
		for i := 0; i < 3; i++ {
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("等待 3 个任务开跑超时")
			}
		}

		// 后台发起 Shutdown(额度 5s,足够任务善终)。
		shutdownErr := make(chan error, 1)
		go func() {
			sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			shutdownErr <- g.Shutdown(sctx)
		}()

		// 停机标记生效后,新 Submit 必须拒收(标记在 Shutdown 一进门就置位,轮询等它可见)。
		waitFor(t, 5*time.Second, func() bool {
			_, err := g.Submit(ctx, "work", nil)
			return errors.Is(err, taskgate.ErrShutdown)
		}, "Shutdown 期间 Submit 应返回 ErrShutdown")

		// 放行 handler:3 个在跑任务善终后 Shutdown 返回 nil。
		close(release)
		select {
		case err := <-shutdownErr:
			if err != nil {
				t.Fatalf("Shutdown 应返回 nil,得到: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("等待 Shutdown 返回超时")
		}
		for _, id := range ids {
			if got := mustGet(t, g, id).Status; got != taskgate.StatusCompleted {
				t.Fatalf("任务 %s 应为 completed,实际 %s", id, got)
			}
		}

		// 重复 Shutdown 幂等:第二次直接返回 nil。
		if err := g.Shutdown(context.Background()); err != nil {
			t.Fatalf("重复 Shutdown 应返回 nil,得到: %v", err)
		}
	})
}

// TestShutdownTimeout Shutdown 超时路径:handler 卡在 ctx.Done 上,Shutdown(300ms) 到点后
// cancel 它的 ctx、等它退出并 Requeue 归还,返回 ctx 超时错误;
// 任务回 pending 且 Attempts/LeaseLost/Throttled 全 0、RunAt 不变(Requeue 合同:不占任何计数)。
func TestShutdownTimeout(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"stuck": {Workers: 1}},
		})
		started := make(chan struct{})
		g.Handle("stuck", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			close(started)
			<-ctx.Done() // 只听取消信号,自己绝不主动结束
			return nil, ctx.Err()
		})
		startGate(t, g)

		ctx := context.Background()
		id, err := g.Submit(ctx, "stuck", nil)
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
		runAtBefore := mustGet(t, g, id).RunAt
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("等待任务开跑超时")
		}

		sctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		if err := g.Shutdown(sctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown 超时应返回 ctx 超时错误,得到: %v", err)
		}

		// Shutdown 返回时 Requeue 已落库:回 pending,三计数与 RunAt 全不动。
		task := mustGet(t, g, id)
		if task.Status != taskgate.StatusPending {
			t.Fatalf("被打断的任务应回 pending,实际 %s", task.Status)
		}
		if task.Attempts != 0 || task.LeaseLost != 0 || task.Throttled != 0 {
			t.Fatalf("Requeue 不占任何计数,实际 Attempts=%d LeaseLost=%d Throttled=%d",
				task.Attempts, task.LeaseLost, task.Throttled)
		}
		if !task.RunAt.Equal(runAtBefore) {
			t.Fatalf("Requeue 不动 RunAt,之前 %v,之后 %v", runAtBefore, task.RunAt)
		}

		// 停机后新 Submit 拒收。
		if _, err := g.Submit(ctx, "stuck", nil); !errors.Is(err, taskgate.ErrShutdown) {
			t.Fatalf("停机后 Submit 应返回 ErrShutdown,得到: %v", err)
		}
	})
}

// ---------- Phase 10(US8):OnStateChange 状态变更回调 ----------

// stateRecorder 并发安全地收集回调快照。Notify 是异步送达,断言一律走 waitFor 轮询。
type stateRecorder struct {
	mu    sync.Mutex
	snaps []taskgate.Task
}

func (r *stateRecorder) record(t taskgate.Task) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snaps = append(r.snaps, t)
}

// statusesOf 返回指定任务按送达顺序收到的状态序列。
func (r *stateRecorder) statusesOf(id string) []taskgate.Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []taskgate.Status
	for _, s := range r.snaps {
		if s.ID == id {
			out = append(out, s.Status)
		}
	}
	return out
}

// contains 序列里是否出现过某状态。
func containsStatus(seq []taskgate.Status, want taskgate.Status) bool {
	for _, s := range seq {
		if s == want {
			return true
		}
	}
	return false
}

// TestOnStateChangeSequence 注册 OnStateChange 后跑一个任务:
// 回调至少能观测到 pending、running、completed 三次流转,快照字段(ID/Type/Result)正确。
func TestOnStateChangeSequence(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		rec := &stateRecorder{}
		g := newGateOn(t, b, taskgate.Config{
			Queues:        map[string]taskgate.QueueConfig{"watch": {Workers: 1}},
			OnStateChange: rec.record,
		})
		g.Handle("watch", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return []byte(`"observed"`), nil
		})
		startGate(t, g)

		ctx := context.Background()
		id, err := g.Submit(ctx, "watch", json.RawMessage(`{"k":1}`))
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := g.Wait(wctx, id); err != nil {
			t.Fatalf("Wait 失败: %v", err)
		}

		// Notify 异步:等三种状态全部送达,不假设即时性。
		waitFor(t, 5*time.Second, func() bool {
			seq := rec.statusesOf(id)
			return containsStatus(seq, taskgate.StatusPending) &&
				containsStatus(seq, taskgate.StatusRunning) &&
				containsStatus(seq, taskgate.StatusCompleted)
		}, "回调应观测到 pending/running/completed 三次流转")

		// 快照字段抽查:completed 那条要带 Type 与 Result。
		rec.mu.Lock()
		defer rec.mu.Unlock()
		for _, s := range rec.snaps {
			if s.ID != id || s.Status != taskgate.StatusCompleted {
				continue
			}
			if s.Type != "watch" {
				t.Fatalf("快照 Type 应为 watch,实际 %q", s.Type)
			}
			if string(s.Result) != `"observed"` {
				t.Fatalf("completed 快照应带 Result,实际 %s", s.Result)
			}
			return
		}
		t.Fatal("没找到 completed 的快照")
	})
}

// TestOnStateChangePanic 回调每次都 panic:主流程完全不受影响,任务照常 completed。
func TestOnStateChangePanic(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		var fired atomic.Int32
		g := newGateOn(t, b, taskgate.Config{
			Queues: map[string]taskgate.QueueConfig{"boomcb": {Workers: 1}},
			OnStateChange: func(task taskgate.Task) {
				fired.Add(1)
				panic("回调故意炸")
			},
		})
		g.Handle("boomcb", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			return []byte(`"fine"`), nil
		})
		startGate(t, g)

		ctx := context.Background()
		id, err := g.Submit(ctx, "boomcb", nil)
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		result, err := g.Wait(wctx, id)
		if err != nil {
			t.Fatalf("回调 panic 不该影响任务,Wait 失败: %v", err)
		}
		if string(result) != `"fine"` {
			t.Fatalf("Result 不对: %s", result)
		}
		// 回调确实被触发过(panic 被 recover 吃掉,不是压根没调)。
		waitFor(t, 5*time.Second, func() bool { return fired.Load() >= 1 },
			"回调应至少被触发一次")
	})
}

// TestOnStateChangeProducerOnly 纯生产者 Gate(不 Handle 不 Run):
// Submit 本身也是一次流转(→ pending),回调同样要送达。
func TestOnStateChangeProducerOnly(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b taskgate.Broker) {
		rec := &stateRecorder{}
		g := newGateOn(t, b, taskgate.Config{
			Queues:        map[string]taskgate.QueueConfig{"produce": {Workers: 1}},
			OnStateChange: rec.record,
		})

		id, err := g.Submit(context.Background(), "produce", nil)
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
		waitFor(t, 5*time.Second, func() bool {
			return containsStatus(rec.statusesOf(id), taskgate.StatusPending)
		}, "纯生产者的 Submit 也应触发 pending 回调")
	})
}
