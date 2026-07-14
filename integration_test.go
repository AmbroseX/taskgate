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
