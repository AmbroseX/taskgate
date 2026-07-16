// pipeline_test.go L4 仿真 E2E:mock LLM/OCR 网关 + 故障注入 + 完整流水线(测试方案第 4 节五用例)。
// 全部用 memory 后端(后端矩阵 L2/L3 已盖,这里测的是"限流×故障×流水线"的组合行为),
// handler 用真 http.Client 打 mockgw:busy 事件 → ErrThrottled 重排;HTTP 500/断连 → 普通重试。
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/AmbroseX/taskgate/e2e/mockgw"
	"github.com/AmbroseX/taskgate/memorybroker"
)

// newGate memory 后端上建 Gate,配置有问题直接挂测试。
func newGate(t *testing.T, cfg taskgate.Config) *taskgate.Gate {
	t.Helper()
	cfg.Broker = memorybroker.New()
	g, err := taskgate.New(cfg)
	if err != nil {
		t.Fatalf("New(cfg) 失败: %v", err)
	}
	return g
}

// startGate 起消费循环,返回幂等的 stop(测试结束自动调)。
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

// waitFor 带超时的轮询断言:所有等待一律轮询到条件成立或超时,不靠碰运气。
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

// statusCount 按 Type 数指定状态的任务数(走 Overview)。
func statusCount(t *testing.T, g *taskgate.Gate, typ string, st taskgate.Status) int64 {
	t.Helper()
	ov, err := g.Overview(context.Background())
	if err != nil {
		t.Fatalf("Overview 失败: %v", err)
	}
	return ov[typ][st]
}

// gwEvent mock 网关 SSE 体里那条 data 事件的解析结果。
type gwEvent struct {
	OK    bool            `json:"ok"`
	Error string          `json:"error"`
	Echo  json.RawMessage `json:"echo"`
}

// callGateway 真 http.Client 打 mockgw 并解析 SSE 体。
// 返回:事件(HTTP 200 时);错误(断连/HTTP 500 等,交给普通重试路径)。
func callGateway(url string, payload []byte) (*gwEvent, error) {
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return nil, err // 断连等网络错误
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("网关返回 HTTP %d: %s", resp.StatusCode, body)
	}
	line := strings.TrimPrefix(strings.TrimSpace(string(body)), "data: ")
	var ev gwEvent
	if err := json.Unmarshal([]byte(line), &ev); err != nil {
		return nil, fmt.Errorf("解析网关响应失败: %w(body=%q)", err, body)
	}
	return &ev, nil
}

// gatewayHandler 标准网关 handler:打 mockgw,busy 事件 → ErrThrottled{150ms} 延后重排
// (HTTP 是 200,只有解析 body 才能发现——这正是"状态码骗人"的判定位置);
// 其余错误原样返回走普通重试。throttled 计数器可选,用来断言 busy 真的触发过。
func gatewayHandler(url string, throttled *atomic.Int64) taskgate.Handler {
	return func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		ev, err := callGateway(url, task.Payload)
		if err != nil {
			return nil, err
		}
		if ev.Error == "busy" {
			if throttled != nil {
				throttled.Add(1)
			}
			return nil, taskgate.ErrThrottled{RetryAfter: 150 * time.Millisecond}
		}
		return json.Marshal(map[string]bool{"done": true})
	}
}

// ---------- 用例①:限流挡 busy(测试方案 4.1 / spec US1 场景 1) ----------

// TestE2EBusyThrottle mock LLM 并发>2 返 busy(藏在 200 体里):
//   - 档一 {Workers:2}:限流卡在网关红线内 → 100 个任务全 completed,
//     网关观测最大并发 ≤2 且全程零 busy(限流真的挡住了);
//   - 档二 {Workers:5}:冲破红线 → busy 真实发生,但全部走 ErrThrottled 延后重排,
//     最终零 failed(Throttled 计数 >0 证明真的触发过)。
func TestE2EBusyThrottle(t *testing.T) {
	const nTasks = 100
	ctx := context.Background()

	// 档一:{Workers:2},恰好卡在红线内。
	gw := mockgw.New(mockgw.Latency(50*time.Millisecond), mockgw.BusyAfterConcurrency(2))
	defer gw.Close()
	g := newGate(t, taskgate.Config{
		Queues: map[string]taskgate.QueueConfig{"llm": {Workers: 2}},
	})
	g.Handle("llm", gatewayHandler(gw.URL(), nil))
	startGate(t, g)
	for i := 0; i < nTasks; i++ {
		if _, err := g.Submit(ctx, "llm", json.RawMessage(fmt.Sprintf(`{"doc":%d}`, i))); err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
	}
	waitFor(t, 30*time.Second, func() bool {
		return statusCount(t, g, "llm", taskgate.StatusCompleted) == nTasks
	}, "档一 100 个任务应全部 completed")
	if got := gw.MaxConcurrency(); got > 2 {
		t.Fatalf("{Workers:2} 档网关观测最大并发应 ≤2,实际 %d", got)
	}
	if got := gw.BusyCount(); got != 0 {
		t.Fatalf("{Workers:2} 档不该触发任何 busy,实际 %d 次", got)
	}
	t.Logf("档一观测: MaxConcurrency=%d BusyCount=%d Requests=%d",
		gw.MaxConcurrency(), gw.BusyCount(), gw.Requests())

	// 档二:{Workers:5},故意冲破红线,busy 走 ErrThrottled 重排。
	gw2 := mockgw.New(mockgw.Latency(50*time.Millisecond), mockgw.BusyAfterConcurrency(2))
	defer gw2.Close()
	var throttled atomic.Int64
	g2 := newGate(t, taskgate.Config{
		Queues: map[string]taskgate.QueueConfig{"llm": {Workers: 5}},
		// busy 是快速拒绝,5 个 worker 空转时判定频率很高(实测平均每任务 ~25 次):
		// 抬高封顶,防止满负载 CI 下个别倒霉任务撞上默认 100 次的 Throttled 封顶。
		ThrottledMax: 1000,
	})
	g2.Handle("llm", gatewayHandler(gw2.URL(), &throttled))
	startGate(t, g2)
	for i := 0; i < nTasks; i++ {
		if _, err := g2.Submit(ctx, "llm", json.RawMessage(fmt.Sprintf(`{"doc":%d}`, i))); err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
	}
	waitFor(t, 60*time.Second, func() bool {
		return statusCount(t, g2, "llm", taskgate.StatusCompleted) == nTasks
	}, "档二 100 个任务应全部 completed(busy 走重排不走 failed)")
	if got := statusCount(t, g2, "llm", taskgate.StatusFailed); got != 0 {
		t.Fatalf("档二应零 failed,实际 %d", got)
	}
	if throttled.Load() == 0 || gw2.BusyCount() == 0 {
		t.Fatalf("档二 busy 应真实触发过: handler 判定 %d 次,网关 busy %d 次",
			throttled.Load(), gw2.BusyCount())
	}
	// 落库的 Throttled 计数与 handler 侧判定对得上(重排真的走了限流路径)。
	tasks, err := g2.List(ctx, taskgate.Filter{Type: "llm"})
	if err != nil {
		t.Fatalf("List 失败: %v", err)
	}
	var sumThrottled int
	for _, task := range tasks {
		sumThrottled += task.Throttled
	}
	if int64(sumThrottled) != throttled.Load() {
		t.Fatalf("落库 Throttled 总和(%d)应等于 handler 判定次数(%d)", sumThrottled, throttled.Load())
	}
	t.Logf("档二观测: MaxConcurrency=%d BusyCount=%d Throttled=%d Requests=%d",
		gw2.MaxConcurrency(), gw2.BusyCount(), throttled.Load(), gw2.Requests())
}

// ---------- 用例②:OCR 灌库与断连重试(测试方案 4.2 / spec US1 场景 2) ----------

// TestE2EOCRCrash mock OCR 延迟 200ms、并发>4 断连:
//   - 档一 {Workers:2}:远离断连红线 → 20 个任务全 completed 且网关零断连;
//   - 档二 {Workers:6}:冲破红线(>4 才断,6 并发必然触发)→ 断连走普通重试
//     (MaxRetry 给足),退避后在并发降下来时补完,最终全 completed。
func TestE2EOCRCrash(t *testing.T) {
	const nTasks = 20
	ctx := context.Background()

	// 档一:{Workers:2} 稳稳落在红线内。
	gw := mockgw.New(mockgw.Latency(200*time.Millisecond), mockgw.CrashAfterConcurrency(4))
	defer gw.Close()
	g := newGate(t, taskgate.Config{
		Queues: map[string]taskgate.QueueConfig{"ocr": {Workers: 2}},
	})
	g.Handle("ocr", gatewayHandler(gw.URL(), nil))
	startGate(t, g)
	for i := 0; i < nTasks; i++ {
		if _, err := g.Submit(ctx, "ocr", json.RawMessage(fmt.Sprintf(`{"pdf":%d}`, i))); err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
	}
	waitFor(t, 30*time.Second, func() bool {
		return statusCount(t, g, "ocr", taskgate.StatusCompleted) == nTasks
	}, "档一 20 个任务应全部 completed")
	if got := gw.CrashCount(); got != 0 {
		t.Fatalf("{Workers:2} 档不该触发断连,实际 %d 次", got)
	}
	if got := gw.MaxConcurrency(); got > 2 {
		t.Fatalf("{Workers:2} 档网关观测最大并发应 ≤2,实际 %d", got)
	}
	t.Logf("档一观测: MaxConcurrency=%d CrashCount=%d Requests=%d",
		gw.MaxConcurrency(), gw.CrashCount(), gw.Requests())

	// 档二:{Workers:6} 必然冲破 >4 的断连红线。
	gw2 := mockgw.New(mockgw.Latency(200*time.Millisecond), mockgw.CrashAfterConcurrency(4))
	defer gw2.Close()
	g2 := newGate(t, taskgate.Config{
		Queues: map[string]taskgate.QueueConfig{"ocr": {Workers: 6}},
	})
	g2.Handle("ocr", gatewayHandler(gw2.URL(), nil))
	startGate(t, g2)
	for i := 0; i < nTasks; i++ {
		// MaxRetry 给足:断连走普通重试(指数退避),重试预算必须足够。
		if _, err := g2.Submit(ctx, "ocr", json.RawMessage(fmt.Sprintf(`{"pdf":%d}`, i)),
			taskgate.MaxRetry(10)); err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
	}
	waitFor(t, 60*time.Second, func() bool {
		return statusCount(t, g2, "ocr", taskgate.StatusCompleted) == nTasks
	}, "档二 20 个任务应在断连重试后全部 completed")
	if got := gw2.CrashCount(); got == 0 {
		t.Fatal("档二断连应真实发生过(CrashCount=0 说明没测到目标场景)")
	}
	if got := statusCount(t, g2, "ocr", taskgate.StatusFailed); got != 0 {
		t.Fatalf("档二应零 failed,实际 %d", got)
	}
	t.Logf("档二观测: MaxConcurrency=%d CrashCount=%d Requests=%d",
		gw2.MaxConcurrency(), gw2.CrashCount(), gw2.Requests())
}

// ---------- 用例③:三队列流水线(测试方案 4.3 / spec US1 场景 3) ----------

// TestE2EThreeQueuePipeline ocr → extract → score 三类型三队列各自限流,
// 10 份文档并行灌入(每份三级依赖链)→ 30 个任务全 completed;
// score 的 handler 里 Get(extract 父)把父 Result 拼进自己的结果,逐份断言传递正确。
func TestE2EThreeQueuePipeline(t *testing.T) {
	const nDocs = 10
	ctx := context.Background()

	gw := mockgw.New(mockgw.Latency(20 * time.Millisecond))
	defer gw.Close()
	g := newGate(t, taskgate.Config{
		Queues: map[string]taskgate.QueueConfig{
			"ocr":     {Workers: 2},
			"extract": {Workers: 2, RPS: 50},
			"score":   {Workers: 2},
		},
	})

	// ocr:打网关拿"识别文本",文本里带上文档号。
	g.Handle("ocr", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		ev, err := callGateway(gw.URL(), task.Payload)
		if err != nil || ev.Error != "" {
			return nil, fmt.Errorf("ocr 网关异常: ev=%+v err=%w", ev, err)
		}
		var in struct {
			Doc int `json:"doc"`
		}
		if err := json.Unmarshal(task.Payload, &in); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]string{"text": fmt.Sprintf("doc-%d-text", in.Doc)})
	})
	// extract:读 ocr 父的 Result,打网关,拼上自己的字段。
	g.Handle("extract", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		parent, err := g.Get(ctx, task.DependsOn[0])
		if err != nil {
			return nil, err
		}
		var in struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(parent.Result, &in); err != nil {
			return nil, err
		}
		if _, err := callGateway(gw.URL(), nil); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]string{"keywords": in.Text + "-kw"})
	})
	// score:Get(extract 父)拼 Result——这就是用例要断言的"结果传递"。
	g.Handle("score", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		parent, err := g.Get(ctx, task.DependsOn[0])
		if err != nil {
			return nil, err
		}
		var in struct {
			Keywords string `json:"keywords"`
		}
		if err := json.Unmarshal(parent.Result, &in); err != nil {
			return nil, err
		}
		if _, err := callGateway(gw.URL(), nil); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]string{"scored": in.Keywords + "-scored"})
	})
	startGate(t, g)

	scoreIDs := make(map[string]int, nDocs) // score 任务 ID → 文档号
	for i := 0; i < nDocs; i++ {
		aid, err := g.Submit(ctx, "ocr", json.RawMessage(fmt.Sprintf(`{"doc":%d}`, i)))
		if err != nil {
			t.Fatalf("Submit ocr 失败: %v", err)
		}
		bid, err := g.Submit(ctx, "extract", nil, taskgate.DependsOn(aid))
		if err != nil {
			t.Fatalf("Submit extract 失败: %v", err)
		}
		cid, err := g.Submit(ctx, "score", nil, taskgate.DependsOn(bid))
		if err != nil {
			t.Fatalf("Submit score 失败: %v", err)
		}
		scoreIDs[cid] = i
	}

	// 30 个任务全部 completed(10 ocr + 10 extract + 10 score)。
	waitFor(t, 30*time.Second, func() bool {
		return statusCount(t, g, "ocr", taskgate.StatusCompleted) == nDocs &&
			statusCount(t, g, "extract", taskgate.StatusCompleted) == nDocs &&
			statusCount(t, g, "score", taskgate.StatusCompleted) == nDocs
	}, "三级流水线 30 个任务应全部 completed")

	// 逐份断言三级字段传递:doc-N-text → doc-N-text-kw → doc-N-text-kw-scored。
	for cid, n := range scoreIDs {
		var out struct {
			Scored string `json:"scored"`
		}
		task := mustGet(t, g, cid)
		if err := json.Unmarshal(task.Result, &out); err != nil {
			t.Fatalf("score 结果不是合法 JSON: %s", task.Result)
		}
		if want := fmt.Sprintf("doc-%d-text-kw-scored", n); out.Scored != want {
			t.Fatalf("文档 %d 的传递链断了: 应为 %q,实际 %q", n, want, out.Scored)
		}
	}
}

// ---------- 用例④:流水线中途取消(测试方案 4.4 / spec US1 场景 4) ----------

// TestE2ECancelMidway 一条 ocr→extract→score 流水线:用一个卡死的"垫队列"任务占住
// extract 的唯一 worker,制造"ocr 已完成、extract 被唤醒但还在排队"的窗口,
// 此时 Cancel extract → extract canceled、score 连锁 canceled、ocr 保持 completed。
func TestE2ECancelMidway(t *testing.T) {
	ctx := context.Background()

	gw := mockgw.New()
	defer gw.Close()
	g := newGate(t, taskgate.Config{
		Queues: map[string]taskgate.QueueConfig{
			"ocr": {Workers: 1}, "extract": {Workers: 1}, "score": {Workers: 1},
		},
	})
	release := make(chan struct{})
	g.Handle("ocr", gatewayHandler(gw.URL(), nil))
	g.Handle("extract", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		if string(task.Payload) == `"filler"` {
			// 垫队列任务:卡住唯一 worker,直到测试放行(或被停机打断)。
			select {
			case <-release:
				return []byte(`"filler done"`), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return gatewayHandler(gw.URL(), nil)(ctx, task)
	})
	g.Handle("score", gatewayHandler(gw.URL(), nil))
	startGate(t, g)

	// 先把垫队列任务占死 extract 的 worker。
	fillerID, err := g.Submit(ctx, "extract", json.RawMessage(`"filler"`))
	if err != nil {
		t.Fatalf("Submit 垫队列任务失败: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return mustGet(t, g, fillerID).Status == taskgate.StatusRunning
	}, "垫队列任务应先跑起来占住 extract 的 worker")

	// 提交流水线。
	aid, err := g.Submit(ctx, "ocr", json.RawMessage(`{"doc":0}`))
	if err != nil {
		t.Fatalf("Submit ocr 失败: %v", err)
	}
	bid, err := g.Submit(ctx, "extract", nil, taskgate.DependsOn(aid))
	if err != nil {
		t.Fatalf("Submit extract 失败: %v", err)
	}
	cid, err := g.Submit(ctx, "score", nil, taskgate.DependsOn(bid))
	if err != nil {
		t.Fatalf("Submit score 失败: %v", err)
	}

	// 等到"ocr 完成、extract 被唤醒成 pending 但排在垫队列后面"的窗口。
	waitFor(t, 10*time.Second, func() bool {
		return mustGet(t, g, aid).Status == taskgate.StatusCompleted
	}, "ocr 应先完成")
	waitFor(t, 10*time.Second, func() bool {
		return mustGet(t, g, bid).Status == taskgate.StatusPending
	}, "extract 应被唤醒成 pending(排队中)")

	// 窗口内取消 extract。
	if err := g.Cancel(ctx, bid); err != nil {
		t.Fatalf("Cancel extract 失败: %v", err)
	}
	if got := mustGet(t, g, bid).Status; got != taskgate.StatusCanceled {
		t.Fatalf("extract 应为 canceled,实际 %s", got)
	}
	waitFor(t, 10*time.Second, func() bool {
		return mustGet(t, g, cid).Status == taskgate.StatusCanceled
	}, "score 应被连锁取消")
	if got := mustGet(t, g, aid).Status; got != taskgate.StatusCompleted {
		t.Fatalf("ocr 应保持 completed,实际 %s", got)
	}

	// 放行垫队列任务,收尾干净。
	close(release)
	waitFor(t, 10*time.Second, func() bool {
		return mustGet(t, g, fillerID).Status == taskgate.StatusCompleted
	}, "垫队列任务放行后应正常完成")
}

// ---------- 用例⑤:SSE 藏错误(测试方案 4.5 / spec US1 场景 5) ----------

// TestE2ESSEHiddenError mock LLM 前 2 次请求返回 HTTP 200 + body 里的 busy 错误事件:
// handler 解析 body 判定后返回 ErrThrottled → 任务按重排路径延后再跑,第 3 次成功。
// 断言最终 completed 且 Throttled=2——"HTTP 状态码骗人"场景被完整覆盖。
func TestE2ESSEHiddenError(t *testing.T) {
	ctx := context.Background()

	gw := mockgw.New(mockgw.BusyFirstN(2))
	defer gw.Close()
	var throttled atomic.Int64
	g := newGate(t, taskgate.Config{
		Queues: map[string]taskgate.QueueConfig{"llm": {Workers: 1}},
	})
	g.Handle("llm", gatewayHandler(gw.URL(), &throttled))
	startGate(t, g)

	id, err := g.Submit(ctx, "llm", json.RawMessage(`{"doc":0}`))
	if err != nil {
		t.Fatalf("Submit 失败: %v", err)
	}
	wctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if _, err := g.Wait(wctx, id); err != nil {
		t.Fatalf("Wait 失败(重排后应成功): %v", err)
	}

	task := mustGet(t, g, id)
	if task.Status != taskgate.StatusCompleted {
		t.Fatalf("最终状态应为 completed,实际 %s", task.Status)
	}
	if task.Throttled != 2 {
		t.Fatalf("前 2 次 busy 应各记一次 Throttled,应为 2,实际 %d", task.Throttled)
	}
	if task.Attempts != 0 {
		t.Fatalf("busy 走限流重排不占 Attempts,应为 0,实际 %d", task.Attempts)
	}
	if throttled.Load() != 2 || gw.BusyCount() != 2 || gw.Requests() != 3 {
		t.Fatalf("观测不对: handler 判定 %d 次、网关 busy %d 次、总请求 %d(应为 2/2/3)",
			throttled.Load(), gw.BusyCount(), gw.Requests())
	}
}

// TestE2ECronReplay(spec 005 US1)cron 配方端到端:确定性业务键防双触发;
// 网关故障期任务 failed;从拒绝错误里解构链尾 → Replay → 网关恢复后新执行跑完;
// 历史链完整可溯源。这是 README cron 配方"失败后死锁"被修复后的完整走法。
func TestE2ECronReplay(t *testing.T) {
	ctx := context.Background()
	gw := mockgw.New()
	defer gw.Close()
	// 拿一个必然连接失败的地址模拟"网关故障期":起一个 mockgw 立刻关掉,端口不再监听。
	dead := mockgw.New()
	deadURL := dead.URL()
	dead.Close()

	var gwURL atomic.Value
	gwURL.Store(deadURL)

	g := newGate(t, taskgate.Config{
		Queues: map[string]taskgate.QueueConfig{"report": {Workers: 1}},
	})
	g.Handle("report", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		ev, err := callGateway(gwURL.Load().(string), task.Payload)
		if err != nil {
			return nil, err // 断连走普通重试路径;MaxRetry(0) 下一次失败即 failed
		}
		if !ev.OK {
			return nil, taskgate.ErrThrottled{RetryAfter: 100 * time.Millisecond}
		}
		return ev.Echo, nil
	})
	startGate(t, g)

	// cron 触发 1:网关不可达,MaxRetry(0) → 一次失败即 failed,业务键被"占住"。
	const key = "daily-report:2026-07-16"
	e1, err := g.Submit(ctx, "report", json.RawMessage(`{"day":"2026-07-16"}`),
		taskgate.WithBusinessKey(key), taskgate.MaxRetry(0))
	if err != nil {
		t.Fatalf("cron 首次 Submit 失败: %v", err)
	}
	if _, err := g.Wait(ctx, e1); err == nil {
		t.Fatal("网关故障期 E1 应以 failed 告终")
	}

	// cron 触发 2(双触发/重试):同键被拒,且错误里直接带出链尾 failed——不用再查询。
	_, err = g.Submit(ctx, "report", json.RawMessage(`{"day":"2026-07-16"}`),
		taskgate.WithBusinessKey(key), taskgate.MaxRetry(0))
	var te *taskgate.TaskExistsError
	if !errors.As(err, &te) || te.ExecutionID != e1 || te.Status != taskgate.StatusFailed {
		t.Fatalf("双触发应被拒且携带链尾 failed,得到 err=%v te=%+v", err, te)
	}

	// 网关恢复,按链尾 Replay:新执行进入正常调度并跑完。
	gwURL.Store(gw.URL())
	e2, err := g.Replay(ctx, te.ExecutionID)
	if err != nil {
		t.Fatalf("Replay 失败: %v", err)
	}
	out, err := g.Wait(ctx, e2)
	if err != nil {
		t.Fatalf("重放执行应成功跑完: %v", err)
	}
	if string(out) != `{"day":"2026-07-16"}` {
		t.Fatalf("重放执行的结果应是网关 echo 的原 Payload,得到 %s", out)
	}

	// 溯源:历史链 [E1, E2],E2 指回 E1,E1 的失败现场原封不动。
	hist, err := g.History(ctx, key)
	if err != nil || len(hist) != 2 || hist[0].ID != e1 || hist[1].ID != e2 {
		t.Fatalf("History 应为 [E1 E2],得到 %d 条 err=%v", len(hist), err)
	}
	if hist[1].ReplayOf != e1 || hist[0].Status != taskgate.StatusFailed {
		t.Fatalf("链字段不对: E2.ReplayOf=%q E1.Status=%s", hist[1].ReplayOf, hist[0].Status)
	}
}

// TestE2EQuotaCombined(spec 006 SC-005)三维度正交:{Workers:2, RPS:8, Quota:10/2s}
// 打 mockgw——并发不超 2(网关观测)、窗口累计不超 10(第 11 个在下窗才跑)。
func TestE2EQuotaCombined(t *testing.T) {
	const (
		period = 2 * time.Second
		limit  = 10
		total  = 15
	)
	ctx := context.Background()
	gw := mockgw.New(mockgw.Latency(5 * time.Millisecond))
	defer gw.Close()
	g := newGate(t, taskgate.Config{
		Queues: map[string]taskgate.QueueConfig{
			"llm": {Workers: 2, RPS: 8, QuotaLimit: limit, QuotaPeriod: taskgate.Duration(period)},
		},
	})
	g.Handle("llm", gatewayHandler(gw.URL(), nil))

	// 对齐窗口边界再开闸,让"每窗 ≤10"的断言不被边界横插。
	now := time.Now()
	time.Sleep(time.Until(now.Truncate(period).Add(period)) + 100*time.Millisecond)
	windowEnd := time.Now().Truncate(period).Add(period)

	for i := 0; i < total; i++ {
		if _, err := g.Submit(ctx, "llm", json.RawMessage(fmt.Sprintf(`{"i":%d}`, i))); err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
	}
	startGate(t, g)

	// 窗口 1 结束前:恰好 limit 个完成(RPS=8、latency 5ms,10 个在 ~1.3s 内跑完)。
	waitFor(t, time.Until(windowEnd)-200*time.Millisecond, func() bool {
		return statusCount(t, g, "llm", taskgate.StatusCompleted) == limit
	}, "窗口 1 应恰好放行 limit 个")
	time.Sleep(time.Until(windowEnd) - 50*time.Millisecond)
	if n := statusCount(t, g, "llm", taskgate.StatusCompleted); n != limit {
		t.Fatalf("窗口 1 内累计完成应恰好 %d,实际 %d(第 %d 个必须等下窗)", limit, n, limit+1)
	}

	// 窗口 2:剩余 5 个跑完;Workers 维度全程成立。
	waitFor(t, period+2*time.Second, func() bool {
		return statusCount(t, g, "llm", taskgate.StatusCompleted) == total
	}, "窗口 2 应跑完剩余任务")
	if got := gw.MaxConcurrency(); got > 2 {
		t.Fatalf("Workers=2 维度被打破:网关观测最大并发 %d", got)
	}
	if n := statusCount(t, g, "llm", taskgate.StatusFailed); n != 0 {
		t.Fatalf("配额路径不得产生 failed,实际 %d", n)
	}
}
