package brokertest

// RunQuota 周期配额能力(spec 006)的统一验收套件,独立于 Broker 的 22 条契约:
// 配额是可选能力接口(QuotaProvider),不是 Broker 方法,分层见调研方案 5.6。
// 五后端跑同一套 Q1~Q5;介质时间由各后端测试文件提供的 advance 回调控制
// (memory 推 fakeclock、sqlite/pg/mysql 推包级测试钩子、redis 推 miniredis.SetTime),
// 套件本身不真 sleep、不知道介质细节。

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/AmbroseX/taskgate/internal/fakeclock"
)

// QuotaFactory 由各后端测试文件提供:构造一个**已 Init**的空 broker(必须实现
// QuotaProvider),并返回"把介质时间往前推 d"的回调。opts.Clock 已注入 fakeclock,
// memory 后端的 advance 就是推它;其他后端推各自的介质时间缝,但**必须同时推
// opts.Clock**(调度侧退避走它,两个钟不许漂移)。
type QuotaFactory func(t *testing.T, opts taskgate.BrokerOptions) (b taskgate.Broker, advance func(time.Duration))

// quotaPeriod 套件统一窗口时长;quotaLimit 统一窗口额度。
const (
	quotaPeriod = 10 * time.Second
	quotaLimit  = 3
)

// RunQuota 对 factory 构造的后端跑全部配额用例。
func RunQuota(t *testing.T, factory QuotaFactory) {
	cases := []struct {
		name string
		run  func(t *testing.T, h *quotaHarness)
	}{
		{"QuotaHardLimit", caseQuotaHardLimit},
		{"QuotaWindowRotate", caseQuotaWindowRotate},
		{"QuotaStaleRelease", caseQuotaStaleRelease},
		{"QuotaKeyIsolation", caseQuotaKeyIsolation},
		{"QuotaConcurrentRace", caseQuotaConcurrentRace},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clk := fakeclock.New(baseTime)
			opts := taskgate.BrokerOptions{
				DefaultLeaseTTL: 60 * time.Second,
				LeaseLostMax:    3,
				ThrottledMax:    100,
				Clock:           clk,
			}
			b, advance := factory(t, opts)
			if b == nil {
				t.Fatal("factory 返回了 nil broker")
			}
			t.Cleanup(func() { _ = b.Close() })
			qp, ok := b.(taskgate.QuotaProvider)
			if !ok {
				t.Fatalf("%T 必须实现 QuotaProvider 才能过配额套件", b)
			}
			h := &quotaHarness{t: t, qp: qp, advance: advance}
			c.run(t, h)
		})
	}
}

// quotaHarness 一条用例的运行环境。
type quotaHarness struct {
	t       *testing.T
	qp      taskgate.QuotaProvider
	advance func(time.Duration)
}

// gate 构造一个配额闸(默认套件参数,key 可指定)。
func (h *quotaHarness) gate(key string) taskgate.QuotaGate {
	h.t.Helper()
	g, err := h.qp.QueueQuota("q", taskgate.QueueConfig{
		Workers:     1,
		QuotaLimit:  quotaLimit,
		QuotaPeriod: taskgate.Duration(quotaPeriod),
		QuotaKey:    key,
	})
	if err != nil {
		h.t.Fatalf("QueueQuota 构造失败: %v", err)
	}
	return g
}

// mustReserve 预留并断言成功,返回预留。
func mustReserve(t *testing.T, g taskgate.QuotaGate) *taskgate.QuotaReservation {
	t.Helper()
	r, err := g.Reserve(context.Background())
	if err != nil {
		t.Fatalf("Reserve 介质故障: %v", err)
	}
	if r == nil {
		t.Fatal("Reserve 应成功,却返回耗尽态")
	}
	return r
}

// mustExhausted 预留并断言耗尽态 (nil, nil)。
func mustExhausted(t *testing.T, g taskgate.QuotaGate) {
	t.Helper()
	r, err := g.Reserve(context.Background())
	if err != nil {
		t.Fatalf("耗尽应是 (nil, nil) 非错误,却报错: %v", err)
	}
	if r != nil {
		t.Fatalf("额度已尽仍预留成功(win=%d):硬配额被打破", r.Window)
	}
}

// Q1 硬上限:N 次成功后必须耗尽;Release 一份后恰好能再预留一份,再多不行。
func caseQuotaHardLimit(t *testing.T, h *quotaHarness) {
	g := h.gate("k1")
	var first *taskgate.QuotaReservation
	for i := 0; i < quotaLimit; i++ {
		r := mustReserve(t, g)
		if first == nil {
			first = r
		}
		if r.Window != first.Window {
			t.Fatalf("同窗口内预留的窗口起点应一致: %d != %d", r.Window, first.Window)
		}
	}
	mustExhausted(t, g)

	// 退还一份 → 恰好能再预留一份;再多仍耗尽。
	if err := g.Release(context.Background(), first); err != nil {
		t.Fatalf("Release 意外失败: %v", err)
	}
	mustReserve(t, g)
	mustExhausted(t, g)
}

// Q2 窗口轮换:耗尽后推介质时间一个周期 → 额度恢复满 N;窗口起点对齐 period。
func caseQuotaWindowRotate(t *testing.T, h *quotaHarness) {
	g := h.gate("k2")
	var win1 int64
	for i := 0; i < quotaLimit; i++ {
		win1 = mustReserve(t, g).Window
	}
	mustExhausted(t, g)
	if periodSec := int64(quotaPeriod / time.Second); win1%periodSec != 0 {
		t.Fatalf("窗口起点 %d 应对齐 period(%ds 的整数倍)", win1, periodSec)
	}

	h.advance(quotaPeriod)
	var win2 int64
	for i := 0; i < quotaLimit; i++ {
		win2 = mustReserve(t, g).Window
	}
	mustExhausted(t, g)
	if win2 <= win1 {
		t.Fatalf("轮换后窗口起点应前进: win1=%d win2=%d", win1, win2)
	}
}

// Q3 旧窗口退还落空无害:窗口 1 的预留在窗口 2 退还,不影响窗口 2 的额度。
func caseQuotaStaleRelease(t *testing.T, h *quotaHarness) {
	g := h.gate("k3")
	old := mustReserve(t, g)

	h.advance(quotaPeriod)
	for i := 0; i < quotaLimit; i++ {
		mustReserve(t, g)
	}
	mustExhausted(t, g)

	// 退还旧窗口的预留:落空无害,新窗口额度不得因此多出一份。
	if err := g.Release(context.Background(), old); err != nil {
		t.Fatalf("旧窗口退还应落空无害(nil),却报错: %v", err)
	}
	mustExhausted(t, g)
}

// Q4 键隔离与键共享:不同 key 各自计数;同 key 的两个闸实例共享介质计数。
func caseQuotaKeyIsolation(t *testing.T, h *quotaHarness) {
	a, b := h.gate("ka"), h.gate("kb")
	for i := 0; i < quotaLimit; i++ {
		mustReserve(t, a)
	}
	mustExhausted(t, a)
	mustReserve(t, b) // kb 不受 ka 耗尽影响

	// 同 key 两个实例:介质计数共享,合计不超 N。
	s1, s2 := h.gate("shared"), h.gate("shared")
	mustReserve(t, s1)
	mustReserve(t, s2)
	mustReserve(t, s1)
	mustExhausted(t, s2)
	mustExhausted(t, s1)
}

// Q5 并发竞态:M(>N)并发 Reserve 恰好 N 个成功,其余全是耗尽态,-race 下验。
func caseQuotaConcurrentRace(t *testing.T, h *quotaHarness) {
	g := h.gate("k5")
	const m = 24
	results := make(chan *taskgate.QuotaReservation, m)
	var wg sync.WaitGroup
	for i := 0; i < m; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := g.Reserve(context.Background())
			if err != nil {
				t.Errorf("并发 Reserve 介质故障: %v", err)
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)
	got := 0
	for r := range results {
		if r != nil {
			got++
		}
	}
	if got != quotaLimit {
		t.Fatalf("%d 并发预留应恰好 %d 个成功,实际 %d(硬配额被打破或少放)", m, quotaLimit, got)
	}
}
