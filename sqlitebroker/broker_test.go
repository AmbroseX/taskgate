package sqlitebroker_test

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/AmbroseX/taskgate/brokertest"
	"github.com/AmbroseX/taskgate/internal/fakeclock"
	"github.com/AmbroseX/taskgate/sqlitebroker"
)

// TestBrokerContract 一行接入统一契约套件:sqlite 后端必须过全部 18 条契约。
// 每条用例一个独立的临时库文件,互不串数据。
func TestBrokerContract(t *testing.T) {
	brokertest.Run(t, func(t *testing.T, opts taskgate.BrokerOptions) taskgate.Broker {
		b, err := sqlitebroker.Open(filepath.Join(t.TempDir(), "tasks.db"))
		if err != nil {
			t.Fatalf("sqlitebroker Open 失败: %v", err)
		}
		if err := b.Init(opts); err != nil {
			t.Fatalf("sqlitebroker Init 失败: %v", err)
		}
		return b
	})
}

// TestUseBeforeInit 未 Init 就调用任何方法必须返回错误,而不是 nil 指针 panic。
func TestUseBeforeInit(t *testing.T) {
	b, err := sqlitebroker.Open(filepath.Join(t.TempDir(), "noinit.db"))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	ctx := context.Background()
	checks := map[string]error{
		"Enqueue":        b.Enqueue(ctx, &taskgate.Task{Type: "x", Queue: "q"}),
		"Ack":            b.Ack(ctx, "id", "tok", nil),
		"Fail":           b.Fail(ctx, "id", "tok", "x", taskgate.FailBusiness, time.Time{}),
		"Cancel":         b.Cancel(ctx, "id"),
		"FinishCanceled": b.FinishCanceled(ctx, "id", "tok"),
		"Requeue":        b.Requeue(ctx, "id", "tok"),
		"Heartbeat":      b.Heartbeat(ctx, "id", "tok"),
	}
	for op, err := range checks {
		if err == nil {
			t.Fatalf("未 Init 调 %s 应返回错误,实际 nil", op)
		}
	}
	if _, err := b.ReapExpired(ctx); err == nil {
		t.Fatal("未 Init 调 ReapExpired 应返回错误,实际 nil")
	}
}

// TestQuotaCapability 周期配额能力套件(spec 006):文件介质,测试时间缝挂到 fakeclock,
// 不真 sleep;生产路径(NULL → strftime)由多进程原型与真机冒烟覆盖。
func TestQuotaCapability(t *testing.T) {
	brokertest.RunQuota(t, func(t *testing.T, opts taskgate.BrokerOptions) (taskgate.Broker, func(time.Duration)) {
		b, err := sqlitebroker.Open(filepath.Join(t.TempDir(), "quota.db"))
		if err != nil {
			t.Fatalf("Open 失败: %v", err)
		}
		if err := b.Init(opts); err != nil {
			t.Fatalf("Init 失败: %v", err)
		}
		clk := opts.Clock.(*fakeclock.Clock)
		sqlitebroker.SetTestQuotaNow(func() int64 { return clk.Now().Unix() })
		t.Cleanup(func() { sqlitebroker.SetTestQuotaNow(nil) })
		return b, clk.Advance
	})
}

// TestQuotaSharedMedium(spec 006 T014,SC-001/002)双 Gate 共享同一 sqlite 库文件、
// 同 quota key:每窗两实例合计恰好 N;介质时间用 SetTestQuotaNow 钩子驱动,不真等窗口。
// 放在本包(而不是根包 L3)是因为钩子经 export_test.go 暴露,只有本包测试可见。
func TestQuotaSharedMedium(t *testing.T) {
	const limit = 5
	path := filepath.Join(t.TempDir(), "shared.db")
	var mediumNow atomic.Int64
	mediumNow.Store(1_700_000_000) // 固定起点,对齐后落在稳定窗口
	sqlitebroker.SetTestQuotaNow(mediumNow.Load)
	t.Cleanup(func() { sqlitebroker.SetTestQuotaNow(nil) })

	var completed atomic.Int64
	newG := func() *taskgate.Gate {
		b, err := sqlitebroker.Open(path)
		if err != nil {
			t.Fatalf("Open 失败: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
		g, err := taskgate.New(taskgate.Config{
			Broker: b,
			Queues: map[string]taskgate.QueueConfig{
				"gw": {Workers: 2, QuotaLimit: limit, QuotaPeriod: taskgate.Duration(time.Hour), QuotaKey: "shared-gw"},
			},
		})
		if err != nil {
			t.Fatalf("New 失败: %v", err)
		}
		g.Handle("gw", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			completed.Add(1)
			return []byte(`"ok"`), nil
		})
		return g
	}
	g1, g2 := newG(), newG()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done1, done2 := make(chan struct{}), make(chan struct{})
	go func() { defer close(done1); _ = g1.Run(ctx) }()
	go func() { defer close(done2); _ = g2.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done1; <-done2 })

	for i := 0; i < 12; i++ {
		if _, err := g1.Submit(ctx, "gw", nil); err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
	}
	waitCount := func(want int64, msg string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if completed.Load() == want {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("%s: handler 启动数=%d 期望 %d", msg, completed.Load(), want)
	}

	// 窗口 1:两实例合计恰好 limit 次 handler 启动,且稳定不再多放(硬配额)。
	waitCount(limit, "窗口 1")
	time.Sleep(400 * time.Millisecond)
	if n := completed.Load(); n != limit {
		t.Fatalf("窗口 1 超发: %d > %d", n, limit)
	}

	// 推介质时间一个窗口:额度恢复;再推一个:跑完剩余。
	mediumNow.Add(3600)
	waitCount(2*limit, "窗口 2")
	mediumNow.Add(3600)
	waitCount(12, "窗口 3")
}
