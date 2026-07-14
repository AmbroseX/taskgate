package taskgate

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestLimiterRPSPrecision RPS=10 → 1 秒窗口放行 10±1 个令牌。
// 用真时钟短窗口:Burst=1 且先烧掉初始令牌,窗口内的放行数就只由补充速率决定,
// 理论值 10,容差 ±1 吸收调度抖动。
func TestLimiterRPSPrecision(t *testing.T) {
	lim := newLocalLimiter(1, 10, 1)

	// 烧掉桶里的初始突发令牌,让计数从"匀速补充"开始。
	if err := lim.WaitToken(context.Background()); err != nil {
		t.Fatalf("烧初始令牌失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	count := 0
	for lim.WaitToken(ctx) == nil {
		count++
	}
	if count < 9 || count > 11 {
		t.Fatalf("RPS=10 一秒应放行 10±1 个,实际 %d", count)
	}
}

// TestLimiterBurst Burst=5 时前 5 个立即放行,第 6 个要等补充。
func TestLimiterBurst(t *testing.T) {
	lim := newLocalLimiter(1, 1, 5) // 每秒才补 1 个,突发额度 5

	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := lim.WaitToken(context.Background()); err != nil {
			t.Fatalf("第 %d 个突发令牌失败: %v", i+1, err)
		}
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("5 个突发令牌应立即放行,却花了 %v", elapsed)
	}

	// 第 6 个:桶空了,RPS=1 意味着要等约 1s,100ms 内必然拿不到。
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := lim.WaitToken(ctx); err == nil {
		t.Fatal("桶空后第 6 个令牌不应在 100ms 内放行")
	}
}

// TestLimiterDefaultBurst Burst 缺省规则:0 → max(1, int(RPS))。
func TestLimiterDefaultBurst(t *testing.T) {
	if got := newLocalLimiter(1, 10, 0).tb.Burst(); got != 10 {
		t.Fatalf("RPS=10 缺省 Burst 应为 10,得到 %d", got)
	}
	if got := newLocalLimiter(1, 0.5, 0).tb.Burst(); got != 1 {
		t.Fatalf("RPS=0.5 缺省 Burst 应为 1,得到 %d", got)
	}
	if got := newLocalLimiter(1, 10, 3).tb.Burst(); got != 3 {
		t.Fatalf("显式 Burst=3 应生效,得到 %d", got)
	}
}

// TestLimiterNoRPS RPS=0 → 不限速,令牌永远立即放行。
func TestLimiterNoRPS(t *testing.T) {
	lim := newLocalLimiter(1, 0, 0)
	if lim.tb != nil {
		t.Fatal("RPS=0 时不应创建令牌桶")
	}
	start := time.Now()
	for i := 0; i < 1000; i++ {
		if err := lim.WaitToken(context.Background()); err != nil {
			t.Fatalf("不限速时 WaitToken 不应失败: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("不限速时 1000 次放行应瞬间完成,却花了 %v", elapsed)
	}
}

// TestLimiterWorkerSlots Workers=2:前两个槽即拿即得,第 3 个必须等归还。
func TestLimiterWorkerSlots(t *testing.T) {
	lim := newLocalLimiter(2, 0, 0)
	ctx := context.Background()

	if err := lim.AcquireSlot(ctx); err != nil {
		t.Fatalf("第 1 个槽失败: %v", err)
	}
	if err := lim.AcquireSlot(ctx); err != nil {
		t.Fatalf("第 2 个槽失败: %v", err)
	}

	// 第 3 个:槽满,50ms 内拿不到,收到 ctx 超时。
	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := lim.AcquireSlot(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("槽满时第 3 个 acquire 应超时,得到: %v", err)
	}

	// 归还一个后,第 3 个立刻能拿到。
	lim.ReleaseSlot()
	okCtx, cancel2 := context.WithTimeout(ctx, 1*time.Second)
	defer cancel2()
	if err := lim.AcquireSlot(okCtx); err != nil {
		t.Fatalf("归还后第 3 个槽应立即可得: %v", err)
	}
}
