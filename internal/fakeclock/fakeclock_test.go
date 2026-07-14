package fakeclock

import (
	"context"
	"sync"
	"testing"
	"time"
)

var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// TestAdvanceWakesMultipleWaiters 一次 Advance 必须唤醒所有到点的 waiter,
// 没到点的一个都不能动。
func TestAdvanceWakesMultipleWaiters(t *testing.T) {
	c := New(start)
	w1 := c.After(time.Second)
	w2 := c.After(2 * time.Second)
	w3 := c.After(10 * time.Second) // 不该被唤醒

	c.Advance(3 * time.Second)
	for i, ch := range []<-chan time.Time{w1, w2} {
		select {
		case got := <-ch:
			if !got.Equal(start.Add(3 * time.Second)) {
				t.Fatalf("waiter %d 收到的时刻应为 Advance 后的 now=%v,实际 %v", i+1, start.Add(3*time.Second), got)
			}
		default:
			t.Fatalf("waiter %d 已到点,Advance 后必须立即可收", i+1)
		}
	}
	select {
	case <-w3:
		t.Fatal("10s 的 waiter 没到点,不该被唤醒")
	default:
	}
}

// TestAdvanceExactBoundary 恰好推进到到点时刻(不多一纳秒)也算到点。
func TestAdvanceExactBoundary(t *testing.T) {
	c := New(start)
	w := c.After(time.Second)
	c.Advance(time.Second) // 压线
	select {
	case <-w:
	default:
		t.Fatal("Advance 恰好到点(at == now)必须唤醒 waiter")
	}

	// d<=0 立即就绪,不用 Advance。
	select {
	case <-c.After(0):
	default:
		t.Fatal("After(0) 应立即就绪")
	}
}

// TestTickerCatchUp 一次 Advance 跨过多个周期:channel 容量 1,只保留最早那格
// (收不动就丢,与 time.Ticker 一致);内部 next 必须补齐到位,后续周期不漂移。
func TestTickerCatchUp(t *testing.T) {
	c := New(start)
	if n := c.TickerCount(); n != 0 {
		t.Fatalf("新时钟不该有 ticker,实际 %d", n)
	}
	tk := c.NewTicker(time.Second)
	defer tk.Stop()
	if n := c.TickerCount(); n != 1 {
		t.Fatalf("注册一个 ticker 后 TickerCount 应为 1,实际 %d", n)
	}

	c.Advance(3 * time.Second) // 跨 3 个周期
	select {
	case got := <-tk.C():
		if !got.Equal(start.Add(time.Second)) {
			t.Fatalf("第一格滴答应为 %v,实际 %v", start.Add(time.Second), got)
		}
	default:
		t.Fatal("跨周期 Advance 后至少要收到一格滴答")
	}
	select {
	case <-tk.C():
		t.Fatal("channel 容量 1,跨周期只保留一格,多余的应被丢弃")
	default:
	}

	// 内部周期已补齐到 start+4s:再推 1s 应收到第 4 格。
	c.Advance(time.Second)
	select {
	case got := <-tk.C():
		if !got.Equal(start.Add(4 * time.Second)) {
			t.Fatalf("补齐后下一格应为 %v,实际 %v", start.Add(4*time.Second), got)
		}
	default:
		t.Fatal("补齐后再推一个周期应收到滴答")
	}

	// Stop 后不再收到任何 tick。
	tk.Stop()
	c.Advance(5 * time.Second)
	select {
	case <-tk.C():
		t.Fatal("Stop 之后不该再收到滴答")
	default:
	}
}

// TestSleepCtxCancel Sleep 在 ctx 取消时立即返回 ctx.Err()。
func TestSleepCtxCancel(t *testing.T) {
	c := New(start)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Sleep(ctx, time.Hour) }()
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Sleep 应返回 context.Canceled,实际 %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ctx 取消后 Sleep 没有退出")
	}
}

// TestConcurrentAdvanceAfter 并发 Advance/After/Now 不许有数据竞争(配 -race 跑)。
func TestConcurrentAdvanceAfter(t *testing.T) {
	c := New(start)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.Advance(time.Millisecond)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = c.After(time.Duration(j%5) * time.Millisecond)
				_ = c.Now()
			}
		}()
	}
	wg.Wait()
	// 8 个 goroutine 各推 100ms,最终时刻必须精确等于起点+800ms(Advance 原子性)。
	if got, want := c.Now(), start.Add(800*time.Millisecond); !got.Equal(want) {
		t.Fatalf("并发 Advance 后 Now 应为 %v,实际 %v", want, got)
	}
}
