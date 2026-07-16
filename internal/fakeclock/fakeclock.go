// Package fakeclock 是测试专用的假时钟:时间只在调 Advance 时前进,
// 测试不真 sleep,时序完全确定(宪法第 V 条)。
package fakeclock

import (
	"context"
	"sync"
	"time"

	"github.com/AmbroseX/taskgate"
)

// Clock 实现 taskgate.Clock。并发安全,所有等待者靠 Advance 手动唤醒。
type Clock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
	tickers []*fakeTicker
}

// waiter 一次性等待者(After/Sleep),到点发一次就完事。
type waiter struct {
	at time.Time
	ch chan time.Time
}

// New 从 start 时刻起步。
func New(start time.Time) *Clock {
	return &Clock{now: start}
}

// Now 当前假时刻。
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After 注册一个到点唤醒;d<=0 立即就绪。channel 带 1 缓冲,Advance 发的时候不会卡住。
func (c *Clock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, &waiter{at: c.now.Add(d), ch: ch})
	return ch
}

// Sleep 等到假时间推进过 d,或 ctx 先取消。
func (c *Clock) Sleep(ctx context.Context, d time.Duration) error {
	done := c.After(d)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// NewTicker 假滴答:每次 Advance 跨过一个周期就发一格(channel 满了就丢,和真 Ticker 一样)。
func (c *Clock) NewTicker(d time.Duration) taskgate.Ticker {
	if d <= 0 {
		panic("fakeclock: ticker interval must be > 0")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tk := &fakeTicker{
		clock:    c,
		ch:       make(chan time.Time, 1),
		interval: d,
		next:     c.now.Add(d),
	}
	c.tickers = append(c.tickers, tk)
	return tk
}

// TickerCount 当前注册的 ticker 数(含已 Stop 未清理的)。给测试做同步用:
// 被测代码的 ticker 常由后台 goroutine 异步创建,先等它挂上再 Advance,
// 否则拨钟发生在注册之前,那一格滴答就永远丢了(假时钟只在 Advance 时发滴答)。
func (c *Clock) TickerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.tickers)
}

// Advance 把时间往前拨 d,唤醒所有到点的等待者,给到点的 ticker 补上滴答。
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)

	// 到点的 waiter 全部唤醒;channel 有 1 缓冲,锁内发送不会阻塞。
	remain := c.waiters[:0]
	for _, w := range c.waiters {
		if !w.at.After(c.now) {
			w.ch <- c.now
		} else {
			remain = append(remain, w)
		}
	}
	c.waiters = remain

	// ticker:补齐跨过的每个周期;收不动就丢,跟 time.Ticker 行为一致。
	alive := c.tickers[:0]
	for _, tk := range c.tickers {
		if tk.stopped {
			continue
		}
		for !tk.next.After(c.now) {
			select {
			case tk.ch <- tk.next:
			default:
			}
			tk.next = tk.next.Add(tk.interval)
		}
		alive = append(alive, tk)
	}
	c.tickers = alive
}

// fakeTicker 假滴答器,Stop 后不再收到任何 tick。
type fakeTicker struct {
	clock    *Clock
	ch       chan time.Time
	interval time.Duration
	next     time.Time
	stopped  bool
}

func (t *fakeTicker) C() <-chan time.Time { return t.ch }

func (t *fakeTicker) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.stopped = true
}
