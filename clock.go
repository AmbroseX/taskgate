package taskgate

import (
	"context"
	"time"
)

// Clock 可注入的时钟。租约、退避、限流全部通过它拿时间,
// 这样测试里用 fakeclock 手动推进,不用真 sleep(宪法第 V 条)。
type Clock interface {
	// Now 当前时刻。
	Now() time.Time
	// After 到点后往返回的 channel 发一次当前时刻。
	After(d time.Duration) <-chan time.Time
	// Sleep 睡 d,ctx 先取消就提前返回 ctx.Err()。
	Sleep(ctx context.Context, d time.Duration) error
	// NewTicker 周期滴答,给 reaper/心跳循环用。
	NewTicker(d time.Duration) Ticker
}

// Ticker 抽出接口是为了 fakeclock 能提供假的滴答。
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// RealClock 系统真时钟。BrokerOptions.Clock 传 nil 时后端应退回到它。
func RealClock() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (realClock) NewTicker(d time.Duration) Ticker {
	return &realTicker{t: time.NewTicker(d)}
}

type realTicker struct{ t *time.Ticker }

func (rt *realTicker) C() <-chan time.Time { return rt.t.C }

func (rt *realTicker) Stop() { rt.t.Stop() }
