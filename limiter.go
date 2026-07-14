package taskgate

import (
	"context"

	"golang.org/x/time/rate"
)

// limiter 单个队列的限流器,两层独立生效:
//   - sem:并发槽,带缓冲 channel,容量 = Workers,限"同时在跑多少个";
//   - tb:RPS 令牌桶(x/time/rate),限"每秒新启动多少个";RPS=0 时为 nil,不限速。
//
// 两层怎么配合用见 scheduler.claimLoop:先占槽、再等令牌。
type limiter struct {
	sem chan struct{}
	tb  *rate.Limiter
}

// newLimiter 构造:workers 是并发槽数(调用方保证 ≥1);
// rps=0 → 不限速;burst≤0 时缺省取 max(1, int(rps))。
func newLimiter(workers int, rps float64, burst int) *limiter {
	l := &limiter{sem: make(chan struct{}, workers)}
	if rps > 0 {
		if burst <= 0 {
			burst = int(rps)
			if burst < 1 {
				burst = 1 // rps 是 0.5 这类小数时至少给 1 个突发额度
			}
		}
		l.tb = rate.NewLimiter(rate.Limit(rps), burst)
	}
	return l
}

// acquireSlot 占一个并发槽,占不到就阻塞;ctx 取消返回 ctx.Err()。
func (l *limiter) acquireSlot(ctx context.Context) error {
	select {
	case l.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseSlot 归还并发槽。必须和 acquireSlot 一一配对。
func (l *limiter) releaseSlot() {
	<-l.sem
}

// waitToken 等一个 RPS 令牌;不限速时立即放行;ctx 取消返回其错误。
func (l *limiter) waitToken(ctx context.Context) error {
	if l.tb == nil {
		return nil
	}
	return l.tb.Wait(ctx)
}
