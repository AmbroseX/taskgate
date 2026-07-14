package taskgate

import (
	"context"

	"golang.org/x/time/rate"
)

// QueueLimiter 单个队列的限流器抽象,两层独立生效:
//   - 并发槽(AcquireSlot/ReleaseSlot):限"同时在跑多少个";
//   - RPS 令牌(WaitToken):限"每秒新启动多少个"。
//
// scheduler 只依赖这个接口,不关心限流器是进程内的还是跨进程共享的:
// 后端实现了 LimiterProvider 就用后端给的,否则用进程内的 localLimiter。
// 两层怎么配合用见 scheduler.claimLoop:先占槽、再等令牌。
type QueueLimiter interface {
	// AcquireSlot 占一个并发槽,占不到就阻塞;ctx 取消返回 ctx.Err()。
	AcquireSlot(ctx context.Context) error
	// ReleaseSlot 归还并发槽。必须和 AcquireSlot 一一配对。
	ReleaseSlot()
	// WaitToken 等一个 RPS 令牌;不限速时立即放行;ctx 取消返回其错误。
	WaitToken(ctx context.Context) error
}

// localLimiter QueueLimiter 的进程内实现(M1 的默认限流器):
//   - sem:并发槽,带缓冲 channel,容量 = Workers;
//   - tb:RPS 令牌桶(x/time/rate);RPS=0 时为 nil,不限速。
type localLimiter struct {
	sem chan struct{}
	tb  *rate.Limiter
}

// 编译期断言:localLimiter 必须实现 QueueLimiter,接口改签名时这里先报错。
var _ QueueLimiter = (*localLimiter)(nil)

// newLocalLimiter 构造进程内限流器:workers 是并发槽数(调用方保证 ≥1);
// rps=0 → 不限速;burst≤0 时缺省取 max(1, int(rps))。
func newLocalLimiter(workers int, rps float64, burst int) *localLimiter {
	l := &localLimiter{sem: make(chan struct{}, workers)}
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

// AcquireSlot 占一个并发槽,占不到就阻塞;ctx 取消返回 ctx.Err()。
func (l *localLimiter) AcquireSlot(ctx context.Context) error {
	select {
	case l.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ReleaseSlot 归还并发槽。必须和 AcquireSlot 一一配对。
func (l *localLimiter) ReleaseSlot() {
	<-l.sem
}

// WaitToken 等一个 RPS 令牌;不限速时立即放行;ctx 取消返回其错误。
func (l *localLimiter) WaitToken(ctx context.Context) error {
	if l.tb == nil {
		return nil
	}
	return l.tb.Wait(ctx)
}
