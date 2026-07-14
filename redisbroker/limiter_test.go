// limiter_test.go L1 分布式限流专项(T111):同一个 miniredis 上开两个限流器实例
// 模拟两个进程,验证并发槽与 RPS 令牌都是全局共享的配额。
// 注意分工:信号量的时间全由 Go 注入(fakeclock 可控);redis_rate 的 GCRA 用
// Redis 服务器时间(FR-018 既有豁免),所以 RPS 用例走真时钟短窗口,
// 稳定性手法照根包 limiter_test.go:Burst=1 先烧掉初始令牌,窗口内只数匀速补充。
package redisbroker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/ambrose/taskgate"
	"github.com/ambrose/taskgate/internal/fakeclock"
	"github.com/ambrose/taskgate/redisbroker"
)

// newLimiterBroker 在指定 miniredis 上建一个已 Init 的 Broker(模拟一个进程)。
func newLimiterBroker(t *testing.T, addr string, opts taskgate.BrokerOptions) *redisbroker.Broker {
	t.Helper()
	b, err := redisbroker.New(redisbroker.Options{Addr: addr})
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Init(opts); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	return b
}

// mustLimiter 构造队列限流器,失败挂测试。
func mustLimiter(t *testing.T, b *redisbroker.Broker, qc taskgate.QueueConfig) taskgate.QueueLimiter {
	t.Helper()
	l, err := b.QueueLimiter("q", qc)
	if err != nil {
		t.Fatalf("QueueLimiter 失败: %v", err)
	}
	return l
}

// TestLimiterGlobalWorkers {Workers:2} 全局共享:两个实例合计只能占 2 个槽,
// 第 3 个阻塞到超时;任一实例归还后,另一实例立刻能占到。
func TestLimiterGlobalWorkers(t *testing.T) {
	mr := miniredis.RunT(t)
	qc := taskgate.QueueConfig{Workers: 2}
	la := mustLimiter(t, newLimiterBroker(t, mr.Addr(), taskgate.BrokerOptions{}), qc)
	lb := mustLimiter(t, newLimiterBroker(t, mr.Addr(), taskgate.BrokerOptions{}), qc)
	ctx := context.Background()

	if err := la.AcquireSlot(ctx); err != nil {
		t.Fatalf("实例 A 第 1 个槽失败: %v", err)
	}
	t.Cleanup(la.ReleaseSlot)
	if err := lb.AcquireSlot(ctx); err != nil {
		t.Fatalf("实例 B 第 2 个槽失败: %v", err)
	}

	// 第 3 个:全局满员,哪个实例来占都得阻塞到 ctx 超时。
	shortCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if err := la.AcquireSlot(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("全局满员时第 3 个槽应超时,得到: %v", err)
	}

	// B 归还一个 → A 能占到(跨实例看见同一份配额)。
	lb.ReleaseSlot()
	okCtx, cancel2 := context.WithTimeout(ctx, 3*time.Second)
	defer cancel2()
	if err := la.AcquireSlot(okCtx); err != nil {
		t.Fatalf("归还后另一实例应能占到: %v", err)
	}
	t.Cleanup(la.ReleaseSlot)
}

// TestLimiterSlotExpiry 槽过期自动回收:实例 A 占满 2 个槽后"崩溃"
// (Close 连接,续期从此写不进去),fakeclock 推过 TTL 后实例 B 直接占到:
// sem_acquire.lua 会先清掉过期槽(SC-003 的崩溃自愈,不用任何人善后)。
// 只测信号量:时间全由 Go 注入,fakeclock 完全可控(redis_rate 不在本用例)。
func TestLimiterSlotExpiry(t *testing.T) {
	mr := miniredis.RunT(t)
	clk := fakeclock.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	const ttl = 3 * time.Second
	opts := taskgate.BrokerOptions{DefaultLeaseTTL: ttl, Clock: clk}
	qc := taskgate.QueueConfig{Workers: 2}

	ba := newLimiterBroker(t, mr.Addr(), opts)
	la := mustLimiter(t, ba, qc)
	lb := mustLimiter(t, newLimiterBroker(t, mr.Addr(), opts), qc)
	ctx := context.Background()

	if err := la.AcquireSlot(ctx); err != nil {
		t.Fatalf("A 第 1 个槽失败: %v", err)
	}
	if err := la.AcquireSlot(ctx); err != nil {
		t.Fatalf("A 第 2 个槽失败: %v", err)
	}
	// 归还兜底放在最后执行(Cleanup 是 LIFO):只为停续期 goroutine,零泄漏。
	t.Cleanup(la.ReleaseSlot)
	t.Cleanup(la.ReleaseSlot)

	// 模拟 A 崩溃:连接关掉,续期 goroutine 从此只会写失败(被容忍),槽不再续命。
	_ = ba.Close()

	// TTL 内:B 占不到(槽还活着)。
	shortCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if err := lb.AcquireSlot(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("TTL 内槽不该被回收,B 应超时,得到: %v", err)
	}

	// 推过 TTL:A 的两个槽过期,B 连占两个都成功。
	clk.Advance(ttl + time.Millisecond)
	okCtx, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	for i := 1; i <= 2; i++ {
		if err := lb.AcquireSlot(okCtx); err != nil {
			t.Fatalf("过期回收后 B 第 %d 个槽应成功: %v", i, err)
		}
		t.Cleanup(lb.ReleaseSlot)
	}
}

// TestLimiterRenewKeepsSlot 续期保活:fakeclock 推进 4 秒(超过 TTL=3s),
// 每步等续期 goroutine 把 score 刷到新的 now+TTL,槽全程不过期——
// 另一实例始终占不到(证明"活着的持有者不会被误回收",与任务租约心跳同语义)。
func TestLimiterRenewKeepsSlot(t *testing.T) {
	mr := miniredis.RunT(t)
	clk := fakeclock.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	const ttl = 3 * time.Second
	opts := taskgate.BrokerOptions{DefaultLeaseTTL: ttl, Clock: clk}
	qc := taskgate.QueueConfig{Workers: 1}

	la := mustLimiter(t, newLimiterBroker(t, mr.Addr(), opts), qc)
	lb := mustLimiter(t, newLimiterBroker(t, mr.Addr(), opts), qc)
	ctx := context.Background()

	if err := la.AcquireSlot(ctx); err != nil {
		t.Fatalf("占槽失败: %v", err)
	}
	t.Cleanup(la.ReleaseSlot)

	// 旁观客户端直读 sem zset,等续期落地(续期 goroutine 是异步的,必须轮询同步)。
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	semKey := "tg:sem:q"

	// 续期 ticker 是 AcquireSlot 返回后由后台 goroutine 异步注册的:
	// 必须先等它挂上假时钟再 Advance,否则拨钟发生在注册之前,
	// 那一格滴答永远丢失、续期永远不会发生(机器繁忙时真出现过)。
	regDeadline := time.Now().Add(5 * time.Second)
	for clk.TickerCount() == 0 {
		if time.Now().After(regDeadline) {
			t.Fatal("续期 ticker 迟迟没有注册到假时钟上")
		}
		time.Sleep(time.Millisecond)
	}

	for step := 0; step < 4; step++ {
		clk.Advance(time.Second) // 续期 ticker 周期 = TTL/3 = 1s,每步触发一次
		want := float64(clk.Now().Add(ttl).UnixMilli())
		deadline := time.Now().Add(5 * time.Second)
		for {
			zs, err := rdb.ZRangeWithScores(ctx, semKey, 0, -1).Result()
			if err == nil && len(zs) == 1 && zs[0].Score == want {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("第 %d 步续期未落地: zset=%v want=%v", step+1, zs, want)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	// 总推进 4s > TTL=3s,槽因续期仍然活着:B 必须占不到。
	shortCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if err := lb.AcquireSlot(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("持有者活着(续期中)时槽不该被抢走,得到: %v", err)
	}

	// 正常归还后 B 立刻能占到。
	la.ReleaseSlot()
	okCtx, cancel2 := context.WithTimeout(ctx, 3*time.Second)
	defer cancel2()
	if err := lb.AcquireSlot(okCtx); err != nil {
		t.Fatalf("归还后 B 应能占到: %v", err)
	}
	t.Cleanup(lb.ReleaseSlot)
}

// TestLimiterDistributedRPS {RPS:10} 全局共享:两个实例轮流要令牌,
// 1 秒窗口合计放行 10±2。真时钟短窗口(GCRA 用 Redis 服务器时间,fakeclock 管不到);
// Burst=1 且先烧掉初始令牌,窗口内的放行数只由补充速率决定(照根包同名手法)。
func TestLimiterDistributedRPS(t *testing.T) {
	mr := miniredis.RunT(t)
	qc := taskgate.QueueConfig{Workers: 2, RPS: 10, Burst: 1}
	lims := []taskgate.QueueLimiter{
		mustLimiter(t, newLimiterBroker(t, mr.Addr(), taskgate.BrokerOptions{}), qc),
		mustLimiter(t, newLimiterBroker(t, mr.Addr(), taskgate.BrokerOptions{}), qc),
	}

	// 烧掉初始突发令牌,让计数从"匀速补充"开始。
	if err := lims[0].WaitToken(context.Background()); err != nil {
		t.Fatalf("烧初始令牌失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	count := 0
	for i := 0; ; i++ {
		if err := lims[i%2].WaitToken(ctx); err != nil {
			break // 窗口结束
		}
		count++
	}
	if count < 8 || count > 12 {
		t.Fatalf("RPS=10 两实例 1 秒合计应放行 10±2 个,实际 %d", count)
	}
}

// TestLimiterNoRPS RPS=0 → 不限速直通,不打任何 Redis 请求,瞬间放行。
func TestLimiterNoRPS(t *testing.T) {
	mr := miniredis.RunT(t)
	lim := mustLimiter(t, newLimiterBroker(t, mr.Addr(), taskgate.BrokerOptions{}),
		taskgate.QueueConfig{Workers: 1})

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

// TestLimiterReleaseIdempotent 多余的 ReleaseSlot 是无害 no-op(不挂死、不panic、
// 不产生负配额);正常配对使用后再多归还一次,后续占用照常。
// 续期 goroutine 的零泄漏由 -race + close/wait 同步保证(归还即同步等续期退出)。
func TestLimiterReleaseIdempotent(t *testing.T) {
	mr := miniredis.RunT(t)
	lim := mustLimiter(t, newLimiterBroker(t, mr.Addr(), taskgate.BrokerOptions{}),
		taskgate.QueueConfig{Workers: 1})
	ctx := context.Background()

	lim.ReleaseSlot() // 什么都没占,直接归还:no-op

	if err := lim.AcquireSlot(ctx); err != nil {
		t.Fatalf("占槽失败: %v", err)
	}
	lim.ReleaseSlot()
	lim.ReleaseSlot() // 多归还一次:no-op

	// 状态没被搞坏:还能正常占/还,且 Workers=1 的互斥仍然成立。
	if err := lim.AcquireSlot(ctx); err != nil {
		t.Fatalf("再次占槽失败: %v", err)
	}
	shortCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if err := lim.AcquireSlot(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Workers=1 已占用时应超时,得到: %v", err)
	}
	lim.ReleaseSlot()
}
