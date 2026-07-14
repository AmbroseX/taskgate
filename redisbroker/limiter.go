// limiter.go 分布式限流器:实现 taskgate.QueueLimiter,多进程共享同一份配额。
// 两层独立(与 localLimiter 的分层一致):
//   - 并发槽:zset 信号量 tg:sem:{q}(sem_acquire.lua 原子占槽,续期 goroutine 保活,
//     进程崩溃 → 续期停 → 槽按 TTL 过期自动回收,research 第 6 节);
//   - RPS 令牌:redis_rate 的 GCRA(research 第 7 节)。注意 redis_rate 的 Lua 用
//     Redis 服务器时间(redis.call TIME),属"RPS 走真时钟"的既有豁免(spec FR-018 例外),
//     与其余脚本"时间全由 Go 注入"的铁律并不冲突:限速要的本来就是物理时间。
//
// 键归属:tg:sem:{q} 与 redis_rate 的 rate:* 键都是限流器私有,不属 Broker 数据
// (data-model.md 第 2 节),丢了最多是限流短暂失准,不影响任务数据。
package redisbroker

import (
	"context"
	_ "embed"
	"math"
	"sync"
	"time"

	"github.com/ambrose/taskgate"
	"github.com/go-redis/redis_rate/v10"
	"github.com/oklog/ulid/v2"
	"github.com/redis/go-redis/v9"
)

//go:embed lua/sem_acquire.lua
var luaSemAcquire string

var scriptSemAcquire = redis.NewScript(luaCommon + "\n" + luaSemAcquire)

const (
	// semRetryWait 占槽失败(满员)或 Redis 抖动时的重试间隔,挂注入 clock。
	semRetryWait = 50 * time.Millisecond
	// limiterOpTimeout 续期/归还这类后台小操作的超时:不能借调用方 ctx
	// (续期没有调用方;归还必须落地),也不能无限等把 goroutine 挂死。
	limiterOpTimeout = 5 * time.Second
)

// 编译期断言:*Broker 必须实现 LimiterProvider 能力接口,scheduler 靠它拿分布式限流器。
var _ taskgate.LimiterProvider = (*Broker)(nil)

// QueueLimiter 为队列构造跨进程共享的限流器(taskgate.LimiterProvider 能力接口)。
// 槽 TTL 复用该队列的 LeaseTTL(任务租约和槽租约同生命周期,好理解;research 第 6 节)。
func (b *Broker) QueueLimiter(queue string, qc taskgate.QueueConfig) (taskgate.QueueLimiter, error) {
	if err := b.requireInit(); err != nil {
		return nil, err
	}
	return &queueLimiter{
		rdb:     b.rdb,
		clk:     b.clk,
		prefix:  b.prefix,
		queue:   queue,
		semKey:  b.prefix + "sem:" + queue,
		workers: qc.Workers,
		ttl:     b.ttlFor(queue),
		rl:      redis_rate.NewLimiter(b.rdb),
		// redis_rate 自己会再加 "rate:" 前缀,最终键形如 "rate:tg:{q}"。
		rateKey: b.prefix + queue,
		limit:   rateLimit(qc.RPS, qc.Burst),
	}, nil
}

// queueLimiter 单个队列的分布式限流器。taskgate.QueueLimiter 的 Redis 实现。
type queueLimiter struct {
	rdb     *redis.Client
	clk     taskgate.Clock
	prefix  string
	queue   string
	semKey  string
	workers int
	ttl     time.Duration

	rl      *redis_rate.Limiter
	rateKey string
	limit   redis_rate.Limit // 零值 = RPS 不限速

	mu   sync.Mutex
	held []*semSlot // 本实例持有的槽(LIFO 归还;槽之间可互换,归还哪个都一样)
}

// 编译期断言:queueLimiter 必须实现 QueueLimiter,接口改签名时这里先报错。
var _ taskgate.QueueLimiter = (*queueLimiter)(nil)

// semSlot 一个已占到的并发槽:成员 ID + 续期 goroutine 的停止/退出信号。
type semSlot struct {
	member string
	stop   chan struct{} // 关闭 = 让续期 goroutine 停
	done   chan struct{} // 续期 goroutine 退出后关闭,归还方同步等它(零泄漏)
}

// AcquireSlot 占一个全局并发槽,占不到就阻塞轮询;ctx 取消返回 ctx.Err()。
// 满员或 Redis 抖动都等 semRetryWait 再试:错误不上抛,因为 claimLoop 把
// AcquireSlot 出错当"停止认领",断连期间不能让消费循环整个退出
// (与 Dequeue 出错走冷却重试是同一个道理,恢复后自动续上)。
func (l *queueLimiter) AcquireSlot(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		member := ulid.Make().String() // 每次尝试都发全新槽 ID,不复用
		now := l.clk.Now()
		got, err := scriptSemAcquire.Run(ctx, l.rdb, nil,
			l.prefix, l.queue, l.workers, now.UnixMilli(), l.ttl.Milliseconds(), member).Int64()
		if err == nil && got == 1 {
			l.startRenew(member)
			return nil
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr // go-redis 在 ctx 取消时报网络形态的错,统一翻译回 ctx.Err()
		}
		if serr := l.clk.Sleep(ctx, semRetryWait); serr != nil {
			return serr
		}
	}
}

// ReleaseSlot 归还并发槽:停掉续期 goroutine(同步等它退出)再 ZREM。
// 没有持有任何槽时是无害 no-op(localLimiter 会卡死,这里选择宽容:
// 分布式实现的错配后果是跨进程配额泄漏,宁可吞掉多余归还也不能挂死调度循环)。
func (l *queueLimiter) ReleaseSlot() {
	l.mu.Lock()
	n := len(l.held)
	if n == 0 {
		l.mu.Unlock()
		return
	}
	s := l.held[n-1]
	l.held = l.held[:n-1]
	l.mu.Unlock()

	// 照 scheduler 心跳的 close+wait 模式:先停续期、等它彻底退出,再删槽,
	// 杜绝"ZREM 之后续期又把槽写回去"的竞态。
	close(s.stop)
	<-s.done

	ctx, cancel := context.WithTimeout(context.Background(), limiterOpTimeout)
	defer cancel()
	// ZREM 失败(断连/已 Close)也没事:槽不续期了,到 TTL 自然过期回收。
	_ = l.rdb.ZRem(ctx, l.semKey, s.member).Err()
}

// WaitToken 等一个全局 RPS 令牌;不限速时立即放行;ctx 取消返回其错误。
// 被拒时按 GCRA 返回的 RetryAfter 挂注入 clock 等待再试;
// Redis 抖动同 AcquireSlot:冷却重试,不上抛。
func (l *queueLimiter) WaitToken(ctx context.Context) error {
	if l.limit.IsZero() {
		return nil // RPS=0 = 不限速直通,与 localLimiter 一致
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		res, err := l.rl.Allow(ctx, l.rateKey, l.limit)
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			if serr := l.clk.Sleep(ctx, semRetryWait); serr != nil {
				return serr
			}
			continue
		}
		if res.Allowed > 0 {
			return nil
		}
		wait := res.RetryAfter
		if wait <= 0 {
			wait = semRetryWait // 防御:被拒却没给等待时长,兜底一个短间隔
		}
		if serr := l.clk.Sleep(ctx, wait); serr != nil {
			return serr
		}
	}
}

// startRenew 登记槽并起续期 goroutine:每 ttl/3 刷一次过期时刻,
// 节奏与任务租约心跳一致(槽和租约同生命周期)。
func (l *queueLimiter) startRenew(member string) {
	s := &semSlot{member: member, stop: make(chan struct{}), done: make(chan struct{})}
	l.mu.Lock()
	l.held = append(l.held, s)
	l.mu.Unlock()
	go l.renewLoop(s)
}

// renewLoop 单个槽的续期循环,直到 stop 关闭。
// 续期用无条件 ZADD(upsert)而不是 ZADD XX:极端情况下(续期被断连/停顿拖过 TTL)
// 成员可能已被 sem_acquire 清掉,这里等于"静默重占"。取舍(research 第 6 节的续期语义):
// 槽表达的是"本进程确实还在跑一个任务",重占把这个事实写回去;
// 若选"发现丢了就放弃",别的进程会立刻占走空位,而本进程的任务还在跑,
// 全局实际并发从此永久超限。重占最多短暂超限一拍,靠对方槽的自然过期收敛,两害取轻。
func (l *queueLimiter) renewLoop(s *semSlot) {
	defer close(s.done)
	tk := l.clk.NewTicker(l.ttl / 3)
	defer tk.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-tk.C():
			ctx, cancel := context.WithTimeout(context.Background(), limiterOpTimeout)
			score := float64(l.clk.Now().Add(l.ttl).UnixMilli())
			// 失败(网络抖动)容忍:下个周期再试;连续失败到 TTL 就让槽过期,不硬撑。
			_ = l.rdb.ZAdd(ctx, l.semKey, redis.Z{Score: score, Member: s.member}).Err()
			cancel()
		}
	}
}

// rateLimit 把队列配置换算成 redis_rate 的 GCRA 参数。
// Burst 缺省规则与 localLimiter 完全一致:burst<=0 → max(1, int(rps))。
// 整数 RPS 直接映射成"每秒 N 个";小数(0.5 之类)换算成"每 1/rps 秒 1 个",
// 两种写法的令牌发放间隔(Period/Rate)相同,只是避免小数被 int 截断成 0。
func rateLimit(rps float64, burst int) redis_rate.Limit {
	if rps <= 0 {
		return redis_rate.Limit{}
	}
	if burst <= 0 {
		burst = int(rps)
		if burst < 1 {
			burst = 1
		}
	}
	if rps == math.Trunc(rps) {
		return redis_rate.Limit{Rate: int(rps), Period: time.Second, Burst: burst}
	}
	return redis_rate.Limit{Rate: 1, Period: time.Duration(float64(time.Second) / rps), Burst: burst}
}
