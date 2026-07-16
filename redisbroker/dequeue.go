package redisbroker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/oklog/ulid/v2"
)

// Dequeue 阻塞认领:直到某队列出现"status∈{pending,retrying} 且 run_at≤now"的任务,
// 或 ctx 取消(返回 ctx.Err())。循环结构与 sqlitebroker 完全同构:
// 试认领一次(claim.lua 原子)→ 无果则挂起等待,三个唤醒源:
//   - 同进程写入踢的内部信号(Enqueue/Ack/Fail/... 后 wakeAll);
//   - 注入 clock 的到点信号:等 min(100ms, 最近的 delayed 到期时刻 - now),
//     延迟任务到点自动醒,100ms 同时兜底跨进程写入(fakeclock 下不推时间就纯挂起,不空转);
//   - ctx 取消。
func (b *Broker) Dequeue(ctx context.Context, queues []string) (*taskgate.Task, error) {
	if len(queues) == 0 {
		return nil, errors.New("redisbroker: dequeue needs at least one queue")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := b.requireInit(); err != nil {
			return nil, err
		}
		wake := b.wakeChan() // 先取信号再试认领:试完到挂起之间的写入不会丢信号
		tk, next, err := b.tryClaim(ctx, queues)
		if err != nil {
			// ctx 取消时 go-redis 会中断在途请求,错误以网络错误的形态漏出来;
			// 对调用方统一翻译回 ctx.Err()(sqlite 后端 SQLITE_INTERRUPT 的同款教训,
			// 合同要求取消只暴露标准取消错误)。
			if cerr := ctx.Err(); cerr != nil {
				return nil, cerr
			}
			return nil, err
		}
		if tk != nil {
			b.fireNotify([]taskgate.Task{*tk})
			return tk, nil
		}
		now := b.clk.Now()
		wait := pollInterval
		if !next.IsZero() {
			if d := next.Sub(now); d < wait {
				wait = d
			}
		}
		if wait <= 0 {
			continue // 查询和现在之间刚好有任务到点,立刻再试
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.clk.After(wait):
		case <-wake:
		}
	}
}

// tryClaim 执行一次 claim.lua:搬到期 delayed → LPOP 校验循环 → 认领(写 running/
// 新令牌/lease_until/首次 started_at/inflight/索引计数)。令牌与时间由 Go 注入。
// 没认领到时带回最近的 delayed 到期时刻,给调用方决定挂多久。
func (b *Broker) tryClaim(ctx context.Context, queues []string) (*taskgate.Task, time.Time, error) {
	now := b.clk.Now()
	token := ulid.Make().String() // 每次认领都发全新令牌
	args := make([]any, 0, 4+len(queues)*2)
	args = append(args, b.prefix, now.UnixMilli(), token, len(queues))
	for _, q := range queues {
		args = append(args, q, b.ttlFor(q).Milliseconds())
	}
	res, err := scriptClaim.Run(ctx, b.rdb, nil, args...).Result()
	if err != nil {
		return nil, time.Time{}, mapLuaErr(err)
	}
	reply, ok := res.([]any)
	if !ok || len(reply) < 1 {
		return nil, time.Time{}, fmt.Errorf("redisbroker: unexpected claim reply %T", res)
	}
	marker, _ := reply[0].(string)
	switch marker {
	case "task":
		m, err := flatToMap(reply[1:])
		if err != nil {
			return nil, time.Time{}, err
		}
		tk, err := decodeTask(m)
		if err != nil {
			return nil, time.Time{}, err
		}
		return tk, time.Time{}, nil
	case "none":
		var next time.Time
		if len(reply) > 1 {
			s, _ := reply[1].(string)
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, time.Time{}, fmt.Errorf("redisbroker: bad next-due reply %q: %w", s, err)
			}
			next = fromMS(n)
		}
		return nil, next, nil
	default:
		return nil, time.Time{}, fmt.Errorf("redisbroker: unexpected claim marker %q", marker)
	}
}
