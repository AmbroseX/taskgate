package redisbroker

import (
	"context"
	"fmt"
	"time"

	"github.com/AmbroseX/taskgate"
)

// runFinish 五个写终点共用的执行器:跑 finish.lua(op 区分语义),
// 把脚本返回的流转快照解码后在脚本外异步 Notify,并踢一次同进程唤醒信号。
func (b *Broker) runFinish(ctx context.Context, op, id, token string, extra ...any) error {
	if err := b.requireInit(); err != nil {
		return err
	}
	args := make([]any, 0, 5+len(extra))
	args = append(args, b.prefix, op, id, token, b.clk.Now().UnixMilli())
	args = append(args, extra...)
	res, err := scriptFinish.Run(ctx, b.rdb, nil, args...).Result()
	if err != nil {
		return mapLuaErr(err)
	}
	snaps, err := decodeSnaps(res)
	if err != nil {
		return err
	}
	b.wakeAll() // 被唤醒的子任务/回队列的任务可能正被 Dequeue 等着
	b.fireNotify(snaps)
	return nil
}

// Ack 成功完结:completed + Result + FinishedAt,子任务唤醒在同一段脚本内收敛。
func (b *Broker) Ack(ctx context.Context, id, leaseToken string, result []byte) error {
	hasResult := "0"
	if result != nil {
		hasResult = "1"
	}
	return b.runFinish(ctx, "ack", id, leaseToken, hasResult, string(result))
}

// Fail 失败路径:按 FailKind 动对应计数,封顶或耗尽进 failed(同脚本连锁传播),
// 否则进 retrying、RunAt=retryAt 到点重跑。
func (b *Broker) Fail(ctx context.Context, id, leaseToken, errMsg string, kind taskgate.FailKind, retryAt time.Time) error {
	var kindStr string
	switch kind {
	case taskgate.FailBusiness:
		kindStr = "business"
	case taskgate.FailThrottled:
		kindStr = "throttled"
	case taskgate.FailSkip:
		kindStr = "skip"
	default:
		return fmt.Errorf("redisbroker: unknown FailKind %d", kind)
	}
	if err := b.requireInit(); err != nil {
		return err
	}
	if retryAt.IsZero() {
		retryAt = b.clk.Now()
	}
	return b.runFinish(ctx, "fail", id, leaseToken,
		errMsg, kindStr, retryAt.UnixMilli(), b.opts.ThrottledMax)
}

// Cancel 取消:排队类状态(blocked/pending/retrying)直接 canceled 并同脚本传播;
// running 只打 cancel_requested 标记(终态由 FinishCanceled 落库);
// 终态报 ErrAlreadyFinal,不存在报 ErrTaskNotFound。Cancel 不带令牌(合同)。
func (b *Broker) Cancel(ctx context.Context, id string) error {
	return b.runFinish(ctx, "cancel", id, "")
}

// FinishCanceled worker 响应取消后收尾:running → canceled 落库并同脚本传播。
func (b *Broker) FinishCanceled(ctx context.Context, id, leaseToken string) error {
	return b.runFinish(ctx, "finish_canceled", id, leaseToken)
}

// Requeue 优雅停机时归还任务:running → pending,三计数与 RunAt 全不动,
// 清租约和取消标记。这不算失败,一个计数都不许占(合同要求)。
func (b *Broker) Requeue(ctx context.Context, id, leaseToken string) error {
	return b.runFinish(ctx, "requeue", id, leaseToken)
}

// Heartbeat 续租:lease_until = now + TTL(hash 与 inflight score 一起续)。
// 发现取消标记时**续租已成**才返回 ErrTaskCanceled,scheduler 收到后 cancel handler ctx。
func (b *Broker) Heartbeat(ctx context.Context, id, leaseToken string) error {
	if err := b.requireInit(); err != nil {
		return err
	}
	// 队列名在 hash 里,Lua 才知道;把按队列的 TTL 覆写表整个传进去挑(对应 ttlFor)。
	args := make([]any, 0, 6+len(b.opts.LeaseTTL)*2)
	args = append(args, b.prefix, id, leaseToken, b.clk.Now().UnixMilli(),
		b.opts.DefaultLeaseTTL.Milliseconds(), len(b.opts.LeaseTTL))
	for q, d := range b.opts.LeaseTTL {
		args = append(args, q, d.Milliseconds())
	}
	res, err := scriptHeartbeat.Run(ctx, b.rdb, nil, args...).Result()
	if err != nil {
		return mapLuaErr(err)
	}
	if flag, _ := res.(string); flag == "1" {
		return fmt.Errorf("%w: task %s", taskgate.ErrTaskCanceled, id)
	}
	return nil
}

// ReapExpired 回收过期租约(lease_until < now 严格小于,压线不算过期):
// 带 cancel_requested 的直接落 canceled(不占 LeaseLost,传播);其余 LeaseLost+1,
// 封顶进 failed(固定文案,传播)否则回 pending 清令牌;顺带防御修复
// "blocked 但父实际全部终态"的任务。整个过程在 reap.lua 一段脚本内完成,
// 返回值只算租约回收条数。
func (b *Broker) ReapExpired(ctx context.Context) (int, error) {
	if err := b.requireInit(); err != nil {
		return 0, err
	}
	res, err := scriptReap.Run(ctx, b.rdb, nil,
		b.prefix, b.clk.Now().UnixMilli(), b.opts.LeaseLostMax).Result()
	if err != nil {
		return 0, mapLuaErr(err)
	}
	reply, ok := res.([]any)
	if !ok || len(reply) != 2 {
		return 0, fmt.Errorf("redisbroker: unexpected reap reply %T", res)
	}
	count, ok := reply[0].(int64)
	if !ok {
		return 0, fmt.Errorf("redisbroker: unexpected reap count %T", reply[0])
	}
	snaps, err := decodeSnaps(reply[1])
	if err != nil {
		return 0, err
	}
	if len(snaps) > 0 {
		b.wakeAll() // 有任务回 pending / 被唤醒,叫醒等待中的 Dequeue
	}
	b.fireNotify(snaps)
	return int(count), nil
}
