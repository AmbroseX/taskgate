package sqlbroker

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/oklog/ulid/v2"
)

// Dequeue 阻塞认领:直到某队列出现"status∈{pending,retrying} 且 run_at≤now"的任务,或 ctx 取消。
// 循环结构与 sqlitebroker 一致,三个唤醒源:同进程写入的内部信号、注入 clock 的到点信号、ctx。
// 跨进程写入靠 PollInterval 兜底轮询发现(默认 200ms,一次网络往返,比 sqlite 的 100ms 放宽)。
func (b *Broker) Dequeue(ctx context.Context, queues []string) (*taskgate.Task, error) {
	if len(queues) == 0 {
		return nil, errors.New("sqlbroker: dequeue needs at least one queue")
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
			// ctx 取消时 database/sql 会中断在跑的语句,错误以驱动私有形态漏出;
			// 对调用方统一翻译回 ctx.Err()(合同要求取消只暴露标准取消错误)。
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
		wait := b.cfg.PollInterval
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

// tryClaim 在一个事务里原子认领一个就绪任务。统一两步式(PG/MySQL 同一份,不用 UPDATE...RETURNING):
//  1. SELECT ... ORDER BY run_at,id LIMIT 1 FOR UPDATE SKIP LOCKED——挑最早就绪的一条并锁住,
//     SKIP LOCKED 保证两个 worker 不认领同一行、也不互相等锁(要求 MySQL 8.0+ / PG 9.5+);
//  2. 这时已知中选队列,一条 UPDATE 置 running、发新令牌、首次写 StartedAt、按队列 TTL 写 lease_until。
//
// 没有就绪任务时顺带算出最近的"到点时刻",给调用方决定挂多久。
func (b *Broker) tryClaim(ctx context.Context, queues []string) (*taskgate.Task, time.Time, error) {
	now := b.clk.Now()
	nowMS := now.UnixMilli()
	token := ulid.Make().String() // 每次认领都发全新令牌
	ph := placeholders(len(queues))

	var claimed *rec
	var next time.Time
	_, err := b.withTx(ctx, func(tx *sql.Tx) ([]taskgate.Task, error) {
		claimed, next = nil, time.Time{}

		// 第一步:锁住一条最早就绪的任务。
		selArgs := make([]any, 0, len(queues)+1)
		for _, q := range queues {
			selArgs = append(selArgs, q)
		}
		selArgs = append(selArgs, nowMS)
		r, err := scanRec(tx.QueryRowContext(ctx, b.prep(`SELECT `+taskCols+` FROM {{t}}
			WHERE queue IN (`+ph+`) AND status IN ('pending','retrying') AND run_at <= ?
			ORDER BY run_at, id LIMIT 1 FOR UPDATE SKIP LOCKED`), selArgs...))
		if errors.Is(err, sql.ErrNoRows) {
			// 没抢到:算下一个到点时刻,回去挂等待。
			qargs := make([]any, 0, len(queues)+1)
			for _, q := range queues {
				qargs = append(qargs, q)
			}
			qargs = append(qargs, nowMS)
			var nx sql.NullInt64
			if err := tx.QueryRowContext(ctx, b.prep(`SELECT MIN(run_at) FROM {{t}}
				WHERE queue IN (`+ph+`) AND status IN ('pending','retrying') AND run_at > ?`),
				qargs...).Scan(&nx); err != nil {
				return nil, err
			}
			if nx.Valid {
				next = fromMS(nx.Int64)
			}
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		// 第二步:已知队列,一条 UPDATE 写全 running/令牌/StartedAt/lease_until。
		startedAt := now.UnixMilli()
		until := now.Add(b.ttlFor(r.task.Queue)).UnixMilli()
		if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET
				status = 'running', lease_token = ?, lease_until = ?,
				started_at = CASE WHEN started_at = 0 THEN ? ELSE started_at END
			WHERE id = ?`), token, until, startedAt, r.task.ID); err != nil {
			return nil, err
		}
		// 回填内存快照(不再回读一行):置 running、写令牌、首认领补 StartedAt。
		r.task.Status = taskgate.StatusRunning
		r.task.LeaseToken = token
		if r.task.StartedAt.IsZero() {
			r.task.StartedAt = fromMS(startedAt)
		}
		claimed = r
		return nil, nil
	})
	if err != nil {
		return nil, time.Time{}, err
	}
	if claimed == nil {
		return nil, next, nil
	}
	return &claimed.task, time.Time{}, nil
}
