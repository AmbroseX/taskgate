package sqlitebroker

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/ambrose/taskgate"
	"github.com/oklog/ulid/v2"
)

// Dequeue 阻塞认领:直到某队列出现"status∈{pending,retrying} 且 run_at≤now"的任务,
// 或 ctx 取消(返回 ctx.Err())。循环结构:试认领一次 → 无果则挂起等待,三个唤醒源:
//   - 同进程写入踢的内部信号(Enqueue/Ack/Fail/... 后 wakeAll);
//   - 注入 clock 的到点信号:等 min(100ms, 最近的 run_at - now),延迟任务到点自动醒,
//     100ms 同时兜底跨进程写入(fakeclock 下不推时间就纯挂起,不空转);
//   - ctx 取消。
func (b *Broker) Dequeue(ctx context.Context, queues []string) (*taskgate.Task, error) {
	if len(queues) == 0 {
		return nil, errors.New("sqlitebroker: dequeue needs at least one queue")
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
			// ctx 取消时 database/sql 会中断正在执行的语句,错误以 sqlite 的
			// SQLITE_INTERRUPT(错误码 9)漏出来;对调用方统一翻译回 ctx.Err(),
			// 合同要求取消只暴露标准取消错误。
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

// tryClaim 在一个 BEGIN IMMEDIATE 事务里原子认领一个就绪任务:
// 子查询挑 run_at 最早(同刻取 ID 最小)的一条,UPDATE ... RETURNING 一步置 running、
// 发新令牌、首次写 StartedAt;认领到后按队列 TTL 补记 lease_until(同事务)。
// 没有就绪任务时顺带算出最近的"到点时刻",给调用方决定挂多久。
func (b *Broker) tryClaim(ctx context.Context, queues []string) (*taskgate.Task, time.Time, error) {
	now := b.clk.Now()
	nowMS := now.UnixMilli()
	token := ulid.Make().String() // 每次认领都发全新令牌
	ph := placeholders(len(queues))

	var claimed *rec
	var next time.Time
	err := b.withTx(ctx, func(tx *sql.Tx) error {
		claimed, next = nil, time.Time{}
		args := make([]any, 0, len(queues)+3)
		args = append(args, token, nowMS)
		for _, q := range queues {
			args = append(args, q)
		}
		args = append(args, nowMS)
		// 子查询写法是标准 SQL,不依赖 SQLITE_ENABLE_UPDATE_DELETE_LIMIT 编译开关。
		r, err := scanRec(tx.QueryRowContext(ctx, `UPDATE tasks SET
				status = 'running',
				lease_token = ?,
				started_at = CASE WHEN started_at = 0 THEN ? ELSE started_at END
			WHERE id = (SELECT id FROM tasks
				WHERE queue IN (`+ph+`) AND status IN ('pending','retrying') AND run_at <= ?
				ORDER BY run_at, id LIMIT 1)
			RETURNING `+taskCols, args...))
		if errors.Is(err, sql.ErrNoRows) {
			// 没抢到:算下一个到点时刻,回去挂等待。
			qargs := make([]any, 0, len(queues)+1)
			for _, q := range queues {
				qargs = append(qargs, q)
			}
			qargs = append(qargs, nowMS)
			var nx sql.NullInt64
			if err := tx.QueryRowContext(ctx, `SELECT MIN(run_at) FROM tasks
				WHERE queue IN (`+ph+`) AND status IN ('pending','retrying') AND run_at > ?`,
				qargs...).Scan(&nx); err != nil {
				return err
			}
			if nx.Valid {
				next = fromMS(nx.Int64)
			}
			return nil
		}
		if err != nil {
			return err
		}
		// 租约时长按队列配置,认领到才知道队列,单独一条 UPDATE 补上(同事务,依旧原子)。
		until := now.Add(b.ttlFor(r.task.Queue)).UnixMilli()
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET lease_until = ? WHERE id = ?`,
			until, r.task.ID); err != nil {
			return err
		}
		claimed = r
		return nil
	})
	if err != nil {
		return nil, time.Time{}, err
	}
	if claimed == nil {
		return nil, next, nil
	}
	return &claimed.task, time.Time{}, nil
}
