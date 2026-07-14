package sqlitebroker

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ambrose/taskgate"
)

// Ack 成功完结:completed + Result + FinishedAt,并在同一事务内唤醒子任务。
// testHookBeforeAckCommit 在全部写入之后、提交之前触发(崩溃专项测试用,默认 nil)。
func (b *Broker) Ack(ctx context.Context, id, leaseToken string, result []byte) error {
	var notifs []taskgate.Task
	err := b.withTx(ctx, func(tx *sql.Tx) error {
		r, err := getRec(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := checkLeaseRec(r, leaseToken); err != nil {
			return err
		}
		if !taskgate.CanTransition(r.task.Status, taskgate.StatusCompleted) {
			return illegalTransition(r.task.Status, taskgate.StatusCompleted, id)
		}
		now := b.clk.Now()
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET
				status = 'completed', result = ?, finished_at = ?,
				lease_token = '', lease_until = 0, cancel_requested = 0
			WHERE id = ?`, result, now.UnixMilli(), id); err != nil {
			return err
		}
		r.task.Status = taskgate.StatusCompleted
		if result != nil {
			r.task.Result = append([]byte(nil), result...)
		}
		r.task.FinishedAt = now
		r.task.LeaseToken = ""
		notifs = append(notifs, r.task)
		if err := b.propagateTx(ctx, tx, id, taskgate.StatusCompleted, now, &notifs); err != nil {
			return err
		}
		if hook := testHookBeforeAckCommit; hook != nil {
			hook() // 崩溃注入点:此刻 panic/崩掉 = 事务未提交,等于什么都没发生
		}
		return nil
	})
	if err != nil {
		return err
	}
	b.wakeAll() // 被唤醒的子任务可能正被 Dequeue 等着
	b.fireNotify(notifs)
	return nil
}

// Fail 失败路径:按 FailKind 动对应计数,封顶或耗尽进 failed(同事务连锁传播),
// 否则进 retrying、RunAt=retryAt 到点重跑。
func (b *Broker) Fail(ctx context.Context, id, leaseToken, errMsg string, kind taskgate.FailKind, retryAt time.Time) error {
	var notifs []taskgate.Task
	err := b.withTx(ctx, func(tx *sql.Tx) error {
		r, err := getRec(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := checkLeaseRec(r, leaseToken); err != nil {
			return err
		}
		now := b.clk.Now()

		toFailed := false
		r.task.LastError = errMsg
		switch kind {
		case taskgate.FailBusiness:
			// 业务失败:只动 Attempts,超过 MaxRetry 就没救了。
			r.task.Attempts++
			toFailed = r.task.Attempts > r.task.MaxRetry
		case taskgate.FailThrottled:
			// 被网关限流:只动 Throttled,Attempts 一根汗毛都不动。
			r.task.Throttled++
			if r.task.Throttled >= b.opts.ThrottledMax {
				toFailed = true
				r.task.LastError = fmt.Sprintf("throttled %d times", r.task.Throttled) // 封顶用固定文案
			}
		case taskgate.FailSkip:
			toFailed = true // 明确不重试,三计数全不动
		default:
			return fmt.Errorf("sqlitebroker: unknown FailKind %d", kind)
		}

		if toFailed {
			if !taskgate.CanTransition(r.task.Status, taskgate.StatusFailed) {
				return illegalTransition(r.task.Status, taskgate.StatusFailed, id)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE tasks SET
					status = 'failed', last_error = ?, attempts = ?, throttled = ?, finished_at = ?,
					lease_token = '', lease_until = 0, cancel_requested = 0
				WHERE id = ?`,
				r.task.LastError, r.task.Attempts, r.task.Throttled, now.UnixMilli(), id); err != nil {
				return err
			}
			r.task.Status = taskgate.StatusFailed
			r.task.FinishedAt = now
			r.task.LeaseToken = ""
			notifs = append(notifs, r.task)
			// failed 也要在同一事务里连锁处理子任务。
			return b.propagateTx(ctx, tx, id, taskgate.StatusFailed, now, &notifs)
		}

		// 还有机会:进 retrying,到点(retryAt)才能被重新认领。
		if retryAt.IsZero() {
			retryAt = now
		}
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET
				status = 'retrying', last_error = ?, attempts = ?, throttled = ?, run_at = ?,
				lease_token = '', lease_until = 0
			WHERE id = ?`,
			r.task.LastError, r.task.Attempts, r.task.Throttled, ms(retryAt), id); err != nil {
			return err
		}
		r.task.Status = taskgate.StatusRetrying
		r.task.RunAt = retryAt
		r.task.LeaseToken = ""
		notifs = append(notifs, r.task)
		return nil
	})
	if err != nil {
		return err
	}
	b.wakeAll() // 让等待中的 Dequeue 重算下一个到点时刻
	b.fireNotify(notifs)
	return nil
}

// Cancel 取消:排队类状态(blocked/pending/retrying)直接 canceled 并同事务传播;
// running 只打 cancel_requested 标记(终态由 FinishCanceled 落库);
// 终态报 ErrAlreadyFinal,不存在报 ErrTaskNotFound。
func (b *Broker) Cancel(ctx context.Context, id string) error {
	var notifs []taskgate.Task
	err := b.withTx(ctx, func(tx *sql.Tx) error {
		r, err := getRec(ctx, tx, id)
		if err != nil {
			return err
		}
		if r.task.Status.IsFinal() {
			return fmt.Errorf("%w: task %s (status=%s)", taskgate.ErrAlreadyFinal, id, r.task.Status)
		}
		if r.task.Status == taskgate.StatusRunning {
			// worker 下次 Heartbeat 会收到 ErrTaskCanceled。
			_, err := tx.ExecContext(ctx, `UPDATE tasks SET cancel_requested = 1 WHERE id = ?`, id)
			return err
		}
		if !taskgate.CanTransition(r.task.Status, taskgate.StatusCanceled) {
			return illegalTransition(r.task.Status, taskgate.StatusCanceled, id)
		}
		now := b.clk.Now()
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET
				status = 'canceled', last_error = 'canceled', finished_at = ?,
				lease_token = '', lease_until = 0
			WHERE id = ?`, now.UnixMilli(), id); err != nil {
			return err
		}
		r.task.Status = taskgate.StatusCanceled
		r.task.LastError = "canceled"
		r.task.FinishedAt = now
		r.task.LeaseToken = ""
		notifs = append(notifs, r.task)
		// 向下传播:FailFast 子连锁取消。
		return b.propagateTx(ctx, tx, id, taskgate.StatusCanceled, now, &notifs)
	})
	if err != nil {
		return err
	}
	b.wakeAll()
	b.fireNotify(notifs)
	return nil
}

// FinishCanceled worker 响应取消后收尾:running → canceled 落库并同事务传播。
func (b *Broker) FinishCanceled(ctx context.Context, id, leaseToken string) error {
	var notifs []taskgate.Task
	err := b.withTx(ctx, func(tx *sql.Tx) error {
		r, err := getRec(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := checkLeaseRec(r, leaseToken); err != nil {
			return err
		}
		if !taskgate.CanTransition(r.task.Status, taskgate.StatusCanceled) {
			return illegalTransition(r.task.Status, taskgate.StatusCanceled, id)
		}
		now := b.clk.Now()
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET
				status = 'canceled', last_error = 'canceled', finished_at = ?,
				lease_token = '', lease_until = 0, cancel_requested = 0
			WHERE id = ?`, now.UnixMilli(), id); err != nil {
			return err
		}
		r.task.Status = taskgate.StatusCanceled
		r.task.LastError = "canceled"
		r.task.FinishedAt = now
		r.task.LeaseToken = ""
		notifs = append(notifs, r.task)
		return b.propagateTx(ctx, tx, id, taskgate.StatusCanceled, now, &notifs)
	})
	if err != nil {
		return err
	}
	b.wakeAll()
	b.fireNotify(notifs)
	return nil
}

// Requeue 优雅停机时归还任务:running → pending,三计数与 RunAt 全不动,
// 清租约和取消标记。这不算失败,一个计数都不许占(合同要求)。
func (b *Broker) Requeue(ctx context.Context, id, leaseToken string) error {
	var notifs []taskgate.Task
	err := b.withTx(ctx, func(tx *sql.Tx) error {
		r, err := getRec(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := checkLeaseRec(r, leaseToken); err != nil {
			return err
		}
		if !taskgate.CanTransition(r.task.Status, taskgate.StatusPending) {
			return illegalTransition(r.task.Status, taskgate.StatusPending, id)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET
				status = 'pending', lease_token = '', lease_until = 0, cancel_requested = 0
			WHERE id = ?`, id); err != nil {
			return err
		}
		r.task.Status = taskgate.StatusPending
		r.task.LeaseToken = ""
		notifs = append(notifs, r.task)
		return nil
	})
	if err != nil {
		return err
	}
	b.wakeAll()
	b.fireNotify(notifs)
	return nil
}

// Heartbeat 续租:lease_until = now + TTL。发现取消标记时**续租照做**(先提交),
// 再返回 ErrTaskCanceled 提醒 scheduler 去 cancel handler 的 ctx。
func (b *Broker) Heartbeat(ctx context.Context, id, leaseToken string) error {
	canceled := false
	err := b.withTx(ctx, func(tx *sql.Tx) error {
		r, err := getRec(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := checkLeaseRec(r, leaseToken); err != nil {
			return err
		}
		until := b.clk.Now().Add(b.ttlFor(r.task.Queue)).UnixMilli()
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET lease_until = ? WHERE id = ?`, until, id); err != nil {
			return err
		}
		canceled = r.cancelRequested
		return nil
	})
	if err != nil {
		return err
	}
	if canceled {
		return fmt.Errorf("%w: task %s", taskgate.ErrTaskCanceled, id)
	}
	return nil
}
