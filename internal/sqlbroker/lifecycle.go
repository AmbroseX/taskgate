package sqlbroker

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/AmbroseX/taskgate"
)

// Ack 成功完结:completed + Result + FinishedAt,并在同一事务内唤醒子任务。
func (b *Broker) Ack(ctx context.Context, id, leaseToken string, result []byte) error {
	if err := b.requireInit(); err != nil {
		return err
	}
	notifs, err := b.withTx(ctx, func(tx *sql.Tx) ([]taskgate.Task, error) {
		var ns []taskgate.Task
		r, err := b.getRecForUpdate(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if err := checkLeaseRec(r, leaseToken); err != nil {
			return nil, err
		}
		if !taskgate.CanTransition(r.task.Status, taskgate.StatusCompleted) {
			return nil, b.illegalTransition(r.task.Status, taskgate.StatusCompleted, id)
		}
		now := b.clk.Now().Truncate(time.Millisecond)
		if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET
				status = 'completed', result = ?, finished_at = ?,
				lease_token = '', lease_until = 0, cancel_requested = 0
			WHERE id = ?`), result, now.UnixMilli(), id); err != nil {
			return nil, err
		}
		r.task.Status = taskgate.StatusCompleted
		if result != nil {
			r.task.Result = append([]byte(nil), result...)
		}
		r.task.FinishedAt = now
		r.task.LeaseToken = ""
		ns = append(ns, r.task)
		if err := b.propagateTx(ctx, tx, id, taskgate.StatusCompleted, now, &ns); err != nil {
			return nil, err
		}
		return ns, nil
	})
	if err != nil {
		return err
	}
	b.wakeAll()
	b.fireNotify(notifs)
	return nil
}

// Fail 失败路径:按 FailKind 动对应计数,封顶或耗尽进 failed(同事务连锁传播),
// 否则进 retrying、RunAt=retryAt 到点重跑。
func (b *Broker) Fail(ctx context.Context, id, leaseToken, errMsg string, kind taskgate.FailKind, retryAt time.Time) error {
	if err := b.requireInit(); err != nil {
		return err
	}
	notifs, err := b.withTx(ctx, func(tx *sql.Tx) ([]taskgate.Task, error) {
		var ns []taskgate.Task
		r, err := b.getRecForUpdate(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if err := checkLeaseRec(r, leaseToken); err != nil {
			return nil, err
		}
		now := b.clk.Now().Truncate(time.Millisecond)

		toFailed := false
		r.task.LastError = errMsg
		switch kind {
		case taskgate.FailBusiness:
			r.task.Attempts++
			toFailed = r.task.Attempts > r.task.MaxRetry
		case taskgate.FailThrottled:
			r.task.Throttled++
			if r.task.Throttled >= b.opts.ThrottledMax {
				toFailed = true
				r.task.LastError = fmt.Sprintf("throttled %d times", r.task.Throttled)
			}
		case taskgate.FailSkip:
			toFailed = true
		default:
			return nil, fmt.Errorf("sqlbroker: unknown FailKind %d", kind)
		}

		if toFailed {
			if !taskgate.CanTransition(r.task.Status, taskgate.StatusFailed) {
				return nil, b.illegalTransition(r.task.Status, taskgate.StatusFailed, id)
			}
			if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET
					status = 'failed', last_error = ?, attempts = ?, throttled = ?, finished_at = ?,
					lease_token = '', lease_until = 0, cancel_requested = 0
				WHERE id = ?`),
				r.task.LastError, r.task.Attempts, r.task.Throttled, now.UnixMilli(), id); err != nil {
				return nil, err
			}
			r.task.Status = taskgate.StatusFailed
			r.task.FinishedAt = now
			r.task.LeaseToken = ""
			ns = append(ns, r.task)
			if err := b.propagateTx(ctx, tx, id, taskgate.StatusFailed, now, &ns); err != nil {
				return nil, err
			}
			return ns, nil
		}

		if retryAt.IsZero() {
			retryAt = now
		}
		retryAt = retryAt.Truncate(time.Millisecond)
		if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET
				status = 'retrying', last_error = ?, attempts = ?, throttled = ?, run_at = ?,
				lease_token = '', lease_until = 0
			WHERE id = ?`),
			r.task.LastError, r.task.Attempts, r.task.Throttled, ms(retryAt), id); err != nil {
			return nil, err
		}
		r.task.Status = taskgate.StatusRetrying
		r.task.RunAt = retryAt
		r.task.LeaseToken = ""
		ns = append(ns, r.task)
		return ns, nil
	})
	if err != nil {
		return err
	}
	b.wakeAll()
	b.fireNotify(notifs)
	return nil
}

// Cancel 取消:排队类状态(blocked/pending/retrying)直接 canceled 并同事务传播;
// running 只打 cancel_requested 标记(终态由 FinishCanceled 落库);
// 终态报 ErrAlreadyFinal,不存在报 ErrTaskNotFound。
func (b *Broker) Cancel(ctx context.Context, id string) error {
	if err := b.requireInit(); err != nil {
		return err
	}
	notifs, err := b.withTx(ctx, func(tx *sql.Tx) ([]taskgate.Task, error) {
		var ns []taskgate.Task
		r, err := b.getRecForUpdate(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if r.task.Status.IsFinal() {
			return nil, fmt.Errorf("%w: task %s (status=%s)", taskgate.ErrAlreadyFinal, id, r.task.Status)
		}
		if r.task.Status == taskgate.StatusRunning {
			if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET cancel_requested = 1 WHERE id = ?`), id); err != nil {
				return nil, err
			}
			return nil, nil
		}
		if !taskgate.CanTransition(r.task.Status, taskgate.StatusCanceled) {
			return nil, b.illegalTransition(r.task.Status, taskgate.StatusCanceled, id)
		}
		now := b.clk.Now().Truncate(time.Millisecond)
		if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET
				status = 'canceled', last_error = 'canceled', finished_at = ?,
				lease_token = '', lease_until = 0
			WHERE id = ?`), now.UnixMilli(), id); err != nil {
			return nil, err
		}
		r.task.Status = taskgate.StatusCanceled
		r.task.LastError = "canceled"
		r.task.FinishedAt = now
		r.task.LeaseToken = ""
		ns = append(ns, r.task)
		if err := b.propagateTx(ctx, tx, id, taskgate.StatusCanceled, now, &ns); err != nil {
			return nil, err
		}
		return ns, nil
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
	if err := b.requireInit(); err != nil {
		return err
	}
	notifs, err := b.withTx(ctx, func(tx *sql.Tx) ([]taskgate.Task, error) {
		var ns []taskgate.Task
		r, err := b.getRecForUpdate(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if err := checkLeaseRec(r, leaseToken); err != nil {
			return nil, err
		}
		if !taskgate.CanTransition(r.task.Status, taskgate.StatusCanceled) {
			return nil, b.illegalTransition(r.task.Status, taskgate.StatusCanceled, id)
		}
		now := b.clk.Now().Truncate(time.Millisecond)
		if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET
				status = 'canceled', last_error = 'canceled', finished_at = ?,
				lease_token = '', lease_until = 0, cancel_requested = 0
			WHERE id = ?`), now.UnixMilli(), id); err != nil {
			return nil, err
		}
		r.task.Status = taskgate.StatusCanceled
		r.task.LastError = "canceled"
		r.task.FinishedAt = now
		r.task.LeaseToken = ""
		ns = append(ns, r.task)
		if err := b.propagateTx(ctx, tx, id, taskgate.StatusCanceled, now, &ns); err != nil {
			return nil, err
		}
		return ns, nil
	})
	if err != nil {
		return err
	}
	b.wakeAll()
	b.fireNotify(notifs)
	return nil
}

// Requeue 优雅停机时归还任务:running → pending,三计数与 RunAt 全不动,清租约和取消标记。
// 这不算失败,一个计数都不许占(合同要求)。
func (b *Broker) Requeue(ctx context.Context, id, leaseToken string) error {
	if err := b.requireInit(); err != nil {
		return err
	}
	notifs, err := b.withTx(ctx, func(tx *sql.Tx) ([]taskgate.Task, error) {
		var ns []taskgate.Task
		r, err := b.getRecForUpdate(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if err := checkLeaseRec(r, leaseToken); err != nil {
			return nil, err
		}
		if !taskgate.CanTransition(r.task.Status, taskgate.StatusPending) {
			return nil, b.illegalTransition(r.task.Status, taskgate.StatusPending, id)
		}
		if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET
				status = 'pending', lease_token = '', lease_until = 0, cancel_requested = 0
			WHERE id = ?`), id); err != nil {
			return nil, err
		}
		r.task.Status = taskgate.StatusPending
		r.task.LeaseToken = ""
		ns = append(ns, r.task)
		return ns, nil
	})
	if err != nil {
		return err
	}
	b.wakeAll()
	b.fireNotify(notifs)
	return nil
}

// Heartbeat 续租:lease_until = now + TTL。发现取消标记时续租照做(先提交),
// 再返回 ErrTaskCanceled 提醒 scheduler 去 cancel handler 的 ctx。
func (b *Broker) Heartbeat(ctx context.Context, id, leaseToken string) error {
	if err := b.requireInit(); err != nil {
		return err
	}
	canceled := false
	_, err := b.withTx(ctx, func(tx *sql.Tx) ([]taskgate.Task, error) {
		canceled = false
		r, err := b.getRecForUpdate(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if err := checkLeaseRec(r, leaseToken); err != nil {
			return nil, err
		}
		until := b.clk.Now().Add(b.ttlFor(r.task.Queue)).UnixMilli()
		if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET lease_until = ? WHERE id = ?`), until, id); err != nil {
			return nil, err
		}
		canceled = r.cancelRequested
		return nil, nil
	})
	if err != nil {
		return err
	}
	if canceled {
		return fmt.Errorf("%w: task %s", taskgate.ErrTaskCanceled, id)
	}
	return nil
}
