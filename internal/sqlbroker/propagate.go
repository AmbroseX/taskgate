package sqlbroker

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/AmbroseX/taskgate"
)

// propagateTx 任务进终态后的依赖传播,必须和触发它的状态写入同处一个事务——
// 这是"不丢唤醒"的生命线(宪法 III)。用工作队列逐层处理:每层只碰直接子任务,
// 子被连锁取消后再入队处理孙,不递归整棵树;整条链在本次调用(同一事务)内收敛。
// 决策全部走 deps.go 的纯函数,与 memorybroker/sqlitebroker 语义完全一致。
//
// 子任务一律 getRecForUpdate 锁行再改:加锁顺序取决于树形状,两个并发传播
// (Cancel 子树 vs ReapExpired 回收、两个 Fail 打进同一片子树)必然产生死锁窗口,
// 由 withTx 的重试环吸收(整个事务回滚重跑),不漏错给调用方。
func (b *Broker) propagateTx(ctx context.Context, tx *sql.Tx, id string, status taskgate.Status, now time.Time, notifs *[]taskgate.Task) error {
	type item struct {
		id     string
		status taskgate.Status
	}
	nowMS := now.UnixMilli()
	work := []item{{id, status}}
	for len(work) > 0 {
		p := work[0]
		work = work[1:]
		// 父到终态,依赖边一律标 done(防御修复与排查用)。
		if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{d}} SET done = 1 WHERE parent_id = ?`), p.id); err != nil {
			return err
		}
		// 先把直接子任务 ID 全部读出来再逐个处理:不能边扫结果集边发新语句。
		childIDs, err := b.childrenOf(ctx, tx, p.id)
		if err != nil {
			return err
		}
		for _, cid := range childIDs {
			c, err := b.getRecForUpdate(ctx, tx, cid)
			if err != nil {
				if errors.Is(err, taskgate.ErrTaskNotFound) {
					continue // 子记录没了,跳过
				}
				return err
			}
			newPending, action := taskgate.DecideOnParentFinal(
				p.status, c.task.Status, c.task.OnParentFailure, c.pendingParents)
			switch action {
			case taskgate.ChildWake:
				if !taskgate.CanTransition(c.task.Status, taskgate.StatusPending) {
					continue
				}
				if _, err := tx.ExecContext(ctx,
					b.prep(`UPDATE {{t}} SET status = 'pending', pending_parents = ? WHERE id = ?`),
					newPending, cid); err != nil {
					return err
				}
				c.task.Status = taskgate.StatusPending
				*notifs = append(*notifs, c.task)
			case taskgate.ChildCancel:
				if c.task.Status == taskgate.StatusRunning {
					// 防御:正常流程里子等父时不可能在跑;万一出现,打取消标记走 Heartbeat 通道。
					if _, err := tx.ExecContext(ctx,
						b.prep(`UPDATE {{t}} SET cancel_requested = 1 WHERE id = ?`), cid); err != nil {
						return err
					}
					continue
				}
				if !taskgate.CanTransition(c.task.Status, taskgate.StatusCanceled) {
					continue
				}
				reason := taskgate.ParentFailureReason(p.id, p.status)
				if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET
						status = 'canceled', last_error = ?, finished_at = ?,
						lease_token = '', lease_until = 0, pending_parents = ?
					WHERE id = ?`),
					reason, nowMS, newPending, cid); err != nil {
					return err
				}
				c.task.Status = taskgate.StatusCanceled
				c.task.LastError = reason
				c.task.FinishedAt = now
				c.task.LeaseToken = ""
				*notifs = append(*notifs, c.task)
				work = append(work, item{cid, taskgate.StatusCanceled}) // 链式:接着处理它的直接子任务
			default: // ChildNone
				if _, err := tx.ExecContext(ctx,
					b.prep(`UPDATE {{t}} SET pending_parents = ? WHERE id = ?`), newPending, cid); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// childrenOf 读出直接子任务 ID(读完即关结果集,再做后续写入)。
func (b *Broker) childrenOf(ctx context.Context, tx *sql.Tx, parentID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx,
		b.prep(`SELECT child_id FROM {{d}} WHERE parent_id = ? ORDER BY child_id`), parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
