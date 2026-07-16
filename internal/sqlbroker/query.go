package sqlbroker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/AmbroseX/taskgate"
)

// Get 取单个任务(只读,不加锁)。scanRec 扫出来就是全新副本,调用方改了不影响存储。
func (b *Broker) Get(ctx context.Context, id string) (*taskgate.Task, error) {
	r, err := b.getRec(ctx, b.db, id)
	if err != nil {
		return nil, err
	}
	return &r.task, nil
}

// List 按 Filter 过滤,零值字段不过滤;先过滤 → ORDER BY (created_at, id) 升序 →
// LIMIT/OFFSET 分页(排序分页合同见 broker-contract.md,Offset 越界返回空)。只读走连接池无锁。
func (b *Broker) List(ctx context.Context, f taskgate.Filter) ([]*taskgate.Task, error) {
	query := `SELECT ` + taskCols + ` FROM {{t}}`
	var conds []string
	var args []any
	if f.Type != "" {
		conds = append(conds, "type = ?")
		args = append(args, f.Type)
	}
	if f.Queue != "" {
		conds = append(conds, "queue = ?")
		args = append(args, f.Queue)
	}
	if f.Status != "" {
		conds = append(conds, "status = ?")
		args = append(args, string(f.Status))
	}
	for i, c := range conds {
		if i == 0 {
			query += " WHERE " + c
		} else {
			query += " AND " + c
		}
	}
	query += " ORDER BY created_at, id"
	// PG/MySQL 都支持 LIMIT ? OFFSET ?;只给 Offset 不给 Limit 时用一个极大 Limit 补位
	// (两库都不接受裸 OFFSET);Offset<0 走不进这个分支,等价按 0 处理。
	if f.Limit > 0 || f.Offset > 0 {
		limit := f.Limit
		if limit <= 0 {
			limit = 1<<63 - 1
		}
		query += " LIMIT ?"
		args = append(args, limit)
		if f.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, f.Offset)
		}
	}
	rows, err := b.db.QueryContext(ctx, b.prep(query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*taskgate.Task
	for rows.Next() {
		r, err := scanRec(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, &r.task)
	}
	return out, rows.Err()
}

// QueueLen 队列积压:status∈{pending,retrying} 的数量(不看 RunAt 到没到点)。
func (b *Broker) QueueLen(ctx context.Context, queue string) (int, error) {
	var n int
	err := b.db.QueryRowContext(ctx,
		b.prep(`SELECT COUNT(*) FROM {{t}} WHERE queue = ? AND status IN ('pending','retrying')`),
		queue).Scan(&n)
	return n, err
}

// Counts 出现过的 Type×Status 稀疏矩阵,和逐个 Get 汇总必须一致(brokertest 验证)。
func (b *Broker) Counts(ctx context.Context) (map[string]map[taskgate.Status]int64, error) {
	rows, err := b.db.QueryContext(ctx, b.prep(`SELECT type, status, COUNT(*) FROM {{t}} GROUP BY type, status`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]map[taskgate.Status]int64)
	for rows.Next() {
		var typ, st string
		var n int64
		if err := rows.Scan(&typ, &st, &n); err != nil {
			return nil, err
		}
		byStatus := out[typ]
		if byStatus == nil {
			byStatus = make(map[taskgate.Status]int64)
			out[typ] = byStatus
		}
		byStatus[taskgate.Status(st)] = n
	}
	return out, rows.Err()
}

// ReapExpired 回收过期租约(lease_until < now 严格小于,压线不算过期)。逐行处理(不用 UPDATE...RETURNING):
// 先 SELECT id ... FOR UPDATE SKIP LOCKED 锁住待收割的行(多进程各跑 reaper 不互踩、不重复计),
// 再逐行按 sqlite 同一套语义改状态、连锁传播;封顶失败的 last_error 文案在 Go 里拼(免 || / CONCAT 方言差异)。
//   - 第零步:带 cancel_requested 的过期任务直接落 canceled(不占 LeaseLost),触发传播;
//   - 第一步:其余过期任务 LeaseLost+1,封顶进 failed(触发传播)否则回 pending;
//   - 第二步:防御修复——blocked 却发现父全终态的,按提交时同一套决策函数补齐。
//
// 返回值只算租约回收条数(第零步 + 第一步)。
func (b *Broker) ReapExpired(ctx context.Context) (int, error) {
	if err := b.requireInit(); err != nil {
		return 0, err
	}
	var count int
	notifs, err := b.withTx(ctx, func(tx *sql.Tx) ([]taskgate.Task, error) {
		count = 0
		var ns []taskgate.Task
		now := b.clk.Now().Truncate(time.Millisecond)
		nowMS := now.UnixMilli()

		// 第零步:带取消标记的过期任务 → canceled(不占 LeaseLost)。
		cancelIDs, err := b.lockExpiredIDs(ctx, tx, nowMS, true)
		if err != nil {
			return nil, err
		}
		for _, id := range cancelIDs {
			r, err := b.getRecForUpdate(ctx, tx, id)
			if err != nil {
				if errors.Is(err, taskgate.ErrTaskNotFound) {
					continue
				}
				return nil, err
			}
			if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET
					status = 'canceled', last_error = 'canceled', finished_at = ?,
					lease_token = '', lease_until = 0, cancel_requested = 0
				WHERE id = ?`), nowMS, id); err != nil {
				return nil, err
			}
			r.task.Status = taskgate.StatusCanceled
			r.task.LastError = "canceled"
			r.task.FinishedAt = now
			r.task.LeaseToken = ""
			ns = append(ns, r.task)
			count++
			if err := b.propagateTx(ctx, tx, id, taskgate.StatusCanceled, now, &ns); err != nil {
				return nil, err
			}
		}

		// 第一步:其余过期任务 LeaseLost+1,封顶 failed / 否则回 pending。
		reapIDs, err := b.lockExpiredIDs(ctx, tx, nowMS, false)
		if err != nil {
			return nil, err
		}
		for _, id := range reapIDs {
			r, err := b.getRecForUpdate(ctx, tx, id)
			if err != nil {
				if errors.Is(err, taskgate.ErrTaskNotFound) {
					continue
				}
				return nil, err
			}
			newLost := r.task.LeaseLost + 1
			toFailed := newLost >= b.opts.LeaseLostMax
			if toFailed {
				lastErr := fmt.Sprintf("lease expired %d times", newLost)
				if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET
						lease_lost = ?, status = 'failed', last_error = ?, finished_at = ?,
						lease_token = '', lease_until = 0, cancel_requested = 0
					WHERE id = ?`), newLost, lastErr, nowMS, id); err != nil {
					return nil, err
				}
				r.task.LeaseLost = newLost
				r.task.Status = taskgate.StatusFailed
				r.task.LastError = lastErr
				r.task.FinishedAt = now
				r.task.LeaseToken = ""
				ns = append(ns, r.task)
				count++
				if err := b.propagateTx(ctx, tx, id, taskgate.StatusFailed, now, &ns); err != nil {
					return nil, err
				}
				continue
			}
			if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET
					lease_lost = ?, status = 'pending',
					lease_token = '', lease_until = 0, cancel_requested = 0
				WHERE id = ?`), newLost, id); err != nil {
				return nil, err
			}
			r.task.LeaseLost = newLost
			r.task.Status = taskgate.StatusPending
			r.task.LeaseToken = ""
			ns = append(ns, r.task)
			count++
		}

		// 第二步:防御修复。blocked 却发现父全是终态 → 用和提交时同一套决策函数补齐。
		blockedIDs, err := b.listBlockedIDs(ctx, tx)
		if err != nil {
			return nil, err
		}
		for _, bid := range blockedIDs {
			r, err := b.getRecForUpdate(ctx, tx, bid)
			if err != nil {
				if errors.Is(err, taskgate.ErrTaskNotFound) {
					continue
				}
				return nil, err
			}
			if r.task.Status != taskgate.StatusBlocked {
				continue
			}
			parents := make([]taskgate.ParentState, 0, len(r.task.DependsOn))
			allExist := true
			for _, pid := range r.task.DependsOn {
				var st string
				err := tx.QueryRowContext(ctx, b.prep(`SELECT status FROM {{t}} WHERE id = ?`), pid).Scan(&st)
				if errors.Is(err, sql.ErrNoRows) {
					allExist = false
					break
				}
				if err != nil {
					return nil, err
				}
				parents = append(parents, taskgate.ParentState{ID: pid, Status: taskgate.Status(st)})
			}
			if !allExist {
				continue
			}
			dec := taskgate.DecideOnSubmit(parents, r.task.OnParentFailure)
			switch dec.Status {
			case taskgate.StatusPending:
				if !taskgate.CanTransition(r.task.Status, taskgate.StatusPending) {
					continue
				}
				if _, err := tx.ExecContext(ctx,
					b.prep(`UPDATE {{t}} SET status = 'pending', pending_parents = 0 WHERE id = ?`), bid); err != nil {
					return nil, err
				}
				r.task.Status = taskgate.StatusPending
				ns = append(ns, r.task)
			case taskgate.StatusCanceled:
				if !taskgate.CanTransition(r.task.Status, taskgate.StatusCanceled) {
					continue
				}
				if _, err := tx.ExecContext(ctx, b.prep(`UPDATE {{t}} SET
						status = 'canceled', last_error = ?, finished_at = ?,
						lease_token = '', lease_until = 0
					WHERE id = ?`), dec.LastError, nowMS, bid); err != nil {
					return nil, err
				}
				r.task.Status = taskgate.StatusCanceled
				r.task.LastError = dec.LastError
				r.task.FinishedAt = now
				ns = append(ns, r.task)
				if err := b.propagateTx(ctx, tx, bid, taskgate.StatusCanceled, now, &ns); err != nil {
					return nil, err
				}
			default:
				if _, err := tx.ExecContext(ctx,
					b.prep(`UPDATE {{t}} SET pending_parents = ? WHERE id = ?`), dec.PendingParents, bid); err != nil {
					return nil, err
				}
			}
		}
		return ns, nil
	})
	if err != nil {
		return 0, err
	}
	if len(notifs) > 0 {
		b.wakeAll()
	}
	b.fireNotify(notifs)
	return count, nil
}

// lockExpiredIDs 锁住过期 running 任务的 id 集合(FOR UPDATE SKIP LOCKED,多 reaper 不互踩)。
// wantCancel=true 只取带 cancel_requested 的(第零步),false 取全部过期(第一步)。
func (b *Broker) lockExpiredIDs(ctx context.Context, tx *sql.Tx, nowMS int64, wantCancel bool) ([]string, error) {
	cond := `status = 'running' AND lease_until < ?`
	if wantCancel {
		cond += ` AND cancel_requested = 1`
	}
	rows, err := tx.QueryContext(ctx,
		b.prep(`SELECT id FROM {{t}} WHERE `+cond+` ORDER BY id FOR UPDATE SKIP LOCKED`), nowMS)
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

// listBlockedIDs 读出全部 blocked 任务 ID(读完即关结果集,再做后续写入)。
func (b *Broker) listBlockedIDs(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, b.prep(`SELECT id FROM {{t}} WHERE status = 'blocked' ORDER BY id`))
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
