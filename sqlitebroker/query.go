package sqlitebroker

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/AmbroseX/taskgate"
)

// Get 取单个任务。scanRec 扫出来的就是全新副本,调用方改了不影响存储。
func (b *Broker) Get(ctx context.Context, id string) (*taskgate.Task, error) {
	r, err := getRec(ctx, b.db, id)
	if err != nil {
		return nil, err
	}
	return &r.task, nil
}

// List 按 Filter 过滤,零值字段不过滤;先过滤 → ORDER BY (created_at, id) 升序 →
// LIMIT/OFFSET 分页(排序分页合同见 broker-contract.md,Offset 越界返回空)。
func (b *Broker) List(ctx context.Context, f taskgate.Filter) ([]*taskgate.Task, error) {
	query := `SELECT ` + taskCols + ` FROM tasks`
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
	if f.Limit > 0 || f.Offset > 0 {
		// SQLite 语法上 OFFSET 必须跟在 LIMIT 后面:只给 Offset 不给 Limit 时,
		// 用 LIMIT -1(SQLite 的"不限量")补位;Offset<0 走不进这个分支,等价按 0 处理。
		limit := f.Limit
		if limit <= 0 {
			limit = -1
		}
		query += " LIMIT ?"
		args = append(args, limit)
		if f.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, f.Offset)
		}
	}
	rows, err := b.db.QueryContext(ctx, query, args...)
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
		`SELECT COUNT(*) FROM tasks WHERE queue = ? AND status IN ('pending','retrying')`,
		queue).Scan(&n)
	return n, err
}

// Counts 出现过的 Type×Status 稀疏矩阵,和逐个 Get 汇总必须一致(brokertest 验证)。
func (b *Broker) Counts(ctx context.Context) (map[string]map[taskgate.Status]int64, error) {
	rows, err := b.db.QueryContext(ctx, `SELECT type, status, COUNT(*) FROM tasks GROUP BY type, status`)
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

// ReapExpired 回收过期租约(lease_until < now 严格小于,压线不算过期):
// 带 cancel_requested=1 的过期任务直接落 canceled(不占 LeaseLost,触发传播),
// 其余一条 UPDATE ... RETURNING 原子完成 LeaseLost+1、封顶进 failed(固定文案)或回 pending、
// 清令牌;翻 failed 的行在同一事务内触发连锁传播。顺带做防御性修复:
// blocked 但父实际全部终态的任务,按提交时同一套决策函数补唤醒/补取消
// (这不是正常路径,是给"唤醒中途崩"这类事故兜底)。返回值只算租约回收条数。
func (b *Broker) ReapExpired(ctx context.Context) (int, error) {
	if err := b.requireInit(); err != nil {
		return 0, err
	}
	var notifs []taskgate.Task
	count := 0
	err := b.withTx(ctx, func(tx *sql.Tx) error {
		now := b.clk.Now().Truncate(time.Millisecond) // 截毫秒:快照与毫秒落库同精度
		nowMS := now.UnixMilli()

		// 第零步:带取消标记的过期任务。用户已请求取消,而此刻租约过期、
		// 没有任何 worker 持有它:直接落 canceled(不占 LeaseLost),
		// 取消不能因为 worker 崩了就凭空丢失;同一事务内触发连锁传播。
		canceledRows, err := tx.QueryContext(ctx, `UPDATE tasks SET
				status = 'canceled', last_error = 'canceled', finished_at = ?1,
				lease_token = '', lease_until = 0, cancel_requested = 0
			WHERE status = 'running' AND lease_until < ?1 AND cancel_requested = 1
			RETURNING `+taskCols, nowMS)
		if err != nil {
			return err
		}
		var reapedCanceled []*rec
		for canceledRows.Next() {
			r, err := scanRec(canceledRows)
			if err != nil {
				canceledRows.Close()
				return err
			}
			reapedCanceled = append(reapedCanceled, r)
		}
		if err := canceledRows.Err(); err != nil {
			canceledRows.Close()
			return err
		}
		canceledRows.Close()
		count += len(reapedCanceled)
		for _, r := range reapedCanceled {
			notifs = append(notifs, r.task)
			if err := b.propagateTx(ctx, tx, r.task.ID, taskgate.StatusCanceled, now, &notifs); err != nil {
				return err
			}
		}

		// 第一步:一条 SQL 原子回收。SET 表达式全部基于旧值计算,先收集完再做后续写入。
		rows, err := tx.QueryContext(ctx, `UPDATE tasks SET
				lease_lost = lease_lost + 1,
				status = CASE WHEN lease_lost + 1 >= ?1 THEN 'failed' ELSE 'pending' END,
				last_error = CASE WHEN lease_lost + 1 >= ?1
					THEN 'lease expired ' || (lease_lost + 1) || ' times' ELSE last_error END,
				finished_at = CASE WHEN lease_lost + 1 >= ?1 THEN ?2 ELSE finished_at END,
				lease_token = '', lease_until = 0, cancel_requested = 0
			WHERE status = 'running' AND lease_until < ?2
			RETURNING `+taskCols, b.opts.LeaseLostMax, nowMS)
		if err != nil {
			return err
		}
		var reaped []*rec
		for rows.Next() {
			r, err := scanRec(rows)
			if err != nil {
				rows.Close()
				return err
			}
			reaped = append(reaped, r)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		count += len(reaped)
		for _, r := range reaped {
			notifs = append(notifs, r.task)
			if r.task.Status == taskgate.StatusFailed {
				// 封顶 failed 同样要在同一事务里连锁处理子任务。
				if err := b.propagateTx(ctx, tx, r.task.ID, taskgate.StatusFailed, now, &notifs); err != nil {
					return err
				}
			}
		}

		// 第二步:防御修复。blocked 却发现父全是终态 → 用和提交时同一套决策函数补齐。
		blockedIDs, err := listBlockedIDs(ctx, tx)
		if err != nil {
			return err
		}
		for _, bid := range blockedIDs {
			r, err := getRec(ctx, tx, bid)
			if err != nil {
				if errors.Is(err, taskgate.ErrTaskNotFound) {
					continue
				}
				return err
			}
			if r.task.Status != taskgate.StatusBlocked {
				continue // 可能已被上面的传播顺手处理掉了
			}
			parents := make([]taskgate.ParentState, 0, len(r.task.DependsOn))
			allExist := true
			for _, pid := range r.task.DependsOn {
				var st string
				err := tx.QueryRowContext(ctx, `SELECT status FROM tasks WHERE id = ?`, pid).Scan(&st)
				if errors.Is(err, sql.ErrNoRows) {
					allExist = false // 父记录都没了,没法判定,跳过不硬修
					break
				}
				if err != nil {
					return err
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
					`UPDATE tasks SET status = 'pending', pending_parents = 0 WHERE id = ?`, bid); err != nil {
					return err
				}
				r.task.Status = taskgate.StatusPending
				notifs = append(notifs, r.task)
			case taskgate.StatusCanceled:
				if !taskgate.CanTransition(r.task.Status, taskgate.StatusCanceled) {
					continue
				}
				if _, err := tx.ExecContext(ctx, `UPDATE tasks SET
						status = 'canceled', last_error = ?, finished_at = ?,
						lease_token = '', lease_until = 0
					WHERE id = ?`, dec.LastError, nowMS, bid); err != nil {
					return err
				}
				r.task.Status = taskgate.StatusCanceled
				r.task.LastError = dec.LastError
				r.task.FinishedAt = now
				notifs = append(notifs, r.task)
				if err := b.propagateTx(ctx, tx, bid, taskgate.StatusCanceled, now, &notifs); err != nil {
					return err
				}
			default:
				// 还该等着,顺手校准计数。
				if _, err := tx.ExecContext(ctx,
					`UPDATE tasks SET pending_parents = ? WHERE id = ?`, dec.PendingParents, bid); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(notifs) > 0 {
		b.wakeAll() // 有任务回 pending / 被唤醒,叫醒等待中的 Dequeue
	}
	b.fireNotify(notifs)
	return count, nil
}

// listBlockedIDs 读出全部 blocked 任务 ID(读完即关结果集,再做后续写入)。
func listBlockedIDs(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id FROM tasks WHERE status = 'blocked' ORDER BY id`)
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
